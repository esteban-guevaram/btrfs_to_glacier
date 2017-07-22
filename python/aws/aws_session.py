from common import *
from file_utils import *
from transaction_log import get_txlog, Record
logger = logging.getLogger(__name__)

class AwsGlobalSession:

  def __init__ (self, session_type):
    # INVARIANT : len(filesegs) <= len(_submitted_aws_jobs) for down sessions
    # => if a fileseg download is started, there must already be a aws job
    self.session_type = session_type
    self.filesegs = {}
    self._submitted_aws_jobs = {}
    self.done = False

  def iterate (self):
    return self.filesegs.values()

  def start_fileseg (self, fileseg):
    self._add_fileseg(fileseg)
    get_txlog().record_fileseg_start(fileseg)

  def close_fileseg (self, key, archive_id=None):
    self.filesegs[key].set_done(archive_id)
    get_txlog().record_fileseg_end(archive_id)

  def start_fileseg_single_chunk (self, fileseg):
    fileseg.chunks.append( Chunk(fileseg.range_bytes) )
    self.start_fileseg(fileseg)

  def close_fileseg_single_chunk (self, key, archive_id):
    self.filesegs[key].chunks[0].done = True
    self.close_fileseg(key, archive_id)

  def close_chunk (self, key, chunk_range):
    chunk = Chunk(chunk_range) 
    chunk.done = True
    self.filesegs[key].chunks.append( chunk ) 
    get_txlog().record_chunk_end(chunk_range)

  def close (self):
    assert all( fs.done for fs in self.filesegs.values() )
    get_txlog().record_aws_session_end()

  def clean_pending_fileseg (self):
    pending = [ fs for fs in self.filesegs.values() if not fs.done ]
    for fs in pending:
      del self.filesegs[fs.key()]

  def get_pending_glacier_fileseg (self):
    pending = [ fs for fs in self.filesegs.values() if not fs.done ]
    assert not pending or len(pending) == 1, 'At most one pending glacier fileseg per session'
    return pending and pending[0]

  def view_fskey_with_submitted_job (self):
    return set( fs.key() for fs in self._submitted_aws_jobs.items() )

  def view_fs_with_submitted_job (self):
    # the view will filter failed job that have been resubmitted
    view = []
    for aws_id,fs in self._submitted_aws_jobs.items():
      if fs.key() in self.filesegs and aws_id == self.filesegs[fs.key()].aws_id:
        view.append(self.filesegs[fs.key()])
      else:
        view.append(fs)
    assert len(view) >= len(self.filesegs)
    return view

  def view_fileout_to_filesegs (self):
    view = {}
    for fs in session.iterate():
      if fs.fileout not in view:
        view[fs.fileout] = []
      view[fs.fileout].append(fs)  
    return view

  @staticmethod
  def start_new (session_type):
    logger.info('Creating new aws session')
    get_txlog().record_aws_session_start(session_type)
    return AwsGlobalSession()

  @staticmethod
  def rebuild_from_txlog_or_new_session (session_type):
    accumulator = AwsGlobalSession.collect_records_from_pending_session(session_type)
    if accumulator != None:
      logger.debug('No previous session found')
      return None
    
    session = AwsGlobalSession.build_glacier_session_from_records(accumulator)
    return session

  @staticmethod
  def collect_records_from_pending_session (session_type):
    accumulator = []
    interesting_record_types = (Record.FILESEG_START, Record.FILESEG_END, Record.CHUNK_END)

    for record in get_txlog().reverse_iterate_through_records():
      assert record.r_type != Record.TXLOG_UPLD, "A txlog upload record should not be found in a pending session"

      if record.r_type == Record.AWS_END and record.session_type == session_type:
        assert not accumulator, 'Inconsistent session'
        return None # the last session is complete, start a new one

      if record.r_type in interesting_record_types:
        accumulator.append(record) 

      if record.r_type == Record.AWS_START and record.session_type == session_type:
        logger.debug('Found pending session with %d records', len(accumulator))
        return reversed(accumulator)
    return None    
  
  @staticmethod
  def build_glacier_session_from_records (accumulator):
    session = AwsGlobalSession()
    state = RestoreFilesegState()

    for record in accumulator:
      state.push(record)
      fileseg = state.flush_fileseg_if_done()
      if fileseg:
        session._add_fileseg(fileseg)

    if state.fileseg:
      fileseg.assert_in_valid_pending_state()
      session._add_fileseg(state.fileseg)
    return session

  def _add_fileseg (self, fileseg):
    if fileseg.key() in self.filesegs:
      logger.warn("%r already added to session, probably a job expired ?", fileseg)
    assert fileseg.aws_id not in self._submitted_aws_jobs
    self.filesegs[fileseg.key()] = copy.copy(fileseg)
    self._submitted_aws_jobs[fileseg.aws_id] = fileseg

  def print_summary (self):
    return 'session_done=%r, submitted_jobs=%d, fileseg_len=%d' % (self.done, len(self._submitted_aws_jobs), len(self.filesegs))

  def print_glacier_summary (self):
    lines = [ repr(fs) for fs in self.filesegs.values() ]
    return "\n".join(lines)

## END AwsGlobalSession

class RestoreFilesegState:
  __slots__ = ['fileseg', 'chunk']

  def push (self, record):
    if record.r_type == Record.FILESEG_START:
      assert not self.fileseg, 'Cannot have 2 pending filesegs'
      self.fileseg = Fileseg.build_from_record(record)
    elif record.r_type == Record.FILESEG_END:
      assert self.fileseg and self.chunk and self.chunk.done, 'Empty or pending chunk when ending fileseg'
      self.fileseg.set_done(record.archive_id)
    elif record.r_type == Record.CHUNK_END:
      assert self.chunk and not self.chunk.done, 'No valid chunk to end'
      assert self.fileseg, 'No fileseg for chunk'
      assert self.chunk == record.range_bytes, 'Chunk range bytes is not consistent'
      self.chunk = Chunk(record.range_bytes)
      self.chunk.done = True
      self.fileseg.add_chunk(self.chunk)
      self.chunk = None
    else:
      logger.debug("Ignoring record %r", record)
  
  def flush_fileseg_if_done (self, record):
    fileseg = None
    if self.fileseg and self.fileseg.done:
      fileseg = self.fileseg
      self.fileseg = None
    return fileseg

## END RestoreFilesegState

class BandwithQuota:
  GLACIER_QUOTA_HOURS = 4
  DELTA = datetime.timedelta(hours=GLACIER_QUOTA_HOURS, seconds=60)
  TIMEOUT_FACTOR = 1.5

  __slots__ = ['expiry_time', 'total_bytes', 'submitted_filesegs']
  def __init__ (self, quota_start=None, total=0):
    QUOTA_SIZE = get_conf().aws.glacier_down_bandwith_gb * 1024**3
    MAX_SIZE = self.QUOTA_SIZE + get_conf().aws.chunk_size_in_mb * 1024**2
    assert total < self.MAX_SIZE

    if not quota_start:
      quota_start = datetime.datetime.now()
    self.expiry_time = quota_start + BandwithQuota.DELTA
    self.total_bytes = total
    self.submitted_fileseg_keys = set()

  def space_left (self):
    # left can be negative
    left = self.QUOTA_SIZE - self.total_bytes
    tolerated = self.MAX_SIZE - self.total_bytes
    return left, tolerated

  def add_to_submit (self, fileseg):
    new_total = fileseg.len() + self.total_bytes
    assert new_total < self.MAX_SIZE
    self.total_bytes = new_total
    self.submitted_fileseg_keys.add(fileseg.key())

  def is_expired (self):
    return datetime.datetime.now() > self.expiry_time

  def __repr__ (self):
    return "(expiry=%r, total=%r, submitted=%d)" % (self.expiry_time, self.total_bytes, len(self.submitted_filesegs))

  @staticmethod
  def check_time_delta_in_quota (past, now):
    expiry_time = past + BandwithQuota.DELTA
    return expiry_time > now

  @staticmethod
  def build_empty_expired ():
    quota_start = datetime.datetime.now() - 2*BandwithQuota.DELTA
    return BandwithQuota(quota_start)

  @staticmethod
  def is_in_timeout (now, creation_date):
    return now - creation_date > BandwithQuota.TIMEOUT_FACTOR * BandwithQuota.DELTA

## END BandwithQuota

