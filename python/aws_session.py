from common import *
from transaction_log import TransactionLog, get_txlog, Record
logger = logging.getLogger(__name__)

class AwsGlobalSession:

  def __init__ (self, session_type):
    self.session_type = session_type
    self.filesegs = {}
    self.txlog_fileseg = None
    self.done = False

  def iterate (self):
    return self.filesegs.values()

  def start_fileseg (self, fileseg):
    self._add_fileseg(fileseg)
    get_txlog().record_fileseg_start(fileseg)

  def close_fileseg (self, key, archive_id):
    self.filesegs[key].set_done(archive_id)
    get_txlog().record_fileseg_end(fileseg)

  def start_fileseg_single_chunk (self, fileseg):
    fileseg.chunks.append( Chunk(fileseg.range_bytes) )
    self.start_fileseg(fileseg)

  def close_fileseg_single_chunk (self, key, archive_id):
    self.filesegs[key].chunks[0].done = True
    self.close_fileseg(key, archive_id)

  def start_chunk (self, key, chunk_range):
    self.filesegs[key].chunks.append( Chunk(chunk_range) ) 
    get_txlog().record_chunk_start(chunk_range)

  def close_chunk (self, key):
    chunk = self.filesegs[key].chunks[-1]
    assert not chunk.done
    chunk.done = True
    get_txlog().record_chunk_end(fileseg)

  def save_atomic_txlog_s3_upload (self, fileseg):
    self.txlog_fileseg = Fileseg.build_from_fileout(fileseg.fileout)
    self.txlog_fileseg.archive_id = fileseg.archive_id
    self.txlog_fileseg.set_done()
    get_txlog().record_txlog_upload(fileseg)

  def close (self):
    assert all( fs.done for fs in self.filesegs.values() )
    assert self.txlog_fileseg
    get_txlog().record_aws_session_end()

  def clean_pending_fileseg (self):
    pending = [ fs for fs in self.filesegs.values() if not fs.done ]
    for fs in pending:
      del self.filesegs[fs.key]

  def get_pending_glacier_fileseg (self):
    pending = [ fs for fs in self.filesegs.values() if not fs.done ]
    assert not pending or len(pending) == 1, 'At most one pending glacier fileseg per session'
    return pending and pending[0]

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
    
    session = AwsGlobalSession.build_glacier_session_from_records(session_type, accumulator)
    return session

  @staticmethod
  def rebuild_last_complete_from_txlog (session_type):
    accumulator = AwsGlobalSession.collect_records_from_last_completed(session_type)
    if accumulator != None:
      logger.debug('No previous session found')
      return None
    
    session = AwsGlobalSession.build_glacier_session_from_records(session_type, accumulator)
    assert not self.get_pending_fileseg()

    txlog_upload_record = next( r for r in accumulator if r.r_type == Record.TXLOG_UPLD )
    self.txlog_fileseg = Fileseg.build_from_txlog_upload(txlog_upload_record)
    session.done = True
    return session

  @staticmethod
  def collect_records_from_last_completed (session_type):
    accumulator = []
    saw_txlog_upld, saw_session_end = False, False

    for record in get_txlog().reverse_iterate_through_records():
      saw_session_end = record.r_type == Record.AWS_END and record.session_type == session_type
      saw_txlog_upld = saw_txlog_upld and record.r_type == Record.TXLOG_UPLD

      if record.r_type in (Record.FILESEG_START, Record.FILESEG_END, Record.CHUNK_START, Record.CHUNK_END, Record.TXLOG_UPLD):
        accumulator.append(record) 

      if record.r_type == Record.AWS_START and record.session_type == session_type:
        logger.debug('Found complete session with %d records', len(accumulator))
        break

    if not accumulator:
      return None
    assert saw_txlog_upld and saw_txlog_upld and accumulator, "Invalid complete session"
    return reversed(accumulator)    

  @staticmethod
  def collect_records_from_pending_session (session_type):
    accumulator = []
    for record in get_txlog().reverse_iterate_through_records():
      assert record.r_type != Record.TXLOG_UPLD, "A txlog record should not be found in a pending session"

      if record.r_type == Record.AWS_END and record.session_type == session_type:
        assert not accumulator, 'Inconsistent session'
        return None # the last session is complete, start a new one

      if record.r_type in (Record.FILESEG_START, Record.FILESEG_END, Record.CHUNK_START, Record.CHUNK_END):
        accumulator.append(record) 

      if record.r_type == Record.AWS_START and record.session_type == session_type:
        logger.debug('Found pending session with %d records', len(accumulator))
        return reversed(accumulator)
    return None    
  
  @staticmethod
  def build_glacier_session_from_records (session_type, accumulator):
    session = AwsGlobalSession()
    state = RestoreState()
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
    assert fileseg.key() not in self.filesegs
    self.filesegs[fileseg.key()] = fileseg

  def print_summary (self):
    return 'session_done=%r, fileseg_len=%r, txlog_upload=%r' % (self.done, len(self.filesegs), self.txlog_fileseg)
  
  def print_glacier_summary (self):
    lines = [ repr(fs) for fs in self.filesegs.values() ]
    return "\n".join(lines)

## END AwsGlobalSession

class RestoreState:
  __slots__ = ['fileseg', 'chunk']

  def push (self, record):
    if record.r_type == Record.FILESEG_START:
      assert not self.fileseg, 'Cannot have 2 pending filesegs'
      self.fileseg = Fileseg.build_from_fileseg_start_record(record)
    elif record.r_type == Record.FILESEG_END:
      assert self.fileseg and self.chunk and self.chunk.done, 'Empty or pending chunk when ending fileseg'
      self.fileseg.set_done(record.archive_id)
    elif record.r_type == Record.CHUNK_START:
      assert self.fileseg, 'No fileseg for chunk'
      assert not self.chunk or self.chunk == record.range_bytes, 'Chunk range bytes is not consistent'
      self.chunk = Chunk(record.range_bytes)
    elif record.r_type == Record.CHUNK_END:
      assert self.chunk and not self.chunk.done, 'No valid chunk to end'
      self.chunk.done = True
      # we only add chunks if they were finished
      self.fileseg.add_chunk(self.chunk)
      self.chunk = None
    assert False, 'Invalid record type in accumulator'  
  
  def flush_fileseg_if_done (self, record):
    fileseg = None
    if self.fileseg and self.fileseg.done:
      fileseg = self.fileseg
      self.fileseg = None
    return fileseg

## END RestoreState

class Fileseg:
  __slots__ = ['aws_id', 'archive_id', 'fileout', 'range_bytes', 'chunks', 'done', 'timestamp']

  @staticmethod
  def build_from_fileseg_start_record (fileout, record):
    fileseg = Fileseg(record.fileout, record.aws_id, record.archive_id, record.range_bytes, record.when)
    return fileseg

  @staticmethod
  def build_from_fileout (fileout, range_bytes=None):
    fileseg = Fileseg(fileout, None, None, range_bytes, None)
    return fileseg

  @staticmethod
  def build_from_txlog_upload (record):
    fileseg = Fileseg(record.fileout, None, record.archive_id, None, record.when)
    fileseg.done = True
    return fileseg

  def __init__ (self, fileout, aws_id, archive_id, fileout, range_bytes, timestamp):
    self.fileout = fileout
    self.aws_id = aws_id
    self.archive_id = archive_id
    self.done = False
    self.chunks = []

    self.timestamp = timestamp
    if not timestamp:
      self.timestamp = datetime.datetime.now()

    if not range_bytes and os.path.exists(fileout):
      size = os.stat(fileout).st_size
      range_bytes = (0, size)
    else:
      self.range_bytes = range_bytes
    assert fileout and range_bytes
  
  def calculate_remaining_range (self):
    if not self.chunks:
      return self.range_bytes
    range_bytes = (self.chunks[-1].range_bytes[1], self.range_bytes[1])
    assert range_bytes[1] - range_bytes[0] > 0
    return range_bytes

  def clean_pending_chunk (self):
    self.done = False
    if self.chunks and not self.chunks[1].done:
      self.chunks.pop()

  def clean_chunks (self):
    self.chunks = []
    self.done = False

  def assert_in_valid_pending_state (self):
    assert not self.done
    if self.chunks and self.chunks[1].done:
      assert self.range_bytes[1] > self.chunks[1].range_bytes[1]

  def set_done (self, archive_id=None):
    if archive_id:
      self.archive_id = archive_id
    assert self.fileout and self.archive_id
    self.done = True

  def add_chunk (self, chunk):
    assert not self.chunks or self.chunks[-1][1] == chunk[0], 'Uncontinous chunk added to fileseg'
    self.chunks.append(chunk)

  def key (self):
    return (self.fileout, self.range_bytes)

  def __repr__ (self):
    return "(aws_id=%r, fileout=%r, range=%r, done=%r)" % (self.aws_id, self.fileout, self.range_bytes, self.done)

## END Fileseg

class Chunk:
  __slots__ = ['range_bytes', 'done']
  def __init__ (self, range_bytes):
    assert not range_bytes or range_bytes[0] < range_bytes[1]
    self.range_bytes = range_bytes
    self.done = False

  def __repr__ (self):
    return "(range=%r, done=%r)" % (self.range_bytes, self.done)

## END Chunk

