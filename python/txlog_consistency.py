from common import *
from transaction_log import *
logger = logging.getLogger(__name__)

# session type | multiple sessions | can be resumed |
#--------------|-------------------|----------------|
# backup       | yes               | no             |
# upload       | yes               | yes            | 
# download     | no                | yes            |
# restore      | no                | no             |
#
# No session can overlap with a pending session of another type
# All previous sessions of different types must have completed
class TxLogConsistencyChecker (object):

  @staticmethod
  def check_log_for_download(tx_list):
    ov_check = OverlapChecker()

    for record in tx_list:
      ov_check.next_record(record)
      if record.r_type in (Record.FILESEG_START, Record.FILESEG_END, Record.CHUNK_END):
        assert ov_check.pending_down_session or ov_check.pending_upld_session, \
          'Not expecting upload/download operations outside session'
    assert ov_check.complete_down_session == 0, 'Txlog can only contain one download session'

  @staticmethod
  def check_log_for_restore(tx_list):
    ov_check = OverlapChecker()

    for record in tx_list:
      ov_check.next_record(record)
      assert record.r_type != Record.FILE_TO_SNAP, 'Not expecting this operation'

    pending_tuple = (ov_check.pending_back_session, ov_check.pending_rest_session, ov_check.pending_down_session, ov_check.pending_upld_session)
    assert all( not p for p in pending_tuple), 'No session type can be pending when starting a restore'
    assert ov_check.complete_rest_session == 0, 'Txlog can only contain one restore session'

  @staticmethod
  def check_log_for_upload(tx_list):
    ov_check = OverlapChecker()
    count_fs_per_session = 0
    fileout_back = []
    fileout_upld = set()

    for record in tx_list:
      ov_check.next_record(record)
      if record.r_type in (Record.FILESEG_START, Record.FILESEG_END, Record.CHUNK_END):
        assert ov_check.pending_down_session or ov_check.pending_upld_session, \
          'Not expecting upload/download operations outside session'

      if record.r_type == Record.FILESEG_START and ov_check.pending_upld_session:
        fileout_upld.add(record.fileout)
        count_fs_per_session += 1
      elif record.r_type == Record.FILESEG_END and ov_check.pending_upld_session:
        count_fs_per_session += 1
      elif record.r_type == Record.SNAP_TO_FILE:
        fileout_back.append( record.fileout )
      elif record.r_type == Record.AWS_END and record.session_type == Record.SESSION_UPLD:
        assert count_fs_per_session % 2 == 0, "If a session has ended all uploads should have completed too"
        count_fs_per_session = 0

    assert not ov_check.pending_back_session, 'Backup session must be complete'
    # We count the fileouts in fileout_back until we find 1 that is not in fileout_upld, 
    # there should NOT be a hole in the list of fileouts already uploaded
    # backup : fs1,        fs2, fs3, fs4
    #           |           |    X
    # upload : fs1, fs666, fs2, ( ), fs4
    back_upld_intersect = [ f for f in fileout_back if f in fileout_upld ]
    assert len(back_upld_intersect) == sum( 1 for f in itertools.takewhile(lambda f: f in fileout_upld, fileout_back)), \
      "A created snapshot has not been uploaded : %r / %r" % (fileout_back, fileout_upld)

  @staticmethod
  def check_log_for_backup(tx_list):
    ov_check = OverlapChecker()
    parent_to_childs = {}
    parent_to_deleted = {}
    parent_to_files = {}

    for record in tx_list:
      ov_check.next_record(record)
      if record.r_type in (Record.DEL_SNAP, Record.SNAP_TO_FILE, Record.NEW_SNAP):
        assert ov_check.pending_back_session, "Backup operations outside a session"

      if record.r_type == Record.NEW_SNAP:
        parent_to_childs.setdefault(record.subvol.puuid, []).append( record.subvol.uuid )
        parent_to_deleted.setdefault(record.subvol.puuid, [])
        parent_to_files.setdefault(record.subvol.puuid, [])

      if record.r_type == Record.DEL_SNAP:
        parent_to_deleted[record.subvol.puuid].append( record.subvol.uuid )
        assert record.subvol.uuid in parent_to_childs[record.subvol.puuid], \
          'All deleted snaps must have been created previously'

      if record.r_type == Record.SNAP_TO_FILE:
        parent_to_files[record.subvol.puuid].append( record.subvol.uuid )
        assert record.subvol.uuid in parent_to_childs[record.subvol.puuid], \
          'A snap was written to file but never created'
        assert record.subvol.uuid not in parent_to_deleted[record.subvol.puuid], \
          'A snap was written to file after deletion'

    assert not ov_check.pending_back_session, 'Backup session cannot be left pending'
    assert all( len(parent_to_files[k]) == len(snaps) for k,snaps in parent_to_childs.items() ), \
      "All snapshots created must have been saved to a file"

  # meant to be used only during corruption of txlog
  @staticmethod
  def _validate_all_individual_batch_hashes(logfilepath):
    segments = []
    batch_hashes = []
    with open(logfilepath, 'rb') as logfile:
      TransactionLog.parse_header_and_advance_file(logfile)
      start_seg = logfile.tell()
      try:
        while 1:
          last_offset = logfile.tell()
          record = pickle.load(logfile)
          if record.r_type == Record.TXLOG_TO_FILE:
            assert start_seg < last_offset
            segments.append( (start_seg, last_offset) )
            start_seg = last_offset
            batch_hashes.append(record)
      except EOFError:
        pass

    logger.debug("Batch segments : %r", segments)     
    hasher = hashlib.sha256()

    with open(logfilepath, 'rb') as logfile:
      TransactionLog.parse_header_and_advance_file(logfile)
      for record,segment in zip(batch_hashes,segments):
        data = logfile.read(segment[1]-segment[0])
        hasher.update(data)
        real_hash = hasher.digest()
        logger.debug("@%r : %r != %r", segment, real_hash, record.hashstr)
        assert real_hash == record.hashstr, "@%r : %r != %r" % (segment, real_hash, record.hashstr)

### END TxLogConsistencyChecker

class OverlapChecker:

  def __init__(self):
    self.pending_upld_session = False
    self.complete_upld_session = 0
    self.pending_down_session = False
    self.complete_down_session = 0
    self.pending_back_session = False
    self.complete_back_session = 0
    self.pending_rest_session = False
    self.complete_rest_session = 0

  def next_record (self, record):
    self._update_session_status(record)
    # detect overlap, no 2 types of sessions can be pending at the same time
    pending_tuple = (self.pending_back_session, self.pending_rest_session, self.pending_down_session, self.pending_upld_session)
    assert sum(1 for s in pending_tuple if s) < 2

  def _update_session_status (self, record):
    if record.r_type == Record.BACK_START: 
      assert not self.pending_back_session
      self.pending_back_session = True
    elif record.r_type == Record.BACK_END:
      assert self.pending_back_session
      self.pending_back_session = False
      self.complete_back_session += 1

    elif record.r_type == Record.REST_START: 
      assert not self.pending_rest_session
      self.pending_rest_session = True
    elif record.r_type == Record.REST_END:
      assert self.pending_rest_session
      self.pending_rest_session = False
      self.complete_rest_session += 1

    elif record.r_type == Record.AWS_START: 
      if record.session_type == Record.SESSION_UPLD:
        assert not self.pending_upld_session
        self.pending_upld_session = True
      else:
        assert not self.pending_down_session
        self.pending_down_session = True
    elif record.r_type == Record.AWS_END:
      if record.session_type == Record.SESSION_UPLD:
        assert self.pending_upld_session
        self.pending_upld_session = False
        self.complete_upld_session += 1
      else:
        assert self.pending_down_session
        self.pending_down_session = False
        self.complete_down_session += 1

### END OverlapChecker

