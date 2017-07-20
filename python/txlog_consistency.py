from common import *
from transaction_log import *
logger = logging.getLogger(__name__)

class TxLogConsistencyChecker (object):
  # Check routines can only validate the previous stage

  @staticmethod
  def check_log_for_upload(tx_list):
    alpha_omega = (Record.AWS_START, Record.AWS_END)
    complete_bak_sessions = 0
    start_bak_sessions = 0
    pending_up_session = False

    for record in tx_list:
      if record.r_type in alpha_omega:
        assert record.session_type == Record.SESSION_UPLD, "Overlapping upload and download sessions"
        pending_up_session = (record.r_type == Record.AWS_START)

      if record.r_type == Record.BACK_END:
        complete_bak_sessions += 1
        assert not pending_up_session, 'Overlapping backup and upload session'
      if record.r_type == Record.BACK_START:
        start_bak_sessions += 1
        assert not pending_up_session, 'Overlapping backup and upload session'

    assert start_bak_sessions == complete_bak_sessions, 'All backup sessions have completed'

  @staticmethod
  def check_log_for_download(tx_list):
    alpha_omega = (Record.AWS_START, Record.AWS_END)
    complete_up_sessions = 0
    start_up_sessions = 0
    pending_down_session = False

    for record in tx_list:
      if record.r_type in alpha_omega and record.session_type == Record.SESSION_UPLD:
        complete_up_sessions += record.r_type == Record.AWS_END
        start_up_sessions += record.r_type == Record.AWS_START
      if record.r_type == Record.AWS_START and record.session_type == Record.SESSION_DOWN:
        pending_down_session = True

      if record.r_type in alpha_omega and record.session_type == Record.SESSION_UPLD:
        assert not pending_down_session, 'no overlapping download/restore session'
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_DOWN:
        assert False, 'Previous download session finished (glacier is expensive!)'

    assert start_up_sessions == complete_up_sessions, 'All down sessions have completed'
    assert complete_up_sessions, 'At least one completed upload session'    

  @staticmethod
  def check_log_for_restore(tx_list):
    complete_down_session = True

    for record in tx_list:
      if record.r_type == Record.AWS_START and record.session_type == Record.SESSION_DOWN:
        complete_down_session = False
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_DOWN:
        complete_down_session = True

      assert record.r_type not in (Record.FILE_TO_SNAP, Record.REST_END, Record.REST_START), \
        'No pending restore session allowed'
    assert complete_down_session, 'Either there is not download session, or it has finished'

  @staticmethod
  def check_log_for_backup(tx_list):
    complete_bak_session = True
    parent_to_childs = {}
    parent_to_deleted = {}

    for record in tx_list:
      if record.r_type == Record.BACK_START: complete_bak_session = False
      if record.r_type == Record.BACK_END:   complete_bak_session = True

      if record.r_type in (Record.DEL_SNAP, Record.SNAP_TO_FILE, Record.NEW_SNAP):
        assert not complete_bak_session, "Backup operations outside a session"

      if record.r_type == Record.NEW_SNAP:
        if record.subvol.puuid not in parent_to_childs: 
          parent_to_childs[record.subvol.puuid] = set()
          parent_to_deleted[record.subvol.puuid] = set()
        parent_to_childs[record.subvol.puuid].add( record.subvol.uuid )

      if record.r_type == Record.DEL_SNAP:
        parent_to_deleted[record.subvol.puuid].add( record.subvol.uuid )
        assert record.subvol.uuid in parent_to_childs[record.subvol.puuid], \
          'All deleted snaps must have been created previously'

      if record.r_type == Record.SNAP_TO_FILE:
        assert record.subvol.uuid in parent_to_childs[record.subvol.puuid], \
          'A snap was written to file but never created'
        assert record.subvol.uuid not in parent_to_deleted[record.subvol.puuid], \
          'A snap was written to file after deletion'
    assert complete_bak_session, 'No pending backup session allowed'      

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

