import types
from common import *
from transaction_log import *
logger = logging.getLogger(__name__)

# Writes a modified copy of the transaction log
# Useful when the log is corrupted and manual intervention is needed
class TxLogManipulation:
  
  @staticmethod
  def remove_download_sessions(src_txlog_path, dest_txlog_path):
    state = types.SimpleNamespace()
    state.session_start = False

    def predicate (record):
      if record.r_type == Record.AWS_START and record.session_type == Record.SESSION_DOWN:
        state.session_start = True
        return False
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_DOWN:
        state.session_start = False
        return False
      return not state.session_start

    return TxLogManipulation.selective_copy(src_txlog_path, dest_txlog_path, predicate)

  @staticmethod
  def remove_restore_sessions(src_txlog_path, dest_txlog_path):
    state = types.SimpleNamespace()
    state.session_start = False

    def predicate (record):
      if record.r_type == Record.REST_START:
        state.session_start = True
        return False
      if record.r_type == Record.REST_END:
        state.session_start = False
        return False
      return not state.session_start

    return TxLogManipulation.selective_copy(src_txlog_path, dest_txlog_path, predicate)

  @staticmethod
  def remove_upload_sessions(src_txlog_path, dest_txlog_path):
    state = types.SimpleNamespace()
    state.session_start = False

    def predicate (record):
      if record.r_type == Record.AWS_START and record.session_type == Record.SESSION_UPLD:
        state.session_start = True
        return False
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_UPLD:
        state.session_start = False
        return False
      return not state.session_start

    return TxLogManipulation.selective_copy(src_txlog_path, dest_txlog_path, predicate)

  @staticmethod
  def selective_copy(src_txlog_path, dest_txlog_path, predicate):
    assert os.path.isfile(src_txlog_path) and not os.path.isfile(dest_txlog_path)
    src_txlog = TransactionLog(src_txlog_path)
    dest_txlog = TransactionLog(dest_txlog_path)
    keep_records = []

    for record in src_txlog.iterate_through_records():
      # since we remove arbitrary records we cannot rely on these hashes
      if record.r_type == Record.TXLOG_TO_FILE: continue
      if not predicate(record): continue
      keep_records.append(record)

    if keep_records:
      dest_txlog.__add_and_flush_many_record__(keep_records)
      dest_txlog.calculate_and_store_txlog_hash()
    logger.info("From %s copied %d records into %s", src_txlog_path, len(keep_records), dest_txlog_path)
    return dest_txlog

### END TxLogManipulation

