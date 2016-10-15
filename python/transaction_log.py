import pickle as pickle, time, struct, hashlib
from common import *
from file_utils import *
logger = logging.getLogger(__name__)

class TransactionLog (object):
  VERSION = 1
  HEADER_LEN = 512
  HEADER_STRUCT = "<II32s472x"

  def __init__(self, name=None):
    self.pickle_proto = get_conf().app.pickle_proto
    self.logfile = name
    self.recorded_snaps = set()
    self.recorded_restores = set()

    if not name:
      self.logfile = get_conf().app.transaction_log
    self.tx_list = self.load_from_file_and_check_hash()

    if self.tx_list:
      self.cur_uid = self.tx_list[-1].uid + 1  
    else:
      self.cur_uid = 0

  def load_from_file_and_check_hash(self):
    logger.info("Loading tx log from %s", self.logfile)
    if not os.path.exists(self.logfile):
      self.save_txlog_header(0, b"\0"*32)
      return []

    tx_list = []
    hasher = hashlib.sha256()
    with open(self.logfile, 'rb') as logfile:
      filehash, hash_domain_upper = TransactionLog.parse_header_and_advance_file(logfile)
      logger.info("Txlog header : hash_domain_upper=%d, hash=%r", hash_domain_upper, filehash)
      try:
        while 1:
          record = pickle.load(logfile)
          tx_list.append( record )
          if record.r_type == Record.NEW_SNAP:
            self.recorded_snaps.add(record.subvol.uuid)
          elif record.r_type == Record.FILE_TO_SNAP:
            self.recorded_restores.add(record.subvol.uuid)
      except EOFError:
        pass
    
    assert len(self.recorded_snaps) >= len(self.recorded_restores)
    self.validate_main_hash(filehash, hash_domain_upper)
    return tx_list

  def backup_to_crypted_file(self):
    logfile = self.logfile
    hashstr = self.calculate_and_store_txlog_main_hash()
    self.record_txlog_to_file(hashstr)

    back_logfile = FileUtils.compress_crypt_file(logfile)
    return back_logfile    

  def save_txlog_header (self, hash_domain_upper, filehash):
    if not os.path.exists(self.logfile):
      with open(self.logfile, 'wb') as logfile: pass
      
    with open(self.logfile, 'r+b') as logfile:
      logfile.seek(0, os.SEEK_SET)
      header = struct.pack(TransactionLog.HEADER_STRUCT, TransactionLog.VERSION, hash_domain_upper, filehash)
      assert len(header) == TransactionLog.HEADER_LEN
      logfile.write(header)
    logger.debug("Wrote header : %d, %r", hash_domain_upper, filehash)  

  
  def reverse_iterate_through_records(self):
    # we copy the list to avoid iterating over elements created after the call
    return iter( self.tx_list[::-1] )

  def iterate_through_records(self):
    # we copy the list to avoid iterating over elements created after the call
    return iter( self.tx_list[:] )

  def new_uid(self):
    self.cur_uid += 1
    return self.cur_uid

  def is_empty (self):
    return not self.tx_list


  @tx_handler.wrap
  def add_and_flush_record(self, record):
    self.tx_list.append( record )
    with open(self.logfile, 'ab') as logfile:
      pickle.dump(record, logfile, self.pickle_proto)

  def record_backup_start(self):
    record = Record(Record.BACK_START, self.new_uid())
    self.add_and_flush_record(record)

  def record_backup_end(self):
    record = Record(Record.BACK_END, self.new_uid())
    self.add_and_flush_record(record)

  def record_restore_start(self):
    record = Record(Record.REST_START, self.new_uid())
    self.add_and_flush_record(record)

  def record_restore_end(self):
    record = Record(Record.REST_END, self.new_uid())
    self.add_and_flush_record(record)

  def record_aws_session_start(self, session_type):
    record = Record(Record.AWS_START, self.new_uid())
    record.session_type = session_type
    self.add_and_flush_record(record)

  def record_aws_session_end(self, session_type):
    record = Record(Record.AWS_END, self.new_uid())
    record.session_type = session_type
    self.add_and_flush_record(record)

  def record_aws_down_job_submit(self, fileout, aws_id, range_bytes):
    record = Record(Record.AWS_DOWN_INIT, self.new_uid())
    record.fileout = os.path.basename( fileout )
    record.aws_id = aws_id
    record.range_bytes = range_bytes
    self.add_and_flush_record(record)

  def record_fileseg_start(self, fileseg):
    # single upload : fileout, range_bytes
    # multipart upload : fileout, aws_id, range_bytes
    # download : fileout, aws_id, archive_id, range_bytes
    record = Record(Record.FILESEG_START, self.new_uid())
    record.fileout = os.path.basename( fileseg.fileout )
    record.aws_id = fileseg.aws_id
    record.archive_id = fileseg.archive_id
    record.range_bytes = fileseg.range_bytes
    self.add_and_flush_record(record)

  def record_fileseg_end(self, fileseg):
    # upload : archive_id
    # download : None
    record = Record(Record.FILESEG_END, self.new_uid())
    record.archive_id = fileseg.archive_id
    self.add_and_flush_record(record)

  def record_chunk_start(self, range_bytes):
    record = Record(Record.CHUNK_START, self.new_uid())
    record.range_bytes = range_bytes
    self.add_and_flush_record(record)

  def record_chunk_end(self):
    record = Record(Record.CHUNK_END, self.new_uid())
    self.add_and_flush_record(record)

  def record_txlog_upload(self, fileseg):
    record = Record(Record.TXLOG_UPLD, self.new_uid())
    record.fileout = os.path.basename( fileseg.fileout )
    record.archive_id = fileseg.archive_id
    record.range_bytes = fileseg.range_bytes
    self.add_and_flush_record(record)

  def record_snap_creation(self, snap):
    record = Record(Record.NEW_SNAP, self.new_uid())
    record.subvol = snap
    self.recorded_snaps.add(snap.uuid)
    self.add_and_flush_record(record)

  def record_subvol_delete(self, subvol):
    record = Record(Record.DEL_SNAP, self.new_uid())
    record.subvol = subvol
    self.add_and_flush_record(record)
  
  def record_txlog_to_file(self, hashstr):
    record = Record(Record.TXLOG_TO_FILE, self.new_uid())
    record.hashstr = hashstr
    self.add_and_flush_record(record)

  def record_snap_to_file(self, fileout, hashstr, snap, pre_uuid=None):
    assert fileout and hashstr
    record = Record(Record.SNAP_TO_FILE, self.new_uid())
    record.pre_uuid = pre_uuid
    record.subvol = snap
    record.fileout = os.path.basename( fileout )
    record.hashstr = hashstr
    self.add_and_flush_record(record)

  def record_file_to_snap(self, fileout, subvol, ancestor_uuid):
    record = Record(Record.FILE_TO_SNAP, self.new_uid())
    record.subvol = subvol
    record.ancestor_uuid = ancestor_uuid
    record.fileout = os.path.basename( fileout )
    self.recorded_restores.add(subvol.uuid)
    self.add_and_flush_record(record)

  def calculate_and_store_txlog_main_hash (self):
    with open(self.logfile, 'rb') as logfile:
      TransactionLog.parse_header_and_advance_file(logfile)
      data = logfile.read()
    hash_domain_upper = len(data)
    assert hash_domain_upper > 0
    real_hash = hashlib.sha256(data).digest()
    self.save_txlog_header(hash_domain_upper, real_hash) 
    return real_hash

  def validate_main_hash(self, filehash, hash_domain_upper):
    if not hash_domain_upper: return 

    with open(self.logfile, 'rb') as logfile:
      TransactionLog.parse_header_and_advance_file(logfile)
      data = logfile.read(hash_domain_upper)
    assert len(data) == hash_domain_upper, "%s : %d != %d" % (self.logfile, len(data), hash_domain_upper)
    real_hash = hashlib.sha256(data).digest()
    logger.info("Comparing domain [%d] : %r / %r", hash_domain_upper, filehash, real_hash)
    assert real_hash == filehash

  @staticmethod
  def restore_from_crypted_file(archive_txlog):
    assert not get_txlog().tx_list, "Will not overwrite tx log"
    FileUtils.decompress_decrypt_file( archive_txlog, get_txlog().logfile )
    # forces lazy reloading
    reset_txlog() 

  @staticmethod
  def parse_header_and_advance_file (logfile):
    header_raw = logfile.read(TransactionLog.HEADER_LEN)
    assert len(header_raw) == TransactionLog.HEADER_LEN
    version, hash_domain_upper, main_hash = struct.unpack(TransactionLog.HEADER_STRUCT, header_raw)
    assert version == TransactionLog.VERSION
    return main_hash, hash_domain_upper

  def __len__(self):
    return len(self.tx_list)

  def __repr__(self):
    return "\n".join(repr(r) for r in self.tx_list)

### END TransactionLog

class Record (object):
  SESSION_DOWN, SESSION_UPLD = 'SESSION_DOWN', 'SESSION_UPLD'

  TXLOG_TO_FILE,   TXLOG_UPLD,  = \
  'TXLOG_TO_FILE', 'TXLOG_UPLD'

  BACK_START,   BACK_END,   DEL_SNAP,   NEW_SNAP,   SNAP_TO_FILE   = \
  'BACK_START', 'BACK_END', 'DEL_SNAP', 'NEW_SNAP', 'SNAP_TO_FILE'

  REST_START,   REST_END,   FILE_TO_SNAP   = \
  'REST_START', 'REST_END', 'FILE_TO_SNAP'

  AWS_START,   AWS_END,   AWS_DOWN_INIT,    FILESEG_START,   FILESEG_END,   CHUNK_START,    CHUNK_END   = \
  'AWS_START', 'AWS_END', 'AWS_DOWN_INIT', 'FILESEG_START', 'FILESEG_END', 'CHUNK_START',  'CHUNK_END'

  def __init__(self, r_type, uid):
    self.r_type = r_type
    self.uid = uid
    self.when = time.time()

  def __repr__(self):
    return "%s=%r" % (self.r_type, vars(self))
### END Record  

singleton_txlog = None
def get_txlog():
  global singleton_txlog
  if singleton_txlog == None:
    logger.info('Creating new tx log')
    singleton_txlog = TransactionLog()
  return singleton_txlog  

def reset_txlog():
  global singleton_txlog
  singleton_txlog = None

