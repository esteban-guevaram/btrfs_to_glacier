import pickle as pickle, time, struct, hashlib
from common import *
from FileUtils import *
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

  def check_log_for_upload(self):
    assert self.tx_list
    assert False, 'no overlapping download/restore session'
    assert False, 'no unfinished backup session'

  def check_log_for_download(self):
    assert self.tx_list
    assert False, 'no previous download session finished (glacier is expensive!)'
    assert False, 'no overlapping download/restore session'

  def check_log_for_restore(self):
    assert self.tx_list
    send_files = set()
    upld_files = set()
    file_snap = {}
    chain_snap = {}

    assert False, 'no download/upload session in progress'

    for record in self.tx_list:
      if record.r_type == Record.SNAP_TO_FILE:
        send_files.add(record.fileout)
        file_snap[record.subvol.uuid] = 1
        origin_uuid = record.subvol.puuid
        if record.predecessor:
          # We expect all snaps of a given volume to be in the delta application order
          assert chain_snap.get(origin_uuid) == record.predecessor.uuid, \
            "Chain failed %r - %r - %r" % (origin_uuid, chain_snap.get(origin_uuid), record.predecessor.uuid)
        chain_snap[origin_uuid] = record.subvol.uuid
      elif record.r_type == Record.UPD_FILE:
        upld_files.add(record.fileout)
      elif record.r_type == Record.NEW_SNAP:
        file_snap[record.subvol.uuid] = 0

    # All snaps were saved to a file
    assert all( v == 1 for v in file_snap.values() ) 
    # All send files were sent to glacier
    assert not upld_files or upld_files == send_files, \
      "Expecting all send files created were uploaded %r / %r" % (send_files, upld_files)
  
  def check_log_for_backup(self, subvols):
    if not self.tx_list: return
    vol_dict = dict( (n.uuid, n) for n in subvols.subvols ) 
    backup_files = set()

    assert False, 'no restore session started'

    for record in self.tx_list:
      if record.r_type == Record.DEL_SNAP:
        # No deleted snap is still present in the filesystem
        assert record.subvol.uuid not in vol_dict
        # Every deleted snap was previously saved to a file
        assert not record.subvol.is_snapshot() or record.subvol.uuid in backup_files, record.subvol
      elif record.r_type == Record.SNAP_TO_FILE:
        # Every snap was saved only once
        assert record.subvol.uuid not in backup_files
        backup_files.add(record.subvol.uuid)
      elif record.r_type == Record.NEW_SNAP:
        # Every new snap has a valid parent in the filesystem
        assert record.subvol.puuid in vol_dict

  def load_from_file_and_check_hash(self):
    logger.info("Loading tx log from %s", self.logfile)
    if not os.path.exists(self.logfile):
      self.save_txlog_header(0, b"\0"*32)
      return []

    tx_list = []
    hasher = hashlib.sha256()
    with open(self.logfile, 'rb') as logfile:
      filehash, hash_domain_upper = self.parse_header_and_advance_file(logfile)
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

  def backup_to_crypted_file():
    logfile = get_txlog().logfile
    back_logfile = '%s/backup_%s_%s' % (get_conf().app.staging_dir, os.path.basename(logfile), timestamp.str)
    hashstr = get_txlog().calculate_and_store_txlog_main_hash()
    get_txlog().record_txlog_to_file(hashstr)

    FileUtils.compress_crypt_file(back_logfile)
    return back_logfile    
  
  def is_empty (self):
    return not self.tx_list

  @staticmethod
  def restore_from_crypted_file(archive_txlog):
    assert not get_txlog().tx_list, "Will not overwrite tx log"
    FileUtils.decompress_decrypt_file( archive_txlog, get_txlog().logfile )
    # forces lazy reloading
    reset_txlog() 

  def add_and_flush_record(self, record):
    with open(self.logfile, 'ab') as logfile:
      self.tx_list.append( record )
      pickle.dump(record, logfile, self.pickle_proto)

  def new_uid(self):
    self.cur_uid += 1
    return self.cur_uid

  def find_last_recorded_snap_uuid(self, subvol):
    for record in reversed(self.tx_list):
      if record.r_type == Record.NEW_SNAP and record.subvol.puuid == subvol.uuid:
        return record.subvol.uuid
    return None    

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

  def record_snap_creation(self, parent, snap):
    record = Record(Record.NEW_SNAP, self.new_uid())
    record.parent = parent
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

  def record_snap_to_file(self, fileout, hashstr, snap, predecessor=None):
    assert fileout and hashstr
    record = Record(Record.SNAP_TO_FILE, self.new_uid())
    record.predecessor = predecessor
    record.subvol = snap
    record.fileout = os.path.basename( fileout )
    record.hashstr = hashstr
    self.add_and_flush_record(record)

  def record_file_to_snap(self, fileout, subvol):
    record = Record(Record.FILE_TO_SNAP, self.new_uid())
    record.subvol = subvol
    record.fileout = os.path.basename( fileout )
    self.recorded_restores.add(subvol.uuid)
    self.add_and_flush_record(record)

  def reverse_iterate_through_records(self):
    # we copy the list to avoid iterating over elements created after the call
    return iter( self.tx_list[::-1] )

  def iterate_through_records(self):
    # we copy the list to avoid iterating over elements created after the call
    return iter( self.tx_list[:] )

  def save_txlog_header (self, hash_domain_upper, filehash):
    if not os.path.exists(self.logfile):
      with open(self.logfile, 'wb') as logfile: pass
      
    with open(self.logfile, 'r+b') as logfile:
      logfile.seek(0, os.SEEK_SET)
      header = struct.pack(TransactionLog.HEADER_STRUCT, TransactionLog.VERSION, hash_domain_upper, filehash)
      assert len(header) == TransactionLog.HEADER_LEN
      logfile.write(header)
    logger.debug("Wrote header : %d, %r", hash_domain_upper, filehash)  

  def parse_header_and_advance_file (self, logfile):
    header_raw = logfile.read(TransactionLog.HEADER_LEN)
    assert len(header_raw) == TransactionLog.HEADER_LEN
    version, hash_domain_upper, main_hash = struct.unpack(TransactionLog.HEADER_STRUCT, header_raw)
    logger.info("Txlog header : version=%d, hash_domain_upper=%d, hash=%r", version, hash_domain_upper, main_hash)
    assert version == TransactionLog.VERSION
    return main_hash, hash_domain_upper

  def validate_main_hash(self, filehash, hash_domain_upper):
    if not hash_domain_upper: return 

    with open(self.logfile, 'rb') as logfile:
      logfile.seek(TransactionLog.HEADER_LEN, os.SEEK_SET)
      data = logfile.read(hash_domain_upper)
    assert len(data) == hash_domain_upper, "%s : %d != %d" % (self.logfile, len(data), hash_domain_upper)
    real_hash = hashlib.sha256(data).digest()
    logger.info("Comparing domain [%d] : %r / %r", hash_domain_upper, filehash, real_hash)
    assert real_hash == filehash

  # meant to be used only during corruption of txlog
  def validate_all_individual_batch_hashes(self):
    segments = []
    batch_hashes = []
    with open(self.logfile, 'rb') as logfile:
      self.parse_header_and_advance_file(logfile)
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

    with open(self.logfile, 'rb') as logfile:
      self.parse_header_and_advance_file(logfile)
      for record,segment in zip(batch_hashes,segments):
        data = logfile.read(segment[1]-segment[0])
        hasher.update(data)
        real_hash = hasher.digest()
        #logger.debug("Validate %d, %r", len(data), data)
        logger.debug("%r ?= %r", real_hash, record.hashstr)
        assert real_hash == record.hashstr, "%r != %r" % (real_hash, record.hashstr)

  def calculate_and_store_txlog_main_hash (self):
    with open(self.logfile, 'rb') as logfile:
      self.parse_header_and_advance_file(logfile)
      data = logfile.read()
    hash_domain_upper = len(data)
    assert hash_domain_upper > 0
    real_hash = hashlib.sha256(data).digest()
    self.save_txlog_header(hash_domain_upper, real_hash) 
    return real_hash

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

singleton_transaction = None
def get_txlog():
  global singleton_transaction
  if not singleton_transaction:
    singleton_transaction = TransactionLog()
  return singleton_transaction  

def reset_txlog():
  global singleton_transaction
  singleton_transaction = None

