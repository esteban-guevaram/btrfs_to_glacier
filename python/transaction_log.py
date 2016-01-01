import pickle, time
from common import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class TransactionLog (object):

  def __init__(self, name=None):
    self.logfile = name
    if not name:
      self.logfile = get_conf().app.transaction_log
    self.tx_list = self.load_log_from_file()

    if self.tx_list:
      self.cur_uid = self.tx_list[-1].uid + 1  
    else:
      self.cur_uid = 0

  def check_log_for_restore(self):
    send_files = set()
    upld_files = set()
    file_snap = {}
    chain_snap = {}

    for record in self.tx_list:
      if record.r_type == Record.BACK_FILE:
        send_files.add(record.fileout)
        file_snap[record.subvol.uuid] = 1
      elif record.r_type == Record.UPD_FILE:
        upld_files.add(record.fileout)
      elif record.r_type == Record.NEW_SNAP:
        file_snap[record.subvol.uuid] = 0
        origin_uuid = record.subvol.parent.uuid
        if record.parent:
          assert chain_snap.get(origin_uuid) == record.parent.uuid  
        chain_snap[origin_uuid] = record.subvol.uuid

    assert all( v == 1 for v in file_snap.values() ) 
    assert upld_files == send_files
  
  def check_log_consistency(self, subvols):
    vol_dict = dict( (n.uuid, n) for n in subvols.subvols ) 
    snap_count = dict( (n.uuid, 0) for n in subvols.subvols if n.is_snapshot() ) 
    file_count = {}

    for record in self.tx_list:
      if record.r_type == Record.DEL_SNAP:
        assert record.subvol.uuid not in vol_dict
        assert record.subvol.uuid in file_count 
      elif record.r_type == Record.BACK_FILE:
        file_count[record.subvol.uuid] = 1
      elif record.r_type == Record.NEW_SNAP and record.subvol.uuid in snap_count:
        snap_count[record.subvol.uuid] += 1

    assert all( v == 1 for v in snap_count.values() ) 
    assert self.tx_list[-1].r_type == Record.BACK_LOG

  def load_log_from_file(self):
    if not os.path.exists(self.logfile):
      with open(self.logfile, 'w'): pass
      return []

    tx_list = []
    with open(self.logfile, 'r') as logfile:
      try:
        while 1:
          tx_list.append( pickle.load(logfile) )
      except EOFError:
        pass
    return tx_list    

  def add_and_flush_record(self, record):
    with open(self.logfile, 'a') as logfile:
      self.tx_list.append( record )
      pickle.dump(record, logfile)

  def new_uid(self):
    self.cur_uid += 1
    return self.cur_uid

  def find_last_recorded_snap_uuid(self, subvol):
    for record in reversed(self.tx_list):
      if record.r_type == Record.NEW_SNAP:
        return record.subvol.uuid
    return None    

  def record_snap_creation(self, parent, snap):
    record = Record(Record.NEW_SNAP, self.new_uid())
    record.parent = parent
    record.subvol = snap
    self.add_and_flush_record(record)

  def record_subvol_delete(self, subvol):
    record = Record(Record.DEL_SNAP, self.new_uid())
    record.subvol = subvol
    self.add_and_flush_record(record)
  
  def record_glacier_upload(self, fileout, job_id):
    record = Record(Record.UPD_FILE, self.new_uid())
    record.fileout = fileout
    record.job_id = job_id
    self.add_and_flush_record(record)

  def record_backup_tx_log(self, fileout, hashstr):
    record = Record(Record.BACK_LOG, self.new_uid())
    record.fileout = fileout
    record.hashstr = hashstr
    self.add_and_flush_record(record)

  def record_backup_file(self, fileout, hashstr, snap, parent=None):
    record = Record(Record.BACK_FILE, self.new_uid())
    record.parent = parent
    record.subvol = snap
    record.fileout = fileout
    record.hashstr = hashstr
    self.add_and_flush_record(record)

  def record_restore_snap(self, fileout, subvol):
    record = Record(Record.REST_FILE, self.new_uid())
    record.subvol = subvol
    record.fileout = fileout
    self.add_and_flush_record(record)

  def iterate_through_records(self):
    return self.tx_list

  def __repr__(self):
    return "\n".join(repr(r) for r in self.tx_list)

### END TransactionLog

class Record (object):
  DEL_SNAP, NEW_SNAP, BACK_FILE, UPD_FILE, BACK_LOG, REST_FILE = 'DEL_SNAP', 'NEW_SNAP', 'BACK_FILE', 'UPD_FILE', 'BACK_LOG', 'REST_FILE'

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

