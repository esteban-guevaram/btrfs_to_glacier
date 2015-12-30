import pickle
from common import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class TransactionLog (object):

  def __init__(self):
    self.logfile = get_conf().app.transaction_log
    self.tx_list = self.load_log_from_file()

    if self.tx_list:
      self.cur_uid = self.tx_list[-1].uid + 1  
    else:
      self.cur_uid = 0

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
  
  def record_backup_file(self, fileout, snap, parent=None):
    record = Record(Record.BACK_FILE, self.new_uid())
    record.parent = parent
    record.subvol = snap
    record.fileout = fileout
    self.add_and_flush_record(record)

### END TransactionLog

class Record (object):
  DEL_SNAP, NEW_SNAP, BACK_FILE = 1,2,3

  def __init__(self, r_type, uid):
    self.r_type = r_type
    self.uid = uid
    self.when = time.time()

### END Record  

singleton_transaction = None
def get_txlog():
  global singleton_transaction
  if not singleton_transaction:
    singleton_transaction = TransactionLog()
  return singleton_transaction  

