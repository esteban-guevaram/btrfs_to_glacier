from common import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

class TransactionLog (object):

  def __init__(self):
    self.logfile = get_conf().app.transaction_log

  def find_last_recorded_snap_uuid(self, subvol):
    pass

  def record_snap_creation(self, parent, snap):
    pass

  def record_subvol_delete(self, subvol):
    pass
  
  def record_backup_file(self, fileout, snap, parent=None):
    pass

### END TransactionLog

singleton_transaction = None
def get_txlog():
  global singleton_transaction
  if not singleton_transaction:
    singleton_transaction = TransactionLog()
  return singleton_transaction  

