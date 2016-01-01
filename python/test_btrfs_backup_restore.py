from common import *
from test_common import *
import unittest as ut
from btrfs_commands import *
from transaction_log import get_txlog, Record
logger = logging.getLogger(__name__)

class TestBtrfsBackupRestore (ut.TestCase):

  def setUp(self):
    clean_tx_log()
    setup_filesystem([], get_conf().btrfs.target_subvols)

  def calculate_record_count(self):
    record_type_count = {}
    for record in get_txlog().iterate_through_records():
      if record.r_type not in record_type_count:
        record_type_count[record.r_type] = 0
      record_type_count[record.r_type] += 1
    return record_type_count  

  def test_backup_subvolume (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.subvols if not s.is_snapshot() )

    fileout, subvols = savior.incremental_backup(subvol)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(len(subvols.get_snap_childs(subvol)) == 1)

    # We need to modify the timestamp or there will be filename collision
    timestamp.str += '_1'

    fileout, subvols = savior.incremental_backup(subvol)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(len(subvols.get_snap_childs(subvol)) == 1)

    record_type_count = self.calculate_record_count()
    self.assertEqual(2, record_type_count[Record.BACK_FILE])
    self.assertEqual(2, record_type_count[Record.NEW_SNAP])
    self.assertEqual(1, record_type_count[Record.DEL_SNAP])

### END TestBtrfsBackupRestore

if __name__ == "__main__":
  conf_for_test()
  ut.main()

