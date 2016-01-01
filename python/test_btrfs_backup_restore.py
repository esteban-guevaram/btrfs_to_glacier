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

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = [ s for s in subvols.subvols if not s.is_snapshot() ]

    savior.incremental_backup_all()
    change_timestamp()
    add_rand_file_to_all_targets(targets)
    savior.incremental_backup_all()
    change_timestamp()

    restore_result = savior.restore_all_subvols()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertEqual(2, len(restore_result.child_subvols))
    self.assertEqual(2, len(restore_result.last_childs.values()))
    self.assertEqual(4, sum( len(l) for l in restore_result.child_subvols.values() ))
    self.assertEqual(4, sum( len(l) for l in restore_result.restored_files.values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in restore_result.last_childs.values() ))

    record_type_count = self.calculate_record_count()
    self.assertEqual(4, record_type_count[Record.REST_FILE])

  #@ut.skip("For quick validation")
  def test_restore_filesystem_full_compare (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = [ s for s in subvols.subvols if not s.is_snapshot() ]

    backup_result = savior.incremental_backup_all()
    change_timestamp()
    restore_result = savior.restore_all_subvols( backup_result.back_tx_log )

    for target in targets:
      last_snap = restore_result.last_childs[target.uuid]
      self.assertEqual(0, compare_all_in_dir(target.path, last_snap.path) )

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_progressively (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = [ s for s in subvols.subvols if not s.is_snapshot() ]

    savior.incremental_backup_all()
    change_timestamp()
    restore_result = savior.restore_all_subvols()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in restore_result.last_childs.values() ))
    change_timestamp()

    add_rand_file_to_all_targets(targets)
    savior.incremental_backup_all()
    change_timestamp()
    restore_result = savior.restore_all_subvols()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in restore_result.last_childs.values() ))

    record_type_count = self.calculate_record_count()
    self.assertEqual(4, record_type_count[Record.REST_FILE])

  #@ut.skip("For quick validation")
  def test_backup_filesystem (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = [ s for s in subvols.subvols if not s.is_snapshot() ]

    backup_result = savior.incremental_backup_all()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(all( os.path.isfile(f) for f in backup_result.restored_files.values() ))
    self.assertTrue(all( subvols.get_snap_childs(v) for v in targets ))

    change_timestamp()
    add_rand_file_to_all_targets(targets)

    backup_result = savior.incremental_backup_all()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(all( os.path.isfile(f) for f in backup_result.restored_files.values() ))
    self.assertTrue(all( subvols.get_snap_childs(v) for v in targets ))

    record_type_count = self.calculate_record_count()
    self.assertEqual(4, record_type_count[Record.BACK_FILE])
    self.assertEqual(4, record_type_count[Record.NEW_SNAP])
    self.assertEqual(2, record_type_count[Record.DEL_SNAP])
    self.assertEqual(2, record_type_count[Record.BACK_LOG])

  #@ut.skip("For quick validation")
  def test_backup_subvolume (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.subvols if not s.is_snapshot() )

    fileout, snap = savior.incremental_backup(subvol)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertEqual([snap.uuid], [s.uuid for s in subvols.get_snap_childs(subvol)])

    change_timestamp()
    add_rand_file_to_dir(subvol.path)

    fileout, snap = savior.incremental_backup(subvol)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertEqual([snap.uuid], [s.uuid for s in subvols.get_snap_childs(subvol)])

    record_type_count = self.calculate_record_count()
    self.assertEqual(2, record_type_count[Record.BACK_FILE])
    self.assertEqual(2, record_type_count[Record.NEW_SNAP])
    self.assertEqual(1, record_type_count[Record.DEL_SNAP])

### END TestBtrfsBackupRestore

if __name__ == "__main__":
  conf_for_test()
  ut.main()

