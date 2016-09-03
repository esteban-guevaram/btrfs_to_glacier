from common import *
from routines_for_test import *
import unittest as ut
from btrfs_commands import *
from transaction_log import get_txlog, Record
logger = logging.getLogger(__name__)

class TestBtrfsBackupRestore (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    setup_filesystem([], get_conf().btrfs.target_subvols)

  def setUp(self):
    reset_conf()
    clean_tx_log()
    clean_send_file_staging()
    change_timestamp()

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_from_backup_txlog (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = subvols.get_main_subvols()

    savior.incremental_backup_all()
    change_timestamp()
    add_rand_file_to_all_targets(targets)

    backup_result = savior.incremental_backup_all()
    change_timestamp()
    clean_tx_log()
    restore_result = savior.restore_all_subvols( backup_result.back_tx_log )
    subvols = BtrfsSubvolList(get_conf().test.root_fs)

    self.assertEqual(2, len(restore_result.child_subvols))
    self.assertEqual(2, len(restore_result.last_childs.values()))
    self.assertEqual(4, sum( len(l) for l in restore_result.child_subvols.values() ))
    self.assertEqual(4, sum( len(l) for l in restore_result.restored_files.values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in restore_result.last_childs.values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_restore_filesystem_full_file_compare (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = subvols.get_main_subvols()

    savior.incremental_backup_all()
    change_timestamp()
    restore_result = savior.restore_all_subvols()

    for target in targets:
      last_snap = restore_result.last_childs[target.uuid]
      self.assertEqual(0, compare_all_in_dir(target.path, last_snap.path) )

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_progressively (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = subvols.get_main_subvols()

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

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_filesystem (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    targets = subvols.get_main_subvols()

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

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(4, record_type_count[Record.NEW_SNAP])
    self.assertEqual(2, record_type_count[Record.TXLOG_TO_FILE])

  #@ut.skip("For quick validation")
  def test_backup_single_subvolume (self):
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.get_main_subvols() )

    fileout, snap = savior.incremental_backup(subvol)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(snap.uuid in [s.uuid for s in subvols.get_snap_childs(subvol)])
    self.assertEqual(1, len(get_txlog().recorded_snaps))

    change_timestamp()
    add_rand_file_to_dir(subvol.path)

    fileout, snap = savior.incremental_backup(subvol)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(snap.uuid in [s.uuid for s in subvols.get_snap_childs(subvol)])
    self.assertEqual(2, len(get_txlog().recorded_snaps))

    record_type_count = calculate_record_type_count()
    self.assertEqual(2, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(2, record_type_count[Record.NEW_SNAP])

  #@ut.skip("For quick validation")
  def test_restore_does_not_delete_unrelated_vols (self):
    get_conf().btrfs.restore_clean_window = 4
    get_conf().btrfs.backup_clean_window = 4
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.get_main_subvols() )

    for i in range(2):
      change_timestamp()
      savior.incremental_backup(subvol)
      add_rand_file_to_dir(subvol.path)

    restore_result = savior.restore_all_subvols()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    previous_subvols = subvols.get_readonly_subvols()
    self.assertTrue( len(previous_subvols) > 0 )

    get_conf().btrfs.restore_clean_window = 1
    get_conf().btrfs.backup_clean_window = 1
    clean_tx_log()
    for i in range(2):
      change_timestamp()
      savior.incremental_backup(subvol)
      add_rand_file_to_dir(subvol.path)

    restore_result = savior.restore_all_subvols()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    # None of the previous subvolumes must be deleted
    self.assertTrue( all( subvols.get_by_uuid(s.uuid) != None for s in previous_subvols ) )

  #@ut.skip("For quick validation")
  def test_restore_clean_window (self):
    restore_created = []
    get_conf().btrfs.restore_clean_window = 1
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.get_main_subvols() )

    for i in range(4):
      change_timestamp()
      savior.incremental_backup(subvol)
      add_rand_file_to_dir(subvol.path)

    delete_count = calculate_record_type_count()[Record.DEL_SNAP]
    restore_result = savior.restore_all_subvols()
    delete_count = calculate_record_type_count()[Record.DEL_SNAP] - delete_count
    self.assertEqual(3, delete_count)

    restore_children = restore_result.get_restores_for_subvol(subvol.uuid)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    # All except the latest restore have been deleted from the filesystem
    self.assertEqual(1, sum( 1 for s in restore_children if subvols.get_by_uuid(s.uuid) ) )

    clean_tx_log()
    get_conf().btrfs.restore_clean_window = 5
    for i in range(4):
      change_timestamp()
      savior.incremental_backup(subvol)
      add_rand_file_to_dir(subvol.path)

    delete_count = calculate_record_type_count()[Record.DEL_SNAP]
    restore_result = savior.restore_all_subvols()
    delete_count = calculate_record_type_count()[Record.DEL_SNAP] - delete_count
    self.assertEqual(0, delete_count)

    restore_children = restore_result.get_restores_for_subvol(subvol.uuid)
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    # No restores have been deleted
    self.assertEqual(4, sum( 1 for s in restore_children if subvols.get_by_uuid(s.uuid) ) )

  #@ut.skip("For quick validation")
  def test_backup_clean_window (self):
    snaps_created = []
    get_conf().btrfs.backup_clean_window = 1
    savior = BtrfsCommands()
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    subvol = next( s for s in subvols.get_main_subvols() )

    for i in range(4):
      change_timestamp()
      fileout, snap = savior.incremental_backup(subvol)
      snaps_created.append(snap)
      add_rand_file_to_dir(subvol.path)

    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    record_type_count = calculate_record_type_count()
    self.assertEqual(3, record_type_count[Record.DEL_SNAP])
    # All except the latest snap have been deleted from the filesystem
    self.assertEqual(1, sum( 1 for s in snaps_created if subvols.get_by_uuid(s.uuid) ) )

    get_conf().btrfs.backup_clean_window = 3
    for i in range(4):
      change_timestamp()
      fileout, snap = savior.incremental_backup(subvol)
      snaps_created.append(snap)
      add_rand_file_to_dir(subvol.path)

    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    record_type_count = calculate_record_type_count()
    self.assertEqual(5, record_type_count[Record.DEL_SNAP])
    # All except the latest 3 snaps have been deleted from the filesystem
    self.assertEqual(3, sum( 1 for s in snaps_created if subvols.get_by_uuid(s.uuid) ) )

  #@ut.skip("For quick validation")
  def test_restore_subvol_different_folder_than_record (self):
    assert False

### END TestBtrfsBackupRestore

class TestTreeHasher (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    pass

  #@ut.skip("For quick validation")
  def test_single_shot_calculate (self):
    assert False

### END TestTreeHasher

if __name__ == "__main__":
  conf_for_test()
  ut.main()

