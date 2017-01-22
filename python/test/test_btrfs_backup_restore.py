from common import *
from routines_for_test import *
import unittest as ut
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestBtrfsBackupRestore (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    setup_filesystem([], get_conf().btrfs.target_subvols)
    pass

  def build_objects (self):
    btrfs_cmd = BtrfsCommands(BtrfsSubvolList, FileUtils)
    back_orch = BtrfsBackupOrchestrator(TxLogConsistencyChecker, btrfs_cmd)
    rest_orch = BtrfsRestoreOrchestrator(TxLogConsistencyChecker, btrfs_cmd)
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    return btrfs_cmd, back_orch, rest_orch, subvols

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    add_rand_file_to_all_targets(targets)

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)

    self.assertEqual(2, len(restore_result.child_subvols))
    self.assertEqual(2, sum( len(l) for l in restore_result.child_subvols.values() ))
    self.assertEqual(2, sum( len(l) for l in restore_result.deleted_subvols.values() ))
    self.assertEqual(4, sum( len(l) for l in restore_result.btrfs_sv_files.values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_restore_filesystem_full_file_compare (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    add_rand_file_to_all_targets(targets)
    add_rand_file_to_all_targets(targets)
    back_orch.snap_backup_clean_all_targets()

    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()

    for target in targets:
      last_snap = pull_last_childs_from_session(restore_result)[target.uuid]
      self.assertEqual(0, compare_all_in_dir(target.path, last_snap.path) )

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_progressively (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))
    change_timestamp()

    add_rand_file_to_all_targets(targets)
    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_filesystem (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    backup_result = back_orch.snap_backup_clean_all_targets()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    send_files = [ f for l in backup_result.btrfs_sv_files.values() for f in l ] 
    self.assertTrue(all( os.path.isfile(f) for f in send_files ))
    self.assertTrue(all( subvols.get_snap_childs(v) for v in targets ))

    change_timestamp()
    add_rand_file_to_all_targets(targets)

    backup_result = back_orch.snap_backup_clean_all_targets()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    send_files = [ f for l in backup_result.btrfs_sv_files.values() for f in l ] 
    self.assertTrue(all( os.path.isfile(f) for f in send_files ))
    self.assertTrue(all( subvols.get_snap_childs(v) for v in targets ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(4, record_type_count[Record.NEW_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_single_subvolume (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    subvol = next( s for s in subvols.get_main_subvols() )
    get_conf().btrfs.target_subvols = [ subvol.path ]

    backup_result = back_orch.snap_backup_clean_all_targets()
    fileout, snap = get_send_file_and_snap_from(backup_result, subvol.uuid)
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(snap.uuid in [s.uuid for s in subvols.get_snap_childs(subvol)])
    self.assertEqual(1, len(get_txlog().recorded_snaps))

    change_timestamp()
    add_rand_file_to_dir(subvol.path)

    backup_result = back_orch.snap_backup_clean_all_targets()
    fileout, snap = get_send_file_and_snap_from(backup_result, subvol.uuid)
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(os.path.isfile(fileout))
    self.assertTrue(snap.uuid in [s.uuid for s in subvols.get_snap_childs(subvol)])
    self.assertEqual(2, len(get_txlog().recorded_snaps))

    record_type_count = calculate_record_type_count()
    self.assertEqual(2, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(2, record_type_count[Record.NEW_SNAP])

  #@ut.skip("For quick validation")
  def test_restore_does_not_delete_unrelated_vols (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    subvol = next( s for s in subvols.get_main_subvols() )

    get_conf().btrfs.target_subvols = [ subvol.path ]
    get_conf().btrfs.restore_clean_window = 4
    get_conf().btrfs.backup_clean_window = 4

    for i in range(2):
      change_timestamp()
      back_orch.snap_backup_clean_all_targets()
      add_rand_file_to_dir(subvol.path)

    rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    previous_subvols = subvols.get_readonly_subvols()
    self.assertTrue( len(previous_subvols) > 0 )

    get_conf().btrfs.restore_clean_window = 1
    get_conf().btrfs.backup_clean_window = 1
    clean_tx_log()
    for i in range(2):
      change_timestamp()
      back_orch.snap_backup_clean_all_targets()
      add_rand_file_to_dir(subvol.path)

    rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    # None of the previous subvolumes must be deleted
    self.assertTrue( all( subvols.get_by_uuid(s.uuid) != None for s in previous_subvols ) )

  #@ut.skip("For quick validation")
  def test_restore_clean_window (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    subvol = next( s for s in subvols.get_main_subvols() )

    restore_created = []
    get_conf().btrfs.restore_clean_window = 1
    get_conf().btrfs.target_subvols = [ subvol.path ]

    for i in range(4):
      change_timestamp()
      back_orch.snap_backup_clean_all_targets()
      add_rand_file_to_dir(subvol.path)

    delete_count = calculate_record_type_count()[Record.DEL_SNAP]
    restore_result = rest_orch.restore_subvols_from_received_files()
    delete_count = calculate_record_type_count()[Record.DEL_SNAP] - delete_count
    self.assertEqual(3, delete_count)
    self.assertEqual(3, len(restore_result.deleted_subvols[subvol.uuid]))

    restore_children = restore_result.child_subvols[subvol.uuid]
    deleted_children = restore_result.deleted_subvols[subvol.uuid]
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)

    # All except the latest restore have been deleted from the filesystem
    self.assertTrue( subvols.get_by_uuid(s.uuid) for s in restore_children )
    self.assertTrue( not subvols.get_by_uuid(s.uuid) for s in deleted_children )

    clean_tx_log()
    get_conf().btrfs.restore_clean_window = 5
    for i in range(4):
      change_timestamp()
      back_orch.snap_backup_clean_all_targets()
      add_rand_file_to_dir(subvol.path)

    delete_count = calculate_record_type_count()[Record.DEL_SNAP]
    restore_result = rest_orch.restore_subvols_from_received_files()
    delete_count = calculate_record_type_count()[Record.DEL_SNAP] - delete_count
    self.assertEqual(0, delete_count)
    self.assertFalse(subvol.uuid in restore_result.deleted_subvols)

    restore_children = restore_result.child_subvols[subvol.uuid]
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    # No restores have been deleted
    self.assertEqual(4, sum( 1 for s in restore_children if subvols.get_by_uuid(s.uuid) ) )

  #@ut.skip("For quick validation")
  def test_backup_clean_window (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    subvol = next( s for s in subvols.get_main_subvols() )

    snaps_created = []
    get_conf().btrfs.backup_clean_window = 1
    get_conf().btrfs.target_subvols = [ subvol.path ]

    for i in range(4):
      change_timestamp()
      backup_result = back_orch.snap_backup_clean_all_targets()
      fileout, snap = get_send_file_and_snap_from(backup_result, subvol.uuid)
      snaps_created.append(snap)
      add_rand_file_to_dir(subvol.path)

    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    record_type_count = calculate_record_type_count()
    self.assertEqual(3, record_type_count[Record.DEL_SNAP])
    # All except the latest snap have been deleted from the filesystem
    self.assertEqual(1, sum( 1 for s in snaps_created if subvols.get_by_uuid(s.uuid) ) )

    get_conf().btrfs.backup_clean_window = 3
    for i in range(4):
      change_timestamp()
      backup_result = back_orch.snap_backup_clean_all_targets()
      fileout, snap = get_send_file_and_snap_from(backup_result, subvol.uuid)
      snaps_created.append(snap)
      add_rand_file_to_dir(subvol.path)

    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    record_type_count = calculate_record_type_count()
    self.assertEqual(5, record_type_count[Record.DEL_SNAP])
    # All except the latest 3 snaps have been deleted from the filesystem
    self.assertEqual(3, sum( 1 for s in snaps_created if subvols.get_by_uuid(s.uuid) ) )

### END TestBtrfsBackupRestore

if __name__ == "__main__":
  conf_for_test()
  ut.main()

