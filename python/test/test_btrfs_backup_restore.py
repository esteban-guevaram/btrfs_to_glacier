from common import *
from routines_for_test import *
import unittest as ut
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestBtrfsBackupRestore (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    setup_filesystem([], get_conf().btrfs.target_subvols)
    klass.sv_count = len(get_conf().btrfs.target_subvols)

  def build_objects (self):
    btrfs_cmd = BtrfsCommands(BtrfsSubvolList, FileUtils)
    back_orch = BtrfsBackupOrchestrator(TxLogConsistencyChecker, btrfs_cmd)
    rest_orch = BtrfsRestoreOrchestrator(TxLogConsistencyChecker, btrfs_cmd)
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    return btrfs_cmd, back_orch, rest_orch, subvols

  #@ut.skip("For quick validation")
  def test_btrfs_commands_empty_args (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    subvols = btrfs_cmd.find_subvols_from_paths([])
    self.assertFalse( subvols )
    snaps = btrfs_cmd.get_snaps_in_txlog_for_subvols({})
    self.assertFalse( snaps )

  #@ut.skip("For quick validation")
  def test_detect_subvols_to_restore_not_uploaded (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    up_session = AwsGlobalSession(Record.SESSION_UPLD)
    targets = subvols.get_main_subvols()

    add_rand_file_to_all_targets(targets)
    backup_info = back_orch.snap_backup_clean_all_targets()

    with use_mock_logger_on_module(btrfs_restore_orchestrator) as mock_logger:
      rest_orch.build_restore_info_from_txlog()
      self.assertEqual( 1, mock_logger.warning.call_count )

  #@ut.skip("For quick validation")
  def test_detect_subvols_to_restore_all_uploaded (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    up_session = AwsGlobalSession(Record.SESSION_UPLD)
    targets = subvols.get_main_subvols()

    add_rand_file_to_all_targets(targets)
    backup_info = back_orch.snap_backup_clean_all_targets()

    for fileouts in backup_info.btrfs_sv_files.values():
      fileseg = Fileseg.build_from_fileout(fileouts[0]) 
      up_session.start_fileseg_single_chunk(fileseg)
      up_session.close_fileseg_single_chunk(fileseg.key(), '123')

    with use_mock_logger_on_module(btrfs_restore_orchestrator) as mock_logger:
      rest_orch.build_restore_info_from_txlog()
      self.assertEqual( 0, mock_logger.warning.call_count )

  #@ut.skip("For quick validation")
  def test_backup_restore_single_sv_one_backup (self):
    btrfs_cmd, back_orch, rest_orch, subvols_before = self.build_objects()
    subvol = next( s for s in subvols_before.get_main_subvols() )
    get_conf().btrfs.target_subvols = [ subvol.path ]

    backup_result = back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)

    self.assertTrue(1, len(restore_result.child_subvols))
    self.assertTrue(1, len(restore_result.btrfs_sv_files))
    self.assertTrue(1, len(restore_result.child_subvols[subvol.uuid]))
    self.assertTrue(1, len(restore_result.btrfs_sv_files[subvol.uuid]))

    restore_snaps = all_child_sv_in_session(restore_result)
    backup_snap_uuids = [ sv.uuid for sv in all_child_sv_in_session(backup_result) ]
    self.assertTrue(all( not subvols_before.get_by_uuid(sv.uuid) for sv in restore_snaps ))
    # all restored snaps should reference in their ruuid the snapshot used to create the send file
    self.assertTrue(all( subvols.get_by_uuid(sv.ruuid) for sv in restore_snaps ))
    self.assertTrue(all( subvols.get_by_ruuid(uuid) for uuid in backup_snap_uuids ))
    self.assertTrue(all( sv.ruuid in backup_snap_uuids for sv in restore_snaps ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(1, record_type_count.get(Record.BACK_START), repr(record_type_count))
    self.assertEqual(1, record_type_count[Record.NEW_SNAP])
    self.assertEqual(1, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(1, record_type_count.get(Record.BACK_END))
    self.assertEqual(1, record_type_count.get(Record.REST_START), repr(record_type_count))
    self.assertEqual(1, record_type_count[Record.FILE_TO_SNAP])
    self.assertEqual(1, record_type_count.get(Record.REST_END))

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_one_backup (self):
    btrfs_cmd, back_orch, rest_orch, subvols_before = self.build_objects()
    targets = subvols_before.get_main_subvols()

    backup_result = back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)

    self.assertTrue(TestBtrfsBackupRestore.sv_count, len(restore_result.child_subvols))
    self.assertTrue(TestBtrfsBackupRestore.sv_count, len(restore_result.btrfs_sv_files))
    self.assertTrue(all( len(restore_result.child_subvols[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( len(restore_result.btrfs_sv_files[v.uuid]) == 1 for v in targets ))

    restore_snaps = all_child_sv_in_session(restore_result)
    backup_snap_uuids = [ sv.uuid for sv in all_child_sv_in_session(backup_result) ]
    self.assertTrue(all( not subvols_before.get_by_uuid(sv.uuid) for sv in restore_snaps ))
    # all restored snaps should reference in their ruuid the snapshot used to create the send file
    self.assertTrue(all( subvols.get_by_uuid(sv.ruuid) for sv in restore_snaps ))
    self.assertTrue(all( subvols.get_by_ruuid(uuid) for uuid in backup_snap_uuids ))
    self.assertTrue(all( sv.ruuid in backup_snap_uuids for sv in restore_snaps ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(1, record_type_count.get(Record.BACK_START), repr(record_type_count))
    self.assertEqual(TestBtrfsBackupRestore.sv_count, record_type_count[Record.NEW_SNAP])
    self.assertEqual(TestBtrfsBackupRestore.sv_count, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(1, record_type_count.get(Record.BACK_END))
    self.assertEqual(1, record_type_count.get(Record.REST_START), repr(record_type_count))
    self.assertEqual(TestBtrfsBackupRestore.sv_count, record_type_count[Record.FILE_TO_SNAP])
    self.assertEqual(1, record_type_count.get(Record.REST_END))

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_several_backups(self):
    btrfs_cmd, back_orch, rest_orch, subvols_before = self.build_objects()
    targets = subvols_before.get_main_subvols()

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    add_rand_file_to_all_targets(targets)

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)

    sum_of_newdel_sv = TestBtrfsBackupRestore.sv_count * get_conf().btrfs.restore_clean_window
    self.assertEqual(TestBtrfsBackupRestore.sv_count, len(restore_result.child_subvols))
    self.assertEqual(sum_of_newdel_sv, sum( len(l) for l in restore_result.child_subvols.values() ))
    self.assertEqual(sum_of_newdel_sv, sum( len(l) for l in restore_result.deleted_subvols.values() ))
    self.assertEqual(TestBtrfsBackupRestore.sv_count*2, sum( len(l) for l in restore_result.btrfs_sv_files.values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(TestBtrfsBackupRestore.sv_count*2, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_full_file_compare (self):
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

    record_type_count = calculate_record_type_count()
    self.assertEqual(1, record_type_count.get(Record.REST_START), repr(record_type_count))
    self.assertEqual(1, record_type_count.get(Record.REST_END))

  #@ut.skip("For quick validation")
  def test_backup_restore_filesystem_progressively (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    back_orch.snap_backup_clean_all_targets()
    save_point = get_txlog().backup_to_crypted_file()
    change_timestamp()

    primary_restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(primary_restore_result).values() ))

    # even cleaning the session, we will not restore twice a snapshot if it is already in the destination filesystem
    restore_txlog_from_file(save_point)
    restore_result = rest_orch.restore_subvols_from_received_files()

    record_type_count = calculate_record_type_count()
    self.assertEqual(0, record_type_count.get(Record.FILE_TO_SNAP, 0) )
    self.assertEqual( 0, len(restore_result.child_subvols) )
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(primary_restore_result).values() ))

    # we backup again, restoring will need to ignore ONLY the existing snapshots matching by ruuid
    restore_txlog_from_file(save_point)
    change_timestamp()
    add_rand_file_to_all_targets(targets)
    back_orch.snap_backup_clean_all_targets()
    change_timestamp()

    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( len(restore_result.child_subvols[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( len(restore_result.btrfs_sv_files[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(primary_restore_result).values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(TestBtrfsBackupRestore.sv_count, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_restore_2_distinct_txlogs (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    targets = subvols.get_main_subvols()

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    primary_restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(primary_restore_result).values() ))

    clean_tx_log()
    change_timestamp()

    add_rand_file_to_all_targets(targets)
    back_orch.snap_backup_clean_all_targets()
    change_timestamp()
    restore_result = rest_orch.restore_subvols_from_received_files()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    self.assertTrue(all( len(restore_result.child_subvols[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( len(restore_result.btrfs_sv_files[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(restore_result).values() ))
    self.assertTrue(all( subvols.get_by_uuid(s.uuid) for s in pull_last_childs_from_session(primary_restore_result).values() ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(TestBtrfsBackupRestore.sv_count, record_type_count[Record.FILE_TO_SNAP])

  #@ut.skip("For quick validation")
  def test_backup_filesystem (self):
    btrfs_cmd, back_orch, rest_orch, subvols_before = self.build_objects()
    targets = subvols_before.get_main_subvols()

    backup_result = back_orch.snap_backup_clean_all_targets()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    send_files = [ f for l in backup_result.btrfs_sv_files.values() for f in l ] 

    self.assertTrue(send_files and all( os.path.isfile(f) for f in send_files ))
    self.assertTrue(targets and all( subvols.get_snap_childs(v) for v in targets ))
    self.assertTrue(all( len(backup_result.child_subvols[v.uuid]) == 1 for v in targets ))
    self.assertTrue(all( len(backup_result.btrfs_sv_files[v.uuid]) == 1 for v in targets ))

    backup_snaps = all_child_sv_in_session(backup_result)
    self.assertTrue(all( subvols.get_by_puuid(v.uuid) for v in targets ))
    self.assertTrue(all( sv.puuid in [v.uuid for v in targets] for sv in backup_snaps ))
    self.assertTrue(all( subvols_before.get_by_uuid(sv.puuid) for sv in backup_snaps ))
    self.assertTrue(all( not subvols_before.get_by_uuid(sv.uuid) for sv in backup_snaps ))

    change_timestamp()
    add_rand_file_to_all_targets(targets)

    backup_result = back_orch.snap_backup_clean_all_targets()
    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    send_files = [ f for l in backup_result.btrfs_sv_files.values() for f in l ] 
    self.assertTrue(all( os.path.isfile(f) for f in send_files ))
    self.assertTrue(all( subvols.get_snap_childs(v) for v in targets ))

    record_type_count = calculate_record_type_count()
    self.assertEqual(2, record_type_count.get(Record.BACK_START), repr(record_type_count))
    self.assertEqual(2, record_type_count.get(Record.BACK_END))
    self.assertEqual(TestBtrfsBackupRestore.sv_count*2, record_type_count.get(Record.SNAP_TO_FILE))
    self.assertEqual(TestBtrfsBackupRestore.sv_count*2, record_type_count.get(Record.NEW_SNAP))

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
  def test_restore_empty_txlog (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    restore_result = rest_orch.restore_subvols_from_received_files()
    self.assertEqual( 0, len(restore_result.child_subvols) )

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

  #@ut.skip("For quick validation")
  def test_avoid_shoot_foot_retore_delete (self):
    btrfs_cmd, back_orch, rest_orch, subvols = self.build_objects()
    main_subvols = subvols.get_main_subvols()
    target = main_subvols[0]
    dont_touch = main_subvols[1]
    get_conf().btrfs.target_subvols = [ target.path ]

    back_orch.snap_backup_clean_all_targets()
    change_timestamp()

    restore_result = rest_orch.restore_subvols_from_received_files()
    change_timestamp()
    add_rand_file_to_all_targets(main_subvols)

    # we create a snapshot to dont_touch sv unrelated to the previous restore session
    reset_conf()
    backup_result = back_orch.snap_backup_clean_all_targets()
    unrelated_snap = backup_result.child_subvols[dont_touch.uuid][0]
    record_type_count = calculate_record_type_count()
    deleted_so_far = record_type_count.get(Record.DEL_SNAP, 0)

    with use_mock_logger_on_module(btrfs_restore_orchestrator) as mock_logger:
      rest_orch.clean_snaps_outside_window(restore_result, unrelated_snap)
      self.assertEqual( 1, mock_logger.warning.call_count )

    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    record_type_count = calculate_record_type_count()
    self.assertEqual( deleted_so_far, record_type_count.get(Record.DEL_SNAP), repr(record_type_count) )
    self.assertEqual( dont_touch.uuid, subvols.get_by_uuid(unrelated_snap.uuid).puuid )

### END TestBtrfsBackupRestore

if __name__ == "__main__":
  conf_for_test()
  ut.main()

