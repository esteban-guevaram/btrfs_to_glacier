import unittest as ut
from common import *
from routines_for_test import *

@deco_setup_each_test
class TestBtrfsCommands (ut.TestCase):
  
  @classmethod
  def setUpClass(klass):
    setup_filesystem(['-s'], get_conf().btrfs.target_subvols)
    pass

  #@ut.skip("For quick validation")
  def test_sendfile_unencrypted_backup_restore (self):
    btrfs_cmd = BtrfsCommands(BtrfsSubvolList, FileUtils)
    restore_path = get_conf().btrfs.restore_path

    subvols = BtrfsSubvolList.get_subvols_from_filesystem(get_conf().test.root_fs)
    snap = next( s for s in subvols if s.is_snapshot() )
    session = BackupSession.start_new()
    session.record_snap_creation(snap)
    fileout = btrfs_cmd.send_volume(session, snap)

    record = next( r for r in get_txlog().iterate_through_records() if r.r_type == Record.SNAP_TO_FILE )
    restored = btrfs_cmd.receive_volume(RestoreSession.start_new(), record.subvol, record.hashstr, record.fileout, restore_path)
    self.assertEqual(0, compare_all_in_dir(snap.path, restored.path))

  #@ut.skip("For quick validation")
  def test_sendfile_encrypted_backup_restore (self):
    get_conf().app.encrypt = True
    self.test_sendfile_unencrypted_backup_restore()

### END TestBtrfsCommands

if __name__ == "__main__":
  conf_for_test()
  ut.main()

