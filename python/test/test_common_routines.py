import unittest as ut
from common import *
from routines_for_test import *

@deco_setup_each_test
class TestBackupFiles (ut.TestCase):
  
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

  #@ut.skip("For quick validation")
  def test_txlog_unencrypted_backup_restore (self):
    add_fake_backup_to_txlog()
    fileout = get_txlog().backup_to_crypted_file()
    clean_tx_log()
    TransactionLog.restore_from_crypted_file(fileout)
    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.NEW_SNAP])
    self.assertEqual(4, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(2, record_type_count[Record.DEL_SNAP])

  #@ut.skip("For quick validation")
  def test_txlog_encrypted_backup_restore (self):
    get_conf().app.encrypt = True
    self.test_txlog_unencrypted_backup_restore()

  #@ut.skip("For quick validation")
  def test_main_hash_protection (self):
    add_fake_backup_to_txlog()
    add_fake_restore_to_txlog()
    get_txlog().calculate_and_store_txlog_main_hash()
    filein = get_conf().app.transaction_log

    for i in range(100):
      corrupt_file = modify_random_byte_in_file(filein, TransactionLog.HEADER_LEN)
      get_conf().app.transaction_log = corrupt_file
      reset_txlog()
      with self.assertRaises(Exception):
        logger.warning("Loaded a corrupt tx log = %r", get_txlog())

  #@ut.skip("For quick validation")
  def test_per_restore_batch_hash_protection (self):
    add_fake_backup_to_txlog()
    good_file = get_txlog().backup_to_crypted_file()
    TxLogConsistencyChecker._validate_all_individual_batch_hashes(get_txlog().logfile)
    
    clean_tx_log()
    for i in range(10):
      add_fake_backup_to_txlog()
      hashstr = get_txlog().calculate_and_store_txlog_main_hash()
      if i == 5:
        get_txlog().record_txlog_to_file(hashstr + b"|oops")
      else:
        get_txlog().record_txlog_to_file(hashstr)
      change_timestamp()

    with self.assertRaises(Exception):
      TxLogConsistencyChecker._validate_all_individual_batch_hashes(get_txlog().logfile)

class TestCommon (ut.TestCase):

  #@ut.skip("For quick validation")
  def test_call_ext_program(self):
    out = call('echo call')
    self.assertEqual('call', out.strip())
    try:
      call('buggy')
      self.fail("Expecting command to fail")
    except: pass  
  
  #@ut.skip("For quick validation")
  def test_temp_files(self):
    with tempfile.TemporaryFile(mode='w+') as out_file:
      out_file.write('chocolat');
      out_file.seek(0, os.SEEK_SET)
      self.assertEqual('chocolat', out_file.read())

### END TestCommon

if __name__ == "__main__":
  conf_for_test()
  ut.main()

