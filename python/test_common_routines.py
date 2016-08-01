import unittest as ut
from common import *
from routines_for_test import *
from backup_file_commands import *
from btrfs_commands import *

class TestBackupFiles (ut.TestCase):
  
  @classmethod
  def setUpClass(klass):
    setup_filesystem(['-s'], get_conf().btrfs.target_subvols)
    pass

  def setUp(self):
    reset_conf()
    clean_tx_log()
    clean_send_file_staging()
    change_timestamp()

  #@ut.skip("For quick validation")
  def test_sendfile_unencrypted_backup_restore (self):
    restore_path = get_conf().btrfs.restore_path
    subvols = BtrfsSubvolList(get_conf().test.root_fs)
    snap = next( s for s in subvols.subvols if s.is_snapshot() )
    fileout = BtrfsCommands().send_volume(snap)
    record = next( r for r in get_txlog().iterate_through_records() if r.r_type == Record.BACK_FILE )
    restored = BtrfsCommands().receive_volume(subvols, restore_path, record)
    self.assertEquals(0, compare_all_in_dir(snap.path, restored.path))

  #@ut.skip("For quick validation")
  def test_sendfile_encrypted_backup_restore (self):
    get_conf().app.encrypt = True
    self.test_sendfile_unencrypted_backup_restore()

  #@ut.skip("For quick validation")
  def test_txlog_unencrypted_backup_restore (self):
    add_fake_backup_to_txlog()
    fileout = BackupFileCommands.write_tx_log()
    clean_tx_log()
    BackupFileCommands.fetch_tx_log(fileout)
    record_type_count = calculate_record_type_count()
    self.assertEquals(4, record_type_count[Record.NEW_SNAP])
    self.assertEquals(4, record_type_count[Record.BACK_FILE])
    self.assertEquals(2, record_type_count[Record.DEL_SNAP])

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
        logger.debug("Error = %r", get_txlog())

  #@ut.skip("For quick validation")
  def test_per_restore_batch_hash_protection (self):
    add_fake_backup_to_txlog()
    good_file = BackupFileCommands.write_tx_log()
    get_txlog().validate_all_individual_batch_hashes()
    
    clean_tx_log()
    for i in range(10):
      add_fake_backup_to_txlog()
      hashstr = get_txlog().calculate_and_store_txlog_main_hash()
      if i == 5:
        get_txlog().record_backup_tx_log(hashstr + "|oops")
      else:
        get_txlog().record_backup_tx_log(hashstr)
      change_timestamp()

    with self.assertRaises(Exception):
      get_txlog().validate_all_individual_batch_hashes()

class TestCommon (ut.TestCase):

  #@ut.skip("For quick validation")
  def test_call_ext_program(self):
    out = call('echo call')
    self.assertEquals('call', out.strip())
    try:
      call('buggy')
      self.fail("Expecting command to fail")
    except: pass  
  
  #@ut.skip("For quick validation")
  def test_temp_files(self):
    with tempfile.TemporaryFile() as out_file:
      out_file.write('chocolat');
      out_file.seek(0, os.SEEK_SET)
      self.assertEquals('chocolat', out_file.read())

### END TestCommon

if __name__ == "__main__":
  conf_for_test()
  ut.main()

