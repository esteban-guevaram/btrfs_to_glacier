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

class TestTreeHasher (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    logger.info("*** Running : %r", self.id())

  #@ut.skip("For quick validation")
  def test_odd_number_of_chunks (self):
    one_meg_data = b'mrmonkey' * (1024*128)

    hasher = TreeHasher()
    result = hasher.update_chunk( one_meg_data )
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', binascii.hexlify(result))
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', hasher.digest_chunk_as_hexstr())

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data )
    self.assertEqual(b'43cadff4f1e8cd7e25c13b7dea589588d76c4e4b778c643cad65b305ebe8a801', result)

    hasher = TreeHasher()
    for i in range(5):
      hasher.update_chunk( one_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'5f3f64924317b10b2ddb99299c9efc54d665d3ad94756d08ed6d2f85f5c7c565', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 5 )
    self.assertEqual(b'5f3f64924317b10b2ddb99299c9efc54d665d3ad94756d08ed6d2f85f5c7c565', result)

  #@ut.skip("For quick validation")
  def test_even_number_of_chunks (self):
    one_meg_data = b'mrmonkey' * (1024*128)

    hasher = TreeHasher()
    for i in range(4):
      hasher.update_chunk( one_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

    hasher = TreeHasher()
    for i in range(2):
      hasher.update_chunk( one_meg_data * 2 )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 4 )
    self.assertEqual(b'c2192941a2d88e71d6c43ce12fec25005ed8d3ab187023f340a12d87d3a47171', result)

  #@ut.skip("For quick validation")
  def test_not_multiple_of_meg (self):
    one_meg_data = b'mrmonkey' * (1024*128)
    one_and_something_meg_data = b'mrmonkey' * (1024*171)
    less_than_meg_data = b'mrmonkey' * (1024*21)

    hasher = TreeHasher()
    hasher.update_chunk( one_and_something_meg_data )
    self.assertEqual(b'4457113e30c619a969c48a7f5456f3b1d076e5f41dae02a453a87b67473eb93a', hasher.digest_chunk_as_hexstr())

    hasher = TreeHasher()
    for i in range(4):
      hasher.update_chunk( one_meg_data )
    hasher.update_chunk( less_than_meg_data )
    result = hasher.digest_all_parts_as_hexstr()
    self.assertEqual(b'6d85431fdba614967e3bfae5569e301737f5fac1ab1dc1da8e01e6613e53a8ef', result)

    hasher = TreeHasher()
    result = hasher.digest_single_shot_as_hexstr( one_meg_data * 4 + less_than_meg_data )
    self.assertEqual(b'6d85431fdba614967e3bfae5569e301737f5fac1ab1dc1da8e01e6613e53a8ef', result)

### END TestTreeHasher

if __name__ == "__main__":
  conf_for_test()
  ut.main()

