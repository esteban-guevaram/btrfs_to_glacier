import unittest as ut
from common import *
from routines_for_test import *

@deco_setup_each_test
class TestBackupFiles (ut.TestCase):
  
  @ut.skip("For quick validation")
  def test_silly_coverage_cases (self):
    assert get_txlog().is_empty()
    # the hash validation should not fail for an empty file
    get_txlog()._validate_main_hash_or_die(b'', 0)
    self.assertTrue( repr(get_txlog()) )
    self.assertEqual( 0, len(get_txlog()) )

    add_fake_backup_to_txlog()
    self.assertTrue( repr(get_txlog()) )
    self.assertTrue( len(get_txlog()) > 0 )

  @ut.skip("For quick validation")
  def test_txlog_unencrypted_backup_restore (self):
    add_fake_backup_to_txlog()
    fileout = get_txlog().backup_to_crypted_file()
    clean_tx_log()
    TransactionLog.restore_from_crypted_file(fileout)
    record_type_count = calculate_record_type_count()
    self.assertEqual(4, record_type_count[Record.NEW_SNAP])
    self.assertEqual(4, record_type_count[Record.SNAP_TO_FILE])
    self.assertEqual(2, record_type_count[Record.DEL_SNAP])

  @ut.skip("For quick validation")
  def test_txlog_encrypted_backup_restore (self):
    get_conf().app.encrypt = True
    self.test_txlog_unencrypted_backup_restore()

  @ut.skip("For quick validation")
  def test_main_hash_protection (self):
    add_fake_backup_to_txlog()
    add_fake_restore_to_txlog()
    get_txlog()._calculate_and_store_txlog_hash()
    filein = get_conf().app.transaction_log

    for i in range(10):
      corrupt_file = modify_random_byte_in_file(filein, TransactionLog.HEADER_LEN)
      get_conf().app.transaction_log = corrupt_file
      reset_txlog()
      with self.assertRaises(Exception):
        logger.warning("Loaded a corrupt tx log = %r", get_txlog())

  @ut.skip("For quick validation")
  def test_recorded_snaps_and_restores (self):
    self.assertEqual(0, len(get_txlog().recorded_snaps))
    self.assertEqual(0, len(get_txlog().recorded_restores))
    add_fake_backup_to_txlog()
    add_fake_restore_to_txlog()
    self.assertEqual(4, len(get_txlog().recorded_snaps))
    self.assertEqual(4, len(get_txlog().recorded_restores))

### END TestTransactionLog

@deco_setup_each_test
class TestTxLogChecker (ut.TestCase):

  @ut.skip("For quick validation")
  def test_check_log_for_backup (self):
    vol1 = DummyBtrfsNode.build()
    snap1 = DummyBtrfsNode.snap(vol1)
    snap11 = DummyBtrfsNode.snap(vol1)

    # empty tx log = ok
    TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # After a previous backup = ok
    add_fake_backup_to_txlog(with_session=True)
    TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # After a backup outside a session = ko
    add_fake_backup_to_txlog(with_session=False)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # Last session not closed
    clean_tx_log()
    get_txlog().record_backup_start()
    add_fake_backup_to_txlog(with_session=False)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # Operation on a snapshot whose parent is not known
    clean_tx_log()
    get_txlog().record_backup_start()
    fake_backup_file_tx(snap11, vol1)
    get_txlog().record_backup_end()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # Send file from a snapshot outside session
    clean_tx_log()
    get_txlog().record_backup_start()
    add_fake_backup_to_txlog(with_session=False)
    # we create record snap1 to add vol1 in the list of subvolumes checked
    get_txlog().record_snap_creation(snap1)
    fake_backup_file_tx(snap11, vol1)
    get_txlog().record_backup_end()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # Deleted snapshot outside of session
    clean_tx_log()
    get_txlog().record_backup_start()
    add_fake_backup_to_txlog(with_session=False)
    # we create record snap1 to add vol1 in the list of subvolumes checked
    get_txlog().record_snap_creation(snap1)
    get_txlog().record_subvol_delete(snap11)
    get_txlog().record_backup_end()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

    # Send file writen after deletion
    clean_tx_log()
    get_txlog().record_backup_start()
    get_txlog().record_snap_creation(snap1)
    get_txlog().record_subvol_delete(snap1)
    fake_backup_file_tx(snap11, vol1)
    get_txlog().record_backup_end()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_backup(get_txlog().iterate_through_records())

  @ut.skip("For quick validation")
  def test_check_log_for_restore (self):
    # Empty txlog nothing to restore = ok
    TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

    # No Completed download session previously
    get_txlog().record_backup_start()
    TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

    # Completed download session previously
    get_txlog().record_aws_session_start(Record.SESSION_DOWN)
    get_txlog().record_aws_session_end(Record.SESSION_DOWN)
    TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

    # Incompleted download session previously
    get_txlog().record_aws_session_start(Record.SESSION_DOWN)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

    # Restore operations recorded outside of session
    add_fake_restore_to_txlog(with_session=False)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

    # Only 1 restore per transaction log
    clean_tx_log()
    add_fake_restore_to_txlog(with_session=True)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_restore(get_txlog().iterate_through_records())

  #@ut.skip("For quick validation")
  def test_check_log_for_upload (self):
    fs1 = Fileseg.build_from_fileout(get_conf().app.staging_dir + '/fs1', (0,2048))
    fs1.aws_id = 'multipart_upload_id1'

    # Empty txlog nothing to upload = ok
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Previous backup session completed = ok
    add_fake_backup_to_txlog(with_session=True)
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Previous upload session completed = ok
    add_fake_upload_to_txlog(with_session=True)
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Resuming from previous pending upload
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    add_fake_upload_to_txlog(with_session=False)
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Resuming from previous pending upload, unfinished fileseg
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    get_txlog().record_fileseg_start(fs1)
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Resuming from previous pending upload, unfinished fileseg with chunks
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    get_txlog().record_fileseg_start(fs1)
    get_txlog().record_chunk_end([0,1024])
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Resuming from previous pending upload, double multipart upload for same fileseg
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    add_fake_upload_to_txlog(with_session=False)
    get_txlog().record_fileseg_start(fs1)
    get_txlog().record_fileseg_start(fs1)
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Resuming from previous pending upload, double multipart upload for same fileseg with chunks
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    add_fake_upload_to_txlog(with_session=False)
    get_txlog().record_fileseg_start(fs1)
    get_txlog().record_chunk_end([0,1024])
    get_txlog().record_fileseg_start(fs1)
    get_txlog().record_chunk_end([0,1024])
    get_txlog().record_chunk_end([1024,2048])
    TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Pending backup session
    clean_tx_log()
    add_fake_backup_to_txlog(with_session=True)
    get_txlog().record_backup_start()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Overlapping backup and upload sessions
    clean_tx_log()
    add_fake_backup_to_txlog(with_session=False)
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    get_txlog().record_backup_end()
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())

    # Overlapping download and upload sessions
    clean_tx_log()
    get_txlog().record_aws_session_start(Record.SESSION_DOWN)
    with self.assertRaises(Exception):
      TxLogConsistencyChecker.check_log_for_upload(get_txlog().iterate_through_records())


  @ut.skip("For quick validation")
  def test_per_restore_batch_hash_protection (self):
    for i in range(3):
      add_fake_backup_to_txlog()
      get_txlog()._calculate_and_store_txlog_hash()
    TxLogConsistencyChecker._validate_all_individual_batch_hashes(get_txlog().logfile)
    
    for j in range(3):
      clean_tx_log()
      change_timestamp()

      for i in range(3):
        add_fake_backup_to_txlog()
        hashstr = get_txlog()._calculate_and_store_txlog_hash()
        if i == j:
          get_txlog()._record_txlog_to_file(hashstr + b"|oops")
        else:
          get_txlog()._record_txlog_to_file(hashstr)

      with self.assertRaises(Exception):
        TxLogConsistencyChecker._validate_all_individual_batch_hashes(get_txlog().logfile)

### END TestTxLogChecker

if __name__ == "__main__":
  conf_for_test()
  ut.main()

