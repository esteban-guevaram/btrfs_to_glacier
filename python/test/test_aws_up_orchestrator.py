import unittest as ut
from common import *
from routines_for_test import *
from aws_glacier_mgr import *
from aws_s3_mgr import *
from aws_up_orchestrator import *
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestAwsUpOrchestrator (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    get_conf().aws.chunk_size_in_mb = 1
    self.session = DummySession.create_dummy_session()
    self.glacier_mgr = AwsGlacierManager(self.session)
    self.s3_mgr = AwsS3Manager(self.session)
    self.orchestrator = AwsUploadOrchestrator(TxLogConsistencyChecker, self.glacier_mgr, self.s3_mgr)
    self.vault = self.session.resource('glacier').Vault('', get_conf().aws.glacier_vault)
    self.bucket = self.session.resource('s3').Bucket(get_conf().aws.s3_bucket)

  def tearDown(self):
    if self.session: self.session._clear()

  @ut.skip("For quick validation")
  def test_orchestrate_upload_empty_txlog(self):
    session = self.orchestrator.upload_all()
    self.assertTrue( not session )
    self.assertEqual( 0, sum( 1 for j in self.vault.jobs.all()) )
    self.assertEqual( 0, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 0, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    self.assertEqual( None, count_per_type.get(Record.AWS_START) )
    self.assertEqual( None, count_per_type.get(Record.AWS_END) )
    self.assertEqual( None, count_per_type.get(Record.TXLOG_TO_FILE) )
    self.assertEqual( None, count_per_type.get(Record.FILESEG_START) )
    self.assertEqual( None, count_per_type.get(Record.FILESEG_END) )
    self.assertEqual( None, count_per_type.get(Record.CHUNK_END) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_for_minimal_session(self):
    backup_file_builder = bake_gaussian_file_builder(2048, 2)
    add_fake_backup_to_txlog_mini(with_session=True, backup_file_builder=backup_file_builder)
    self.orchestrator.upload_all()

    self.assertEqual( 1, len(self.vault.multipart_uploads) )
    self.assertEqual( 2, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_with_failures(self):
    backup_file_builder = bake_gaussian_file_builder(2048, 2)
    add_fake_backup_to_txlog_mini(with_session=True, backup_file_builder=backup_file_builder)

    DummySession.behaviour = always_ko_behaviour()
    DummySession.blowup_on_fail = True
    with self.assertRaises(Exception):
      self.orchestrator.upload_all()

    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyMultiPart])
    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      self.orchestrator.upload_all()

    self.session._clear()
    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyVault])
    DummySession.blowup_on_fail = True
    session = self.orchestrator.upload_all()
    self.assertTrue( session.done )
    self.assertEqual( 1, len(self.vault.multipart_uploads) )
    self.assertEqual( 2, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    self.session._clear()
    DummySession.behaviour = fail_at_first_then_ok(1, black=[DummyMultiPart])
    DummySession.blowup_on_fail = True
    session = self.orchestrator.upload_all()
    self.assertTrue( session.done )
    self.assertEqual( 1, len(self.vault.multipart_uploads) )
    self.assertEqual( 2, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_for_one_backup_small_files(self):
    backup_file_builder = bake_gaussian_file_builder(512, 17)
    add_fake_backup_to_txlog(with_session=True, backup_file_builder=backup_file_builder)
    session = self.orchestrator.upload_all()

    self.assertEqual( 0, len(self.vault.multipart_uploads) )
    self.assertEqual( 5, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    self.assertEqual( None, count_per_type.get(Record.CHUNK_END) )
    self.assertEqual( 0, len(session._submitted_aws_jobs) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_for_one_backup_session(self):
    backup_file_builder = bake_gaussian_file_builder(4096, 666)
    add_fake_backup_to_txlog(with_session=True, backup_file_builder=backup_file_builder)
    session = self.orchestrator.upload_all()

    self.assertEqual( 4, len(self.vault.multipart_uploads) )
    self.assertEqual( 5, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    self.assertEqual( 1, count_per_type.get(Record.AWS_START) )
    self.assertEqual( 1, count_per_type.get(Record.AWS_END) )
    self.assertEqual( 1, count_per_type.get(Record.TXLOG_TO_FILE) )
    self.assertEqual( 4, count_per_type.get(Record.FILESEG_START) )
    self.assertEqual( 4, count_per_type.get(Record.FILESEG_END) )
    self.assertTrue( count_per_type.get(Record.CHUNK_END) > 0 )

    self.assertEqual( True, session.done )
    self.assertEqual( Record.SESSION_UPLD, session.session_type )
    self.assertEqual( 4, len(session.filesegs) )
    self.assertEqual( 4, len(session._submitted_aws_jobs) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_for_several_backup_session(self):
    backup_file_builder = bake_gaussian_file_builder(2048, 111)
    for i in range(3):
      add_fake_backup_to_txlog(with_session=True, backup_file_builder=backup_file_builder)
      add_fake_backup_to_txlog_mini(with_session=True, backup_file_builder=backup_file_builder)
    self.orchestrator.upload_all()

    self.assertEqual( 15, len(self.vault.multipart_uploads) )
    self.assertEqual( 16, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_resumed_missing_just_close(self):
    backup_file_builder = bake_gaussian_file_builder(4096, 666)
    backup_filesegs = add_fake_backup_to_txlog(with_session=True, backup_file_builder=backup_file_builder)
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    upload_filesegs = add_fake_upload_to_txlog_matching_backup(backup_filesegs, with_session=False)
    session = self.orchestrator.upload_all()

    self.assertEqual( 0, len(self.vault.multipart_uploads) )
    self.assertEqual( 1, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    self.assertEqual( 1, count_per_type.get(Record.AWS_END) )
    self.assertEqual( 4, count_per_type.get(Record.FILESEG_START) )
    self.assertEqual( 4, count_per_type.get(Record.FILESEG_END) )
    self.assertEqual( 1, count_per_type.get(Record.TXLOG_TO_FILE) )

    self.assertEqual( True, session.done )
    self.assertEqual( 4, len(session.filesegs) )
    self.assertEqual( 4, len(session._submitted_aws_jobs), repr(session._submitted_aws_jobs) )
    self.assertTrue( all( j in session._submitted_aws_jobs for j in [f.aws_id for f in upload_filesegs]) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_resumed_after_first_record(self):
    backup_file_builder = bake_gaussian_file_builder(4096, 666)
    add_fake_backup_to_txlog(with_session=True, backup_file_builder=backup_file_builder)
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    session = self.orchestrator.upload_all()

    self.assertEqual( 4, len(self.vault.multipart_uploads) )
    self.assertEqual( 5, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    # we do not start a new session
    self.assertEqual( 1, count_per_type.get(Record.AWS_START) )

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_resume_recover_chunks(self):
    backup_file_builder = bake_gaussian_file_builder(3072, 0)
    backup_filesegs = add_fake_backup_to_txlog_mini(with_session=True, backup_file_builder=backup_file_builder)
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    get_txlog().record_fileseg_start(backup_filesegs[0])
    get_txlog().record_chunk_end([0,1024**2])
    get_txlog().record_chunk_end([1024**2,2*1024**2])

    session = self.orchestrator.upload_all()

    count_per_type = calculate_record_type_count()
    session_fileseg = next(session.filesegs.values())
    self.assertEqual( 1, len(self.vault.multipart_uploads) )
    self.assertEqual( 1, len(session.filesegs) )
    self.assertEqual( 3, len(session_fileseg.chunks) )
    self.assertEqual( 3, count_per_type.get(Record.CHUNK_END) )
    self.assertEqual( 1, count_per_type.get(Record.FILESEG_END) )

  #@ut.skip("For quick validation")
  def test_orchestrate_resume_with_unrelated_upload_in_session(self):
    backup_file_builder = bake_gaussian_file_builder(4096, 666)
    backup_filesegs = add_fake_backup_to_txlog_mini(with_session=True, backup_file_builder=backup_file_builder)
    for fs in backup_filesegs: fs.fileout = uuid.uuid4().hex
    get_txlog().record_aws_session_start(Record.SESSION_UPLD)
    upload_filesegs = add_fake_upload_to_txlog_matching_backup(backup_filesegs, with_session=False)
    session = self.orchestrator.upload_all()

    self.assertEqual( 1, len(self.vault.multipart_uploads) )
    self.assertEqual( 2, len(self.vault._archives), repr(self.vault._archives) )
    self.assertEqual( 1, len(self.bucket.objects) )

    count_per_type = calculate_record_type_count()
    self.assertEqual( 2, count_per_type.get(Record.FILESEG_START) )
    self.assertEqual( 2, count_per_type.get(Record.FILESEG_END) )

  @ut.skip("For quick validation")
  def test_orchestrate_upload_resume_some_fileseg_to_finish(self):
    assert False

  @ut.skip("For quick validation")
  def test_orchestrate_upload_nothing_todo_because_of_previous_session(self):
    assert False

  @ut.skip("For quick validation")
  def test_orchestrate_resume_middle_fileseg_but_job_expired(self):
    # verify we end up with something like this :
    # AWS_START, FILESEG_START, CHUNK_END, FILESEG_START, CHUNK_END, CHUNK_END, FILESEG_END ...
    assert False

  @ut.skip("For quick validation")
  def test_orchestrate_resume_with_several_previous_expired_jobs(self):
    # start with a txlog like this :
    # AWS_START, FILESEG_START, CHUNK_END, FILESEG_START, CHUNK_END, CHUNK_END, FILESEG_END,
    #            FILESEG_START, FILESEG_START, CHUNK_END, CHUNK_END, FILESEG_END, ...
    assert False

### END TestAwsUpOrchestrator

if __name__ == "__main__":
  conf_for_test()
  ut.main()

