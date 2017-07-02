import unittest as ut
from common import *
from routines_for_test import *
from aws_glacier_mgr import *
from aws_session import *
from aws_mock import *
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestAwsGlacierMgr (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    get_conf().aws.chunk_size_in_mb = 1
    self.session = DummySession.create_dummy_session()
    self.glacier_res = self.session.resource('glacier')
    self.glacier_mgr = AwsGlacierManager(self.session)
    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    self.vault = self.glacier_res.Vault('', get_conf().aws.glacier_vault)

  @ut.skip("For quick validation")
  def test_single_shoot_upload(self):
    fileseg = add_rand_file_to_staging(256)
    archive = self.glacier_mgr.upload(up_session, fileseg)

    assert len(self.vault._archives) == 1
    assert len(self.up_session.filesegs) == 1
    assert self.up_session.filesegs[fileseg.key()].range_bytes[1] == 256 * 1024
    assert len(get_txlog()) == 2
    assert any(r for r in get_txlog().iterate_through_records() if r.r_type == Record.FILESEG_START)
    assert any(r for r in get_txlog().iterate_through_records() if r.r_type == Record.FILESEG_END)

  @ut.skip("For quick validation")
  def test_single_shoot_upload_out_of_session(self):
    fileseg = add_rand_file_to_staging(256)
    archive = self.glacier_mgr.upload_out_of_session(fileseg)

    assert len(self.vault._archives) == 1
    assert len(get_txlog()) == 0

    # no out of session uploads for big files
    fileseg = add_rand_file_to_staging(1025)
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload_out_of_session(fileseg)

  @ut.skip("For quick validation")
  def test_single_shoot_upload_failures(self):
    fileseg = add_rand_file_to_staging(256)

    DummySession.behaviour = always_ko_behaviour
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)

    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = True
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    assert archive.id in self.vault._archives

    self.up_session = AwsGlobalSession(Record.SESSION_UPLD)
    DummySession.behaviour = fail_at_first_then_ok(1)
    DummySession.blowup_on_fail = False
    with self.assertRaises(Exception):
      archive = self.glacier_mgr.upload(self.up_session, fileseg)

  #@ut.skip("For quick validation")
  def test_multipart_upload(self):
    fileseg = add_rand_file_to_staging(1256)
    archive = self.glacier_mgr.upload(self.up_session, fileseg)
    # check session modified
    assert False

  @ut.skip("For quick validation")
  def test_multipart_resume_job_valid(self):
    assert False

  @ut.skip("For quick validation")
  def test_multipart_resume_job_expired(self):
    assert False

### END TestAwsGlacierMgr

if __name__ == "__main__":
  conf_for_test()
  ut.main()

