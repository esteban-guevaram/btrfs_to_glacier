import unittest as ut
from common import *
from routines_for_test import *
from aws_s3_mgr import *
from aws_mock import *
logger = logging.getLogger(__name__)

@deco_setup_each_test
class TestAwsS3Manager (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  def setUp(self):
    self.session = boto3.session.Session(profile_name='test_s3')

  def tearDown(self):
    s3_session = self.session.resource('s3')
    if s3_session:
      s3_session._clear()

  #@ut.skip("For quick validation")
  def test_simple_failures(self):
    get_conf().aws.chunk_size_in_mb = 1
    fileseg = add_rand_file_to_staging(1055)
    s3_mgr = AwsS3Manager(self.session)

    with self.assertRaises(Exception):
      s3_mgr.upload_txlog(fileseg)

    fileseg = add_rand_file_to_staging(256)
    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      s3_mgr.upload_txlog(fileseg)

    DummySession.behaviour = fail_at_first_then_ok(1)
    s3_mgr.upload_txlog(fileseg)

    # We will not retry in case we have a valid HTTP response but an error code
    DummySession.blowup_on_fail = False
    DummySession.behaviour = fail_at_first_then_ok(1)
    with self.assertRaises(Exception):
      s3_mgr.upload_txlog(fileseg)

  #@ut.skip("For quick validation")
  def test_idempotent_bucket_creation(self):
    s3_res = self.session.resource('s3')
    s3_mgr = AwsS3Manager(self.session)
    s3_mgr = AwsS3Manager(self.session)
    self.assertEqual(1, len(s3_res.buckets))

    get_conf().aws.s3_bucket = 'choco_bucket'
    s3_mgr = AwsS3Manager(self.session)
    s3_mgr = AwsS3Manager(self.session)
    self.assertEqual(2, len(s3_res.buckets))

  #@ut.skip("For quick validation")
  def test_upload_txlog_big(self):
    get_conf().aws.chunk_size_in_mb = 1
    fileseg = add_rand_file_to_staging(1024*3)
    s3_mgr = AwsS3Manager(self.session)

    # too big to be written to s3
    with self.assertRaises(Exception):
      s3_object = s3_mgr.upload_txlog(fileseg)

  #@ut.skip("For quick validation")
  def test_upload_txlog(self):
    get_conf().aws.chunk_size_in_mb = 1
    fileseg = add_rand_file_to_staging(256)
    s3_mgr = AwsS3Manager(self.session)

    s3_object = s3_mgr.upload_txlog(fileseg)
    assert s3_object.bucket_name == get_conf().aws.s3_bucket, '%r / %r' % (s3_object.bucket_name, get_conf().aws.s3_bucket)

  #@ut.skip("For quick validation")
  def test_download_txlog_s3_empty(self):
    get_conf().aws.chunk_size_in_mb = 1
    txlog_target = give_stage_filepath()
    s3_mgr = AwsS3Manager(self.session)

    fileseg = s3_mgr.download_most_recent_txlog(txlog_target)
    assert not fileseg

  #@ut.skip("For quick validation")
  def test_download_txlog(self):
    get_conf().aws.chunk_size_in_mb = 1
    txlog_target = give_stage_filepath()
    s3_mgr = AwsS3Manager(self.session)

    s3_resource = self.session.resource('s3')
    bucket = s3_resource.Bucket(get_conf().aws.s3_bucket)
    bucket.put_object(Key='salut', Body=b'mrmonkey')

    fileseg = s3_mgr.download_most_recent_txlog(txlog_target)
    assert fileseg.fileout == txlog_target
    assert fileseg.range_bytes[0] == 0
    assert fileseg.range_bytes[1] == len('mrmonkey')

  #@ut.skip("For quick validation")
  def test_download_txlog_multi_candidate(self):
    get_conf().aws.chunk_size_in_mb = 1
    txlog_target = give_stage_filepath()
    s3_mgr = AwsS3Manager(self.session)

    s3_resource = self.session.resource('s3')
    bucket = s3_resource.Bucket(get_conf().aws.s3_bucket)
    bucket.put_object(Key='salut', Body=b'mrmonkey')
    bucket.put_object(Key='oldie', Body=b'oops')
    bucket.objects['oldie'].last_modified = datetime.datetime(2001, 1, 1)

    fileseg = s3_mgr.download_most_recent_txlog(txlog_target)
    assert fileseg.range_bytes[1] == len('mrmonkey')

  #@ut.skip("For quick validation")
  def test_download_txlog_failures(self):
    txlog_target = give_stage_filepath()
    s3_mgr = AwsS3Manager(self.session)

    s3_resource = self.session.resource('s3')
    bucket = s3_resource.Bucket(get_conf().aws.s3_bucket)
    bucket.put_object(Key='salut', Body=b'mrmonkey')

    DummySession.behaviour = always_ko_behaviour()
    with self.assertRaises(Exception):
      s3_mgr.download_most_recent_txlog(txlog_target)

    DummySession.behaviour = fail_at_first_then_ok(1)
    s3_mgr.download_most_recent_txlog(txlog_target)

### END TestAwsS3Manager

if __name__ == "__main__":
  conf_for_test()
  ut.main()

