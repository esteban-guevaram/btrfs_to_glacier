import unittest as ut
from common import *
from aws_glacier_mgr import *
from aws_mock import *
logger = logging.getLogger(__name__)

class TestAwsGlacierMgr (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  #@ut.skip("For quick validation")
  def test_single_shoot_upload(self):
    # check session modified
    assert False

  #@ut.skip("For quick validation")
  def test_multipart_upload(self):
    # check session modified
    assert False

  #@ut.skip("For quick validation")
  def test_multipart_resume_job_valid(self):
    assert False

  #@ut.skip("For quick validation")
  def test_multipart_resume_job_expired(self):
    assert False

### END TestAwsGlacierMgr

if __name__ == "__main__":
  conf_for_test()
  ut.main()


