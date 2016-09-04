import unittest as ut
from common import *
from aws_session import *
logger = logging.getLogger(__name__)

class TestAwsS3Manager (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  #@ut.skip("For quick validation")
  def test_idempotent_bucket_creation(self):
    assert False

  #@ut.skip("For quick validation")
  def test_upload_txlog(self):
    # check session modified
    assert False

  #@ut.skip("For quick validation")
  def test_upload_txlog_failure(self):
    assert False

### END TestAwsS3Manager

if __name__ == "__main__":
  conf_for_test()
  ut.main()



