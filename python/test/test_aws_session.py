import unittest as ut
from common import *
from aws_session import *
logger = logging.getLogger(__name__)

class TestAwsGlobalSession (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  #@ut.skip("For quick validation")
  def test_rebuild_done_session(self):
    # no done session case, no sessions at all case
    assert False

  #@ut.skip("For quick validation")
  def test_rebuild_pending_session(self):
    # no pending session case, no sessions at all case
    assert False

### END TestPyBtrfs

if __name__ == "__main__":
  conf_for_test()
  ut.main()


