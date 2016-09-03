import unittest as ut
from common import *
from aws_orchestrator import *
logger = logging.getLogger(__name__)

class TestAwsOrchestrator (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_for_several_backup_sessions(self):
    assert False

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_with_no_files(self):
    assert False

### END TestPyBtrfs

if __name__ == "__main__":
  conf_for_test()
  ut.main()

