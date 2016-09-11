import unittest as ut
from common import *
from aws_orchestrator import *
logger = logging.getLogger(__name__)

class TestAwsOrchestrator (ut.TestCase):

  @classmethod
  def setUpClass(klass):
    pass

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_for_one_backup_session_single_file(self):
    assert False

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_for_one_backup_session(self):
    assert False

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_for_resumed_backup_session(self):
    assert False

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_for_several_backup_sessions(self):
    assert False

  #@ut.skip("For quick validation")
  def test_orchestrate_upload_with_no_files(self):
    assert False


  #@ut.skip("For quick validation")
  def test_schedule_download(self):
    # from new session
    # resuming session
    # splitting filesegs
    assert False

### END TestAwsOrchestrator

if __name__ == "__main__":
  conf_for_test()
  ut.main()

