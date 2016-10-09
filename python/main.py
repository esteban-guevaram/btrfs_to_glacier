import boto3
from common import *
from file_utils import *
from txlog_consistency import *
from aws_s3_mgr import *
from aws_glacier_mgr import *
from aws_emergency import *
from aws_up_orchestrator import *
from aws_down_orchestrator import *
from btrfs_backup_orchestrator import *
from btrfs_restore_orchestrator import *
from btrfs_commands import *
from btrfs_subvol_list import *
logger = logging.getLogger(__name__)

def main():
  action = get_conf().app.action
  logger.info("Starting, action %s", action)
  try:
    dispatch_action(action)
  except Exception as error:
    logger.exception("Failed to perform '%s'", action)
  logger.info("All done")

def dispatch_action (action):
  if action == 'noop':
    return
  elif action == 'clean_staging':
    FileUtils.clean_staging_dir()
  elif action == 'backup_local':
    build_and_call_backup_local_orchestrator()
  elif action == 'backup_full':
    build_and_call_backup_local_orchestrator()
    build_and_call_aws_upload_orchestrator()
  elif action == 'aws_upload':
    build_and_call_aws_upload_orchestrator()
  elif action == 'aws_download':
    build_and_call_aws_download_orchestrator()
  elif action == 'restore_local':
    build_and_call_restore_local_orchestrator()
  elif action == 'restore_full':
    build_and_call_aws_download_orchestrator()
    build_and_call_restore_local_orchestrator()
  else:
    raise Exception("Unknown action '%s'" % action)

def build_and_call_backup_local_orchestrator ():
  btrfs_cmds = BtrfsCommands(BtrfsSubvolList, FileUtils)
  orchestrator = BtrfsBackupOrchestrator(TxLogConsistencyChecker, btrfs_cmds)
  return orchestrator.snap_backup_clean_all_targets()

def build_and_call_restore_local_orchestrator ():
  btrfs_cmds = BtrfsCommands(BtrfsSubvolList, FileUtils)
  orchestrator = BtrfsRestoreOrchestrator(TxLogConsistencyChecker, btrfs_cmds)
  return orchestrator.restore_subvols_from_received_files()

def build_and_call_aws_upload_orchestrator ():
  profile = get_conf().aws.profile
  session = boto3.session.Session(profile_name=profile)
  glacier_mgr = AwsGlacierManager(session)
  s3_manager = AwsS3Manager(session)
  orchestrator = AwsUploadOrchestrator(TxLogConsistencyChecker, glacier_mgr, s3_manager)
  return orchestrator.upload_all()

def build_and_call_aws_download_orchestrator ():
  profile = get_conf().aws.profile
  session = boto3.session.Session(profile_name=profile)
  glacier_mgr = AwsGlacierManager(session)
  s3_manager = AwsS3Manager(session)
  emergency_mgr = AwsGlacierEmergency(session)
  orchestrator = AwsDownloadOrchestrator(TxLogConsistencyChecker, glacier_mgr, s3_manager, emergency_mgr)
  return orchestrator.download_all()


if __name__ == '__main__':
  main()

