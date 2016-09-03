from common import *
from aws_glacier_mgr import *
from aws_s3_mgr import *
from aws_glacier_session import *
logger = logging.getLogger(__name__)

# Should work without any parameters from the clients, it must rebuilt everything from the tx log
class AwsOrchestrator:

  def upload_all (self):
    get_txlog().check_log_for_upload()

    session = AwsGlobalSession.rebuild_from_txlog_or_new_session(Record.SESSION_UPLD)
    if not session:
      session = AwsGlobalSession.start_new(Record.SESSION_UPLD)

    self.upload_all_subvols_to_glacier()
    backup_fileseg = self.backup_and_upload_txlog_to_glacier(session)
    self.upload_txlog_to_s3(session, backup_fileseg)
    session.close()

    logger.info("Upload finished :\n%r", session.print_summary())
    return session

  def upload_all_subvols_to_glacier (self, session):
    glacier = AwsGlacierManager()
    filepaths = self.collect_files_to_upload_since_last_session()
    filesegs_left = self.search_filesegs_pending_upload(session, filepaths)
    pending_fileseg = session.get_pending_glacier_fileseg()
  
    if pending_fileseg:
      glacier.finish_upload(session, pending_fileseg)
    for fileseg in filesegs_left:
      glacier.upload(session, fileseg)

    logger.info("Upload finished :\n%r", session.print_glacier_summary())
    return session

  def backup_and_upload_txlog_to_glacier (self, session):
    glacier = AwsGlacierManager()
    backup_fileseg = Fileseg.build_from_fileout( get_txlog().backup_to_crypted_file() )
    glacier.upload(session, backup_fileseg)
    return backup_fileseg

  def upload_txlog_to_s3 (self, session, backup_fileseg):
    s3 = AwsS3Manager()
    fileseg = Fileseg.build_from_fileout(backup_fileseg.fileoutm backup_fileseg.range_bytes)
    s3.upload(session, fileseg)
    return fileseg

  def collect_files_to_upload_since_last_session (self):
    accumulator = []
    for record in get_txlog().reverse_iterate_through_records():
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_UPLD:
        logger.debug('Found %d records for messages to be uploaded', len(accumulator))
        return accumulator

      if record.r_type == Record.SNAP_TO_FILE:
        assert os.isfile(record.fileout), 'Cannot find file to upload'
        accumulator.append(record.fileout) 
    return None    

  def search_filesegs_pending_upload (self, session, filepaths):
    # Only files that have not started uploading
    done_files = set( fs.fileout for fs in session.iterate() )
    filesegs_left = [ Fileseg.build_from_fileout(f) for f in filepaths if f not in done_files ]
    logger.debug('Resuming upload session %d/%d files left', len(filesegs_left), len(filepaths))
    return filesegs_left
  

