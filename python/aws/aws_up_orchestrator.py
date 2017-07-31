from common import *
from aws_session import *
from transaction_log import get_txlog
logger = logging.getLogger(__name__)

# Should work without any parameters from the clients, it must rebuilt everything from the tx log
class AwsUploadOrchestrator:

  def __init__ (self, txlog_checker, glacier_mgr, s3_manager):
    self.txlog_checker = txlog_checker
    self.glacier_mgr = glacier_mgr
    self.s3_mgr = s3_manager

  def upload_all (self):
    self.txlog_checker.check_log_for_upload( get_txlog().iterate_through_records() )
    logger.info("Upload phase starting ...")
    filepaths = self.collect_files_to_upload_since_last_session()

    if not filepaths:
      logger.warning("Found nothing to upload in tx log")
      return None

    session = self.upload_all_subvols_to_glacier(filepaths)
    # txlog uploads are not in session so that they have a consistent sequence of records
    backup_fileseg = self.backup_and_upload_txlog_to_glacier()
    self.upload_txlog_to_s3(backup_fileseg)

    logger.info("Upload finished :\n%s", session.print_summary())
    return session

  def upload_all_subvols_to_glacier (self, filepaths):
    session = AwsGlobalSession.rebuild_from_txlog_or_new_session(Record.SESSION_UPLD)
    if not session:
      session = AwsGlobalSession.start_new(Record.SESSION_UPLD)

    filesegs_left = self.search_filesegs_pending_upload(session, filepaths)
  
    if session.get_pending_glacier_fileseg():
      self.glacier_mgr.finish_pending_upload(session)
    for fileseg in filesegs_left:
      self.glacier_mgr.upload(session, fileseg)

    session.close()
    logger.debug("Filesegs in session :\n%s", session.print_glacier_summary())
    return session

  def backup_and_upload_txlog_to_glacier (self):
    backup_fileseg = Fileseg.build_from_fileout( get_txlog().backup_to_crypted_file() )
    self.glacier_mgr.upload_out_of_session(backup_fileseg)
    return backup_fileseg

  def upload_txlog_to_s3 (self, backup_fileseg):
    # we drop the aws_id and archive_id from glacier
    fileseg = Fileseg.build_from_fileout(backup_fileseg.fileout, backup_fileseg.range_bytes)
    self.s3_mgr.upload_fileseg(fileseg)
    return fileseg

  def collect_files_to_upload_since_last_session (self):
    accumulator = []
    for record in get_txlog().reverse_iterate_through_records():
      if record.r_type == Record.AWS_END and record.session_type == Record.SESSION_UPLD:
        logger.debug('Found %d records for messages to be uploaded', len(accumulator))
        return accumulator

      if record.r_type == Record.SNAP_TO_FILE:
        fileout = os.path.join( get_conf().app.staging_dir, record.fileout )
        assert os.path.isfile(fileout), 'Cannot find file to upload'
        accumulator.append(fileout) 
    return accumulator    

  def search_filesegs_pending_upload (self, session, filepaths):
    # Only files that have not started uploading
    done_files = set( fs.fileout for fs in session.iterate() )
    filesegs_left = [ Fileseg.build_from_fileout(f) for f in filepaths if f not in done_files ]

    logger.debug('Upload session %d/%d files left', len(filesegs_left), len(filepaths))
    if len(filepaths) != len(done_files) + len(filesegs_left):
      logger.warning("Upload session contains files unrelated to backups :\nfilepaths=%r\ndone_files=%r\nfilesegs_left=%r",
        filepaths, done_files, filesegs_left)
    return filesegs_left
  
## END AwsUploadOrchestrator

