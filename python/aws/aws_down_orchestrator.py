from common import *
from aws_session import *
from transaction_log import get_txlog
logger = logging.getLogger(__name__)

# Should work without any parameters from the clients, it must rebuilt everything from the tx log
# Scheduling rules : 
# - only one segment of the same file can be scheduled per quota
# - no new job is submitted to glacier before the pending ones are complete
# - at most one download in flight at any given time
# - files within the same quota can downloaded in arbitrary order
# - but they are ordered with respect to the previous and next quota 
# (otherwise segments of a same file may be concatenated in the wrong order)
class AwsDowloadOrchestrator:

  def __init__ (self, txlog_checker, glacier_mgr, s3_manager, emergency_mgr):
    self.txlog_checker = txlog_checker
    self.glacier_mgr = glacier_mgr
    self.s3_mgr = s3_mgr
    self.emergency_mgr = emergency_mgr

  def download_all (self):
    self.txlog_checker.check_for_download( get_txlog().iterate_through_records() )
    logger.info("Download phase starting ...")

    # we expect each fileseg in session to contain the glacier archive_id
    session = AwsGlobalSession.rebuild_from_txlog_or_new_session(Record.SESSION_DOWN)
    if not session:
      session = AwsGlobalSession.start_new(Record.SESSION_DOWN)

    self.download_all_subvols_from_glacier(session)
    session.close()

    logger.info("Download finished :\n%r", session.print_summary())
    return session

  def load_txlog_from_aws_or_local_copy ():
    # If you want to use s3 txlog instead of local you must delete it manually (avoid potential mistakes!)
    if not get_txlog().is_empty():
      logger.info("Using local txlog with %d records", len(get_txlog()))
      return

    logger.info("Remote txlog will be used, downloading it")
    back_logfile = '%s/backup_%s_%s' % (get_conf().app.staging_dir, get_conf().app.transaction_log, timestamp.new())
    fileseg = self.s3_mgr.download_most_recent_txlog(back_logfile)

    if not fileseg:
      logger.warn("No local or s3 txlog, last chance is to look in glacier !")
      fileseg = self.emergency_mgr.download_last_txlog(get_conf().app.transaction_log, back_logfile)

    TransactionLog.restore_from_crypted_file(fileseg.fileout)
    return back_logfile

  def download_all_subvols_from_glacier (session):
    filesegs = self.find_all_archives_in_glacier()
    total_download_size = sum_fs_size(filesegs)

    filesegs_left = self.filter_filesegs_downloaded_in_session(session, filesegs)
    filesegs_left = self.split_fs_according_to_existing_down_jobs(session, filesegs_left)
    bandwith_quota = self.calculate_initial_quota_from_restored_session(session)

    while True:
      assert_scheduler_invariant(session, bandwith_quota, filesegs_left, total_download_size)
      bandwith_quota, filesegs_left = self.schedule_tasks(bandwith_quota, session, filesegs_left)
      if not filesegs_left: break
      wait_for_polling_period()

    logger.info("Download finished :\n%r", session.print_glacier_summary())
    assert sum_fs_size(session.iterate()) == total_download_size
    return session

  def schedule_tasks (self, bandwith_quota, session, filesegs_left):
    has_expired = bandwith_quota.is_expired()
    all_jobs = self.glacier_mgr.get_all_down_jobs_in_vault_by_stx()
    submitted_jobs_done = self.check_if_filesegs_in_quota_done(session, bandwith_quota)
    jobs_ready = self.poll_for_complete_job_not_downloaded(session, bandwith_quota, all_jobs)
    expired_fs = self.look_for_fs_with_expired_job_in_quota(session, bandwith_quota, filesegs_left, all_jobs)

    if has_expired and submitted_jobs_done:
      bandwith_quota = self.allocate_filesegs_to_new_quota(filesegs_left)
      self.submit_down_jobs_for_fs_in_quota(session, bandwith_quota, filesegs_left)
    elif jobs_ready:
      # during this block session and filesegs_left may overlap breaking the main invariant
      job_selected = self.download_job_output(session, filesegs_left, jobs_ready)
      filesegs_left = self.filter_job_downloaded_from(filesegs_left, job_selected)
    elif expired_fs:
      self.resubmit_fs_down_jobs_in_quota(session, filesegs_left, expired_fs)
    else:
      logger.debug("Scheduler woken up but nothing to do ...")
      self.warn_about_timeout_jobs(all_jobs)

    logger.info( self.print_download_status(session, bandwith_quota, filesegs_left) )
    return bandwith_quota, filesegs_left

  def assert_scheduler_invariant (self, session, bandwith_quota, filesegs_left, total_download_size):
    # Invariant : the datalen of all the filesegs in the session and filesegs_left is constant
    #
    # |---:-----:------:-------:------:----||---:-----:------------:--------------:----------|
    #        session                                               filesegs_left
    #                          |------:----:----:-----|
    #                                bandwith_quota
    # [i1 , i2  , i3   , i4    , i5   , i6 , i7 , i8  ]
    #        session submitted download jobs
    #
    session_len = sum_fs_size(session.iterate())
    fs_left_len = sum_fs_size(filesegs_left)
    keys_with_job = session.view_fskey_with_submitted_job()
    assert fs_left_len + session_len == total_download_size
    assert all( k in keys_with_job for k in bandwith_quota.submitted_fileseg_keys )
    return True

  def allocate_filesegs_to_new_quota (self, filesegs_left):
    quota = BandwithQuota()
    cursor = 0

    while True:
      left, tolerated = quota.space_left()
      if left < 1: break

      if tolerated > filesegs_left[cursor].len():
        quota.add_to_submit( filesegs_left[cursor] )
        cursor += 1
      else:
        fs1, fs2 = self.split_on_chunk_boundary_until(tolerated, filesegs_left[cursor])
        del filesegs_left[cursor]
        filesegs_left.insert(cursor, fs2)
        filesegs_left.insert(cursor, fs1)
        quota.add_to_submit( filesegs_left[cursor] )
     
    logger.info("New quota created : %r", quota)
    return quota

  def submit_down_jobs_for_fs_in_quota (self, session, bandwith_quota, filesegs_left):
    for fs in filesegs_left:
      if fs.key() in bandwith_quota.submitted_fileseg_keys:
        self.glacier_mgr.initiate_archive_retrieval(session, fs)

  def resubmit_fs_down_jobs_in_quota (self, session, filesegs_left, expired_fs):
    # the references in expired_fs should point to the filesegs in session or filesegs_left
    for fs in expired_fs:
      self.glacier_mgr.initiate_archive_retrieval(session, fs)

  def download_job_output (session, filesegs_left, jobs_ready):
    # Since concurrent fileseg download in not allowed, the restore session pending fileseg takes priority
    fs = session.get_pending_glacier_fileseg()
    matching_job = fs and next((j for j in jobs_ready if j.id == fs.awd_id), None)

    if fs and not matching_job:
      logger.warn("%d jobs ready but none for pending fileseg %r", len(jobs_ready), fs)
      return None

    if fs and matching_job:
      self.glacier_mgr.finish_job_ouput_to_fs(session, matching_job)

    else:
      matching_job = jobs_ready[0]
      fs = next( fs for fs in filesegs_left
                 if filesegs_left[index].aws_id == matching_job.id )
      self.glacier_mgr.get_job_ouput_to_fs(session, matching_job, fs)

    assert fs.done
    return matching_job

  def filter_job_downloaded_from (filesegs_left, aws_job):
    # This can be a no-op if :
    # - the aws job corresponded with the pending fileseg in the restored session
    # - there is a pending fileseg but its corresponding job is not ready
    if aws_job:
      return filesegs_left[:]
    else:
      return [ fs for fs in filesegs_left if fs.aws_id != aws_job.id ]

  def check_if_filesegs_in_quota_done (self, session, bandwith_quota):
    return all( key in session.filesegs and session.filesegs[key].done
                for key in bandwith_quota.submitted_fileseg_keys )

  def poll_for_complete_job_not_downloaded (self, session, bandwith_quota, all_jobs):
    already_done_ids = set( fs.aws_id for fs in session.iterate()
                            if fs.key() in bandwith_quota.submitted_fileseg_keys 
                               and fs.done )
    jobs_ready = [ job for job in all_jobs['InProgress'] 
                   if job.id not in already_done_ids ]
    return jobs_ready

  def warn_about_timeout_jobs (self, all_jobs):
    now = datetime.datetime.now()
    timeout_jobs = [ job for job in all_jobs['InProgress']
                     if job.id not in already_done_ids 
                        and BandwithQuota.is_in_timeout(now, job.creation_date) ]
    
    if timeout_jobs:
      logger.warn("Timeout jobs found : %r", timeout_jobs)
    return timeout_jobs

  def look_for_fs_with_expired_job_in_quota (self, session, bandwith_quota, filesegs_left, all_jobs):
    failed_ids = set( job.id for job in all_jobs['Failed'] )
    current_ids = set( job.id for job in flatten_dict(all_jobs, 'Succeded', 'InProgress') )
    expired_fs = []
    expired_id = []

    for fs in self._get_fs_in_quota_it(session, bandwith_quota, filesegs_left):
      assert fs.aws_id
      fs_job_failed = not fs.done and fs.aws_id in failed_ids
      fs_job_not_found = not fs.done and fs.aws_id not in current_ids

      if fs_job_failed or fs_job_not_found:
        expired_id.append(fs.aws_id)
        fs.aws_id = None
        expired_fs.append(fs)

    logger.warn( "Filesegs with expired job found : %r" % expired_id )
    return expired_fs

  # It is important the return value is a list because we want to preserve the order for downloads
  def find_all_archives_in_glacier (self):
    # we expect each item in filesegs_left to contain the glacier archive_id
    filesegs = []
    fileseg = None

    for record in get_txlog().iterate_through_records():
      if record.r_type == Record.FILESEG_START:
        # Note that we do not take the aws_id since it belongs to the upload job which is irrelevant
        fileseg = Fileseg.build_from_fileout(record.fileout, record.range_bytes)
      if record.r_type == Record.FILESEG_END:
        fileseg.set_done(archive_id)
        filesegs.append(fileseg)
        fileseg = None

      # since you can only download files once we stop at the beginning of the download session
      if record.r_type == Record.AWS_START and record.session_type == Record.SESSION_DOWN:
        break
        
    logger.debug("Found %d filesegs to download", len(filesegs))
    assert filesegs and not fileseg
    return filesegs
  
  def filter_filesegs_downloaded_in_session (self, session, filesegs):
    view = session.view_fileout_to_filesegs()
    filesegs_left = []

    for fs in filesegs:
      filename = os.path.basename(fs.fileout)
      # First we determine the filesegs which were not started in a previous download session
      if filename not in view:
        filesegs_left.append(fs)
      else:
        # The fileseg ranges may be different between upload and download
        substracted_fs = Fileseg.calculate_range_substraction(view[filename], fs)
        if substracted_fs:
          # Resume a session which did not finish its last fileseg
          filesegs_left.append(substracted_fs)
    return filesegs_left

  def split_fs_according_to_existing_down_jobs (self, session, filesegs_left):
    # [ "file1" :  [ fs("aws_i1", [0, 666]), fs("aws_i2", [666, 999]), ... ],
    #   "file2" :  [ fs("aws_i3", [0, 333]) ],
    #   ...
    # ]
    groupby_fileout = itertools.groupby(
      sorted( session.view_fs_with_submitted_job(), key=lambda x:x.key() ),
      lambda x:x.fileout
    )
    groupby_fileout = dict( (fileout, list(it)) for fileout,it in groupby_fileout ) 
    result = []

    for fs_left in filesegs_left:
      if fs_left.fileout not in groupby_fileout: 
        result.append(fs_left)
        continue

      # we clone to avoid sharing objects between filesegs_left and session
      fs_partition = [ copy.copy(fs)
                       for fs in groupby_fileout[fs_left.fileout] 
                         if fs.range_bytes[0] >= fs_left.range_bytes[0]
                            and fs.range_bytes[1] <= fs_left.range_bytes[1] ]
      assert fs_partition
      result.extend(fs_partition)
    return result 

  def calculate_initial_quota_from_restored_session (self, session):
    fs_pending = [ fs for fs in session.view_fs_with_submitted_job() if not fs.done ]
    assert all( fs.aws_id for fs in fs_pending )
    assert sum_fs_size(fs_pending) <= BandwithQuota.MAX_SIZE

    if not fs_pending:
      bandwith_quota = BandwithQuota.build_empty_expired()
    else:
      create_time = self.find_youngest_aws_job_creation_date(fs_pending)
      bandwith_quota = BandwithQuota(create_time)
      for fs in fs_pending: bandwith_quota.add_to_submit(fs)

    logger.debug("Accounting jobs in restored session, initial bandwith quota : %r", bandwith_quota)
    return bandwith_quota

  def find_youngest_aws_job_creation_date(self, fs_pending):
    pending_ids = set( fs.aws_id for fs in fs_pending )
    all_jobs = self.glacier_mgr.get_all_down_jobs_in_vault_by_stx()
    all_jobs_sorted = sorted( flatten_dict(all_jobs), key=lambda x:x.creation_date, reverse=True )
    return next( (j.creation_date for j in all_jobs_sorted if j.id in pending_ids),
                 default=None )

  def split_on_chunk_boundary_until (self, tolerate, fileseg):
    chunk_size = get_conf().aws.chunk_size_in_mb * 1024**2
    chunk_acceptable = tolerated // chunk_size

    if chunk_acceptable:
      fs1 = copy.copy(fileseg)
      fs2 = copy.copy(fileseg)
      fs1.range_bytes = (fileseg.range_bytes[0], fileseg.range_bytes[0] + chunk_size*chunk_acceptable )
      fs2.range_bytes = (range1[1], fileseg.range_bytes[1])
    else:
      fs1 = None
      fs2 = copy.copy(fileseg)
    return fs1, fs2  

  def _get_fs_in_quota_it(self, session, bandwith_quota, filesegs_left):
    left_key_fs = dict( (fs.key(), fs) for fs in filesegs_left ) 
    for key in bandwith_quota.submitted_fileseg_keys:
      if key in session.filesegs:
        yield session.filesegs[key]
      else:  
        yield left_key_fs[key]
    
  def print_download_status (self, session, bandwith_quota, filesegs_left):
    lines = [ " - %r, done=%r" % (k, k in session.filesegs and session.filesegs[k].done)
              for k in bandwith_quota.submitted_fileseg_keys ]
    quota_stx = "\n".join( lines )
    return "Download status : session=%r, filesegs_left=%d\n%s\n" % (session, len(filesegs_left), quota_stx)

## END AwsDowloadOrchestrator

