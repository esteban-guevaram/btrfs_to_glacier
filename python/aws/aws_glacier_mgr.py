import botocore.exceptions as botoex
from common import *
from file_utils import *
logger = logging.getLogger(__name__)

class AwsGlacierManager:
  DEFAULT_ACCOUNT = '-'

  def __init__ (self, boto_session):
    self.vault = self._idenpotent_vault_creation(boto_session)
    self.max_chunk_bytes = get_conf().aws.chunk_size_in_mb * 1024**2

  def _idenpotent_vault_creation (self, boto_session):
    glacier = boto_session.resource('glacier')
    vault_name = get_conf().aws.glacier_vault
    logger.debug("Opening vault : %s", vault_name)
    vault = glacier.Vault(AwsGlacierManager.DEFAULT_ACCOUNT, vault_name)
    vault.create()
    return vault


  def upload (self, session, fileseg):
    logger.debug('Uploading into glacier: %r', fileseg)

    if fileseg.range_bytes[1] <= self.max_chunk_bytes:
      archive = self.single_shot_upload(session, fileseg)
    else:
      archive = self.multi_part_upload(session, fileseg)
    return archive  

  def upload_out_of_session (self, fileseg):
    logger.debug('Uploading (no session) into glacier: %r', fileseg)

    if fileseg.range_bytes[1] <= self.max_chunk_bytes:
      archive = self.single_shot_upload(None, fileseg)
    else:
      assert False, 'Out of session uploads do not support multipart'
    return archive  

  def finish_pending_upload (self, session):
    fileseg = session.get_pending_glacier_fileseg()
    logger.debug('Finish uploading : %r', fileseg)

    if fileseg.range_bytes[1] <= self.max_chunk_bytes:
      archive = self.single_shot_upload(session, fileseg)
    else:
      multipart_job = self.attempt_pending_multipart_job_retrieval(fileseg)
      if multipart_job:
        fileseg.clean_pending_chunk()
        archive = self.finish_multi_part_upload(multipart_job, session, fileseg)
      else:
        fileseg.clean_chunks()
        archive = self.multi_part_upload(session, fileseg)
    return archive

  def finish_job_ouput_to_fs (self, session, aws_job):
    pending_fs = session.get_pending_glacier_fileseg()
    pending_fs.clean_pending_chunk()

    range_to_download = self.determine_range_to_download(aws_job, pending_fs)
    logger.info("Download %r to fileseg %r", range_to_download, pending_fs)

    for chunk_range in range_bytes_it(range_to_download, self.max_chunk_bytes):
      self.get_output_chunk_and_write_to_fileout(session, chunk_range, aws_job, pending_fs.key())

    session.close_fileseg(pending_fs.key())
    return session.filesegs[pending_fs.key()]

  def get_job_ouput_to_fs (self, session, aws_job, fileseg):
    range_to_download = self.determine_range_to_download(aws_job, fileseg)
    logger.info("Download %r to fileseg %r", range_to_download, fileseg)
    fileseg.aws_id = aws_job.id
    session.start_fileseg(fileseg)

    for chunk_range in range_bytes_it(range_to_download, self.max_chunk_bytes):
      self.get_output_chunk_and_write_to_fileout(session, chunk_range, aws_job, fileseg.key())

    session.close_fileseg(fileseg.key())
    return session.filesegs[fileseg.key()]

  def initiate_archive_retrieval (self, session, fileseg):
    # note that this mutates filesegs_left adding the aws job id
    assert fileseg.archive_id
    archive = self.vault.Archive(fileseg.archive_id)
    jobParameters = {
      "Type" : "archive-retrieval", # WTF job.action is "ArchiveRetrieval"
      "Tier" : "Bulk", # cheapest retrieval option
      "Description" : fileseg.fileout,
      "ArchiveId" : archive.id,
      "RetrievalByteRange" : build_arch_retrieve_range(fileseg.range_bytes),
    }

    logger.info("Submitting job : %r", jobParameters)
    with tx_handler():
      job = retry_operation (
        lambda : archive.initiate_inventory_retrieval(**jobParameters),
        botoex.ClientError
      )
      assert job.id
      fileseg.aws_id = job.id
      session.add_download_job(fileseg)
    return job

  def get_all_down_jobs_in_vault_by_stx (self):
    max_jobs = get_conf().aws.glacier_max_jobs_in_flight
    result = {}
    for job in self.vault.jobs.all():
      if job.action != 'ArchiveRetrieval': continue
      if job.status_code not in result:
        result[job.status_code] = []
      result[job.status_code].append(job)  

    logger.debug("Glacier job status : %r", [ (k, len(v)) for k,v in result.items() ])
    if 'InProgress' in result and len(result['InProgress']) > max_jobs:
      logger.warning("Too many jobs in progress : %d", len(result['InProgress']))
    return result


  # Session can be null if we do not want to log anything in to the txlog
  def single_shot_upload (self, session, fileseg):
    byte_array = read_fileseg(fileseg)
    kwargs = {
      'body' : byte_array,
      'archiveDescription' : fileseg.get_aws_arch_description(),
      'checksum' : TreeHasher().digest_single_shot_as_hexstr(byte_array),
    }

    # do not add a start fileseg to the txlog if we are resumning the download
    if session and fileseg.key() not in session.filesegs:
      session.start_fileseg_single_chunk(fileseg)

    archive = retry_operation (
      lambda : self.vault.upload_archive(**kwargs),
      botoex.ClientError
    )
    assert archive.id

    if session:
      session.close_fileseg_single_chunk(fileseg.key(), archive.id)
    return archive

  def multi_part_upload (self, session, fileseg):
    kwargs = {
      'partSize' : str(self.max_chunk_bytes),
      'archiveDescription' : fileseg.get_aws_arch_description(),
    }

    with tx_handler():
      multipart_job = retry_operation (
        lambda : self.vault.initiate_multipart_upload(**kwargs),
        botoex.ClientError
      )
      assert multipart_job.id
      fileseg.aws_id = multipart_job.id

      if fileseg.key() not in session.filesegs:
        session.start_fileseg(fileseg)

    hasher = TreeHasher()
    for chunk_range in range_bytes_it(fileseg.range_bytes, self.max_chunk_bytes):
      self.upload_chunk(hasher, multipart_job, session, fileseg, chunk_range)
    
    kwargs = {
      'archiveSize' : str(fileseg.range_bytes[1]),
      'checksum' : hasher.digest_all_parts_as_hexstr(),
    }
    response = retry_operation (
      lambda : multipart_job.complete(**kwargs),
      botoex.ClientError
    )
    assert is_between(int(response['ResponseMetadata']['HTTPStatusCode']), 200, 300)
    archive = self.vault.Archive( response['archiveId'] )

    session.close_fileseg(fileseg.key(), archive.id)
    return archive

  def finish_multi_part_upload (self, multipart_job, session, fileseg):
    hasher = TreeHasher()
    uploaded_count = self.load_hasher_with_uploaded_chunks_checksums(multipart_job, hasher)
    assert uploaded_count == len(fileseg.chunks)
    remaining_range = fileseg.calculate_remaining_range()

    for chunk_range in range_bytes_it(remaining_range, self.max_chunk_bytes):
      self.upload_chunk(hasher, multipart_job, session, fileseg, chunk_range)
    
    kwargs = {
      'archiveSize' : str(fileseg.range_bytes[1]),
      'checksum' : hasher.digest_all_parts_as_hexstr(byte_array),
    }
    response = retry_operation (
      lambda : multipart_job.complete(**kwargs),
      botoex.ClientError
    )
    assert is_between(int(response['ResponseMetadata']['HTTPStatusCode']), 200, 300)
    archive = self.vault.Archive( response['archiveId'] )

    session.close_fileseg(fileseg.key(), archive.id)
    return archive

  def upload_chunk (self, hasher, multipart_job, session, fileseg, chunk_range):
    logger.debug("Uploading chunk : %s %r", fileseg.fileout, chunk_range)
    byte_array = read_fileseg(fileseg, chunk_range)

    hasher.update_chunk(byte_array)
    kwargs = {
      'body' : byte_array,
      'range' : build_mime_range(chunk_range),
      'checksum' : hasher.digest_chunk_as_hexstr(),
    }

    session.start_chunk(fileseg.key(), chunk_range)
    chunk = retry_operation (
      lambda : multipart_job.upload_part(**kwargs),
      botoex.ClientError
    )
    assert is_between(int(chunk['ResponseMetadata']['HTTPStatusCode']), 200, 300)
    session.close_chunk(fileseg.key())
    return chunk

  def get_output_chunk_and_write_to_fileout (self, chunk_range, aws_job, fskey):
    logger.debug("Downloading chunk %r", chunk_range)
    session.start_chunk(fskey, chunk_range)
    fileseg = session.filesegs[fskey]

    body_bytes = retry_operation (
      self._build_download_and_check_closure(chunk_range, aws_job),
      (botoex.ClientError, IOError)
    )
    assert body_bytes

    write_fileseg(fileseg, chunk_range, body_bytes)
    session.close_chunk(fskey)

  def _build_download_and_check_closure (self, chunk_range, aws_job):
    def __closure__():
      str_range = build_arch_retrieve_range(chunk_range),
      response = aws_job.get_output(range=str_range)

      body_bytes = response['body'].read()
      checksum = TreeHasher.digest_single_shot_as_hexstr(body_bytes)

      if checksum != response['checksum']: 
        raise IOError('Checksum mismatch : %s / %s' % (checksum, response['checksum']))
      return body_bytes  
    return __closure__  

  def attempt_pending_multipart_job_retrieval (self, fileseg):
    multipart_job = self.vault.MultipartUpload(fileseg.aws_id)
    try:
      # we expect this to raise a client exception if the job is not valid anymore
      multipart_job.load()
      assert multipart_job.archive_description == fileseg.fileout
      return multipart_job
    except botoex.ClientError:
      logger.warning('MultipartUpload %r is not valid anymore', fileseg.aws_id)
    return None

  def load_hasher_with_uploaded_chunks_checksums (self, multipart_job, hasher):
    uploaded_count = 0
    marker = None

    # We expect parts to be returned in range order
    while True:
      response = retry_operation (
        lambda : multipart_job.parts(marker=marker),
        botoex.ClientError
      )
      assert len(response)

      if 'Parts' in response and response['Parts']:
        # the only part which is smaller than the chunk size is the last one, but this method should not be called on a fully uploaded fileseg
        assert int(response['PartSizeInBytes']) == self.max_chunk_bytes

        for part in response['Parts']:
          hasher.add_chunk_hash_as_hexstr(part['SHA256TreeHash'])
        uploaded_count += len(response['Parts'])
      
      if not response.get('Marker'):
        break
    return uploaded_count
  
  def determine_range_to_download (self, aws_job, fileseg):
    job_range = build_range_from_mime(aws_job.retrieval_byte_range)
    remain_range = fileseg.calculate_remaining_range()
    assert range_contains(remain_range, job_range), "Job retrieval range must contain the fileseg byte range"
    return remaining_range

## END AwsGlacierManager

