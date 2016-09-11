import boto3, botocore.exceptions as botoex
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

  def finish_upload (self, session):
    fileseg = session.get_pending_glacier_fileseg()
    logger.debug('Finish uploading : %r', fileseg)

    if fileseg.range_bytes[1] <= self.max_chunk_bytes:
      session.clean_pending_fileseg()
      fileseg.clean_chunks()
      archive = self.single_shot_upload(session, fileseg)
    else:
      multipart_job = self.attempt_pending_multipart_job_retrieval(fileseg)
      if multipart_job:
        fileseg.clean_pending_chunk()
        archive = self.finish_multi_part_upload(multipart_job, session, fileseg)
      else:
        session.clean_pending_fileseg()
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
      "Description" : fileseg.fileout,
      "ArchiveId" : archive.id,
      "RetrievalByteRange" : build_arch_retrieve_range(fileseg.range_bytes),
    }

    logger.info("Submitting job : %r", jobParameters)
    job = retry_operation (
      lambda : archive.initiate_inventory_retrieval(**kwargs),
      botoex.ClientError
    )

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
      logger.warn("Too many jobs in progress : %d", len(result['InProgress']))
    return result


  def single_shot_upload (self, session, fileseg):
    byte_array = read_fileseg(fileseg)
    kwargs = {
      'body' : byte_array,
      'archiveDescription' : fileseg.get_aws_arch_description(),
      'checksum' : TreeHasher().digest_single_shot_as_hexstr(byte_array),
    }

    session.start_fileseg_single_chunk(fileseg)
    archive = retry_operation (
      lambda : self.vault.upload_archive(**kwargs),
      botoex.ClientError
    )
    session.close_fileseg_single_chunk(fileseg.key(), archive.id)
    return archive

  def multi_part_upload (self, session, fileseg):
    kwargs = {
      'partSize' : str(self.max_chunk_bytes),
      'archiveDescription' : fileseg.get_aws_arch_description(),
    }
    multipart_job = retry_operation (
      lambda : self.vault.initiate_multipart_upload(**kwargs),
      botoex.ClientError
    )
    fileseg.aws_id = multipart_job.id
    session.start_fileseg(fileseg)

    hasher = TreeHasher()
    for chunk_range in range_bytes_it(fileseg.range_bytes, self.max_chunk_bytes):
      self.upload_chunk(hasher, multipart_job, session, fileseg, chunk_range)
    
    kwargs = {
      'archiveSize' : str(fileseg.range_bytes[1]),
      'checksum' : hasher.digest_all_parts_as_hexstr(byte_array),
    }
    response = retry_operation (
      lambda : multipart_job.complete(**kwargs),
      botoex.ClientError
    )
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

    with open(fileseg.fileout, 'ab') as fileobj:
      fileobj.write(body_bytes)
      file_size = fileobj.tell()

    assert file_size == fileseg.chunks[-1][1], \
      "The real file lenght does not match the last chunk : %d / %d" % (file_size, fileseg.chunks[-1][1])
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
      logger.warn('MultipartUpload %r is not valid anymore', fileseg.aws_id)
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

      if 'Parts' in response and response['Parts']:
        part_range = build_range_from_mime(response['RangeInBytes'])
        # the only part which is smaller than the chunk size is the last one, but this method should not be called on a fully uploaded fileseg
        assert part_range[1] - part_range[0] == self.max_chunk_bytes

        for part in response['Parts']:
          hasher.add_chunk_hash_as_hexstr(part['SHA256TreeHash'])
        uploaded_count += len(response['Parts'])
      
      if 'Marker' not in response or not response['Marker']:
        break
    return uploaded_count
  
  def determine_range_to_download (self, aws_job, fileseg):
    job_range = build_range_from_mime(aws_job.retrieval_byte_range)
    remain_range = fileseg.calculate_remaining_range()
    assert range_contains(remain_range, job_range), "Job retrieval range must contain the fileseg byte range"
    return remaining_range

## END AwsGlacierManager

# Separate class because seldom used and does not alter any session
class AwsGlacierEmergencyManager:

  def __init__ (self, boto_session):
    glacier = boto_session.resource('glacier')
    vault_name = get_conf().aws.glacier_vault
    self.vault = glacier.Vault(AwsGlacierManager.DEFAULT_ACCOUNT, vault_name)

  # In case you cannot retrieve the txlog from a local copy or s3 !!
  def download_last_txlog (self, search_key, fileout):
    inventory = self.check_for_recent_inventory_job()
    if not inventory:
      inventory = self.retrieve_vault_inventory()
    archive_id, size = self.find_most_recent_tx_log(inventory, search_key)

    fileseg = Fileseg(fileout, None, archive_id, (0,size))
    if not self.download_from_existing_job(fileseg):
      self.single_shot_download (fileseg)
    return fileseg

  def retrieve_vault_inventory (self):
    retrieval_job = retry_operation (
      lambda : self.vault.initiate_inventory_retrieval(),
      botoex.ClientError
    )  

    logger.info("Submitted inventory retrieval job %r", retrieval_job.id)
    self.wait_for_job_completion(retrieval_job)
    output = retry_operation (
      lambda : retrieval_job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    inventory = convert_json_bytes_to_dict(output['body'].read())
    logger.info("Got inventory with %d items", len(inventory['ArchiveList']))
    return inventory

  def find_most_recent_tx_log (self, inventory, search_key):
    candidates = [ arc for arc in inventory['ArchiveList'] 
                   if arc['ArchiveDescription'].find(search_key) > -1 ]

    most_recent = next(sorted( candidates, reverse=True, key=lambda x:x['CreationDate'] ))
    logger.info("Most recent txlog found : %r", most_recent)
    return most_recent['ArchiveId'], int(most_recent['Size'])

  def check_for_recent_inventory_job (self):
    # in case we crash before retrieving the txlog we do not start many other jobs
    job = next( j for j in self.vault.jobs.all() 
                if j.status_code in ('InProgress', 'Succeeded') and j.action == 'InventoryRetrieval',
                None )
    if not job:
      return None

    logger.info("Found existing inventory retrieval job %r", job.id)
    self.wait_for_job_completion(job)
    output = retry_operation (
      lambda : job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    inventory = convert_json_bytes_to_dict(output['body'].read())
    logger.info("Got inventory with %d items", len(inventory['ArchiveList']))
    return inventory

  def wait_for_job_completion (self, aws_job):
    # no job timeout handling ...
    if aws_job.status_code = 'Succeeded': return

    while True:
      logger.info('Checking if glacier job %r has completed', aws_job.id)
      fresh_stx = next( j for j in self.vault.jobs.all() if j.id == aws_job.id )
      if fresh_stx.status_code = 'Succeeded': return
      assert fresh_stx.status_code == 'InProgress'
      wait_for_polling_period()

  def download_from_existing_job (self, fileseg):
    job = next( j for j in self.vault.jobs.all() 
                if j.status_code in ('InProgress', 'Succeeded') and j.archive_id == fileseg.archive_id,
                None )
    if not job:
      return False

    logger.info("Found existing job %r for %r", job.id, fileseg.archive_id)
    self.wait_for_job_completion(job)
    output = retry_operation (
      lambda : job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    self.write_into_fileseg(fileseg, output)
    return True

  def single_shot_download (self, fileseg):
    archive = self.vault.Archive(fileseg.archive_id)
    job = retry_operation (
      lambda : archive.initiate_inventory_retrieval(**kwargs),
      botoex.ClientError
    )
    logger.info("Submitted download job %r", job.id)

    self.wait_for_job_completion(job)
    output = retry_operation (
      lambda : job.get_output(),
      botoex.ClientError
    )  

    assert int(output['status']) == 200
    self.write_into_fileseg(fileseg, output)
  
  def write_into_fileseg (self, fileseg, job_output):
    logger.info("Writing into %r : %r", fileseg.fileout, output['ResponseMetadata']['HTTPHeaders'])
    body_bytes = job_output['body'].read()
    
    # we truncate the fileseg
    with open(fileseg.fileout, 'wb') as fileobj:
      fileobj.write(body_bytes)
      assert fileobj.tell() == fileseg.range_bytes[1]

## END AwsGlacierEmergencyManager

