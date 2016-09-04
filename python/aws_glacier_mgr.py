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

  def finish_upload (self, session, fileseg):
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

  def single_shot_upload (self, fileseg):
    session.start_fileseg_single_chunk(fileseg)
    byte_array = read_fileseg(fileseg)
    
    kwargs = {
      'body' : byte_array,
      'archiveDescription' : fileseg.fileout,
      'checksum' : TreeHasher().digest_single_shot_as_hexstr(byte_array),
    }
    archive = retry_operation (
      lambda : self.vault.upload_archive(**kwargs),
      botoex.ClientError
    )

    session.close_fileseg_single_chunk(fileseg.key(), archive.id)
    return archive

  def multi_part_upload (self, session, fileseg):
    hasher = TreeHasher()
    session.start_fileseg(fileseg)

    kwargs = {
      'partSize' : str(self.max_chunk_bytes),
      'archiveDescription' : fileseg.fileout,
    }
    multipart_job = retry_operation (
      lambda : self.vault.initiate_multipart_upload(**kwargs),
      botoex.ClientError
    )

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
    session.start_chunk(fileseg.key(), chunk_range)
    logger.debug("Uploading chunk : %s %r", fileseg.fileout, chunk_range)
    byte_array = read_fileseg(fileseg, chunk_range)

    hasher.update_chunk(byte_array)
    kwargs = {
      'body' : byte_array,
      'range' : build_mime_range(chunk_range),
      'checksum' : hasher.digest_chunk_as_hexstr(),
    }
    chunk = retry_operation (
      lambda : multipart_job.upload_part(**kwargs),
      botoex.ClientError
    )

    session.close_chunk(fileseg.key())
    return chunk

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

## END AwsGlacierManager

