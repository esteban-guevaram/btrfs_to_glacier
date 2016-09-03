import boto3, botocore.exceptions as botoex
from common import *
from file_utils import *
logger = logging.getLogger(__name__)

class AwsGlacierManager:

  def upload (self, session, fileseg):
    logger.debug('Uploading : %r', fileseg)
    max_chunk_bytes = get_conf().aws.chunk_size_in_mb * 1024**2

    if fileseg.range_bytes[1] < max_chunk_bytes:
      self.single_shot_upload(fileseg)
    else:
      self.multi_part_upload(fileseg)

  def single_shot_upload (self, fileseg):
    # va.upload_archive(body=b'chocolat', archiveDescription='one_shot_boto_upl', checksum='9491cb')
    with open(fileseg.fileout, 'rb') as fileobj:
      byte_array = fileobj.read()
      assert len(byte_array) == fileseg.range_bytes[1]
    
    tree_hash = TreeHasher().single_shot_calculate_as_hexstr(byte_array)
    kwargs = {
      'body' : byte_array,
      'archiveDescription' : fileseg.fileout,
      'checksum' : tree_hash,
    }
    archive = retry_operation (
      lambda : self.vault.upload_archive(**kwargs),
      botoex.ClientError
    )
    return archive

  def multi_part_upload (self, fileseg):
    pass

  def finish_upload (self, session, fileseg):
    pass

