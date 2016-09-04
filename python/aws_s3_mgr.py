import boto3, botocore.exceptions as botoex
from common import *
logger = logging.getLogger(__name__)

class AwsS3Manager:

  def __init__ (self, boto_session):
    self.bucket = self._idenpotent_bucket_creation(boto_session)
    self.max_s3_size = get_conf().aws.chunk_size_in_mb * 1024**2
    self.s3_object_ttl_days = get_conf().aws.s3_object_ttl_days

  def _idenpotent_bucket_creation (self, boto_session):
    s3 = boto_session.resource('s3')
    bucket_name = get_conf().aws.s3_bucket
    logger.debug("Opening bucket : %s", bucket_name)

    for bucket in s3.buckets.all():
      if bucket.name == bucket_name:
        return bucket
    return self._create_and_configure_bucket(bucket_name)
  
  def _create_and_configure_bucket (self, bucket_name):
    logger.info("Creating s3 bucket : %s", bucket_name)
    bucket = s3.Bucket(bucket_name)

    kwargs = {
      'ACL':'private', 
      'CreateBucketConfiguration' : { 'LocationConstraint' : boto_session.region_name },
    }
    response = retry_operation (
      lambda : bucket.create(**kwargs)
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200

    lifecycle = bucket.Lifecycle()
    rule = {
      'ID' : bucket.name + '.lifecycle.rule', 
      'Status' : 'Enabled',
      'Prefix' : ''
      'Expiration' : { 'Days' : self.s3_object_ttl_days },
    }
    response = retry_operation (
      lambda : lifecycle.put(LifecycleConfiguration={ 'Rules' : [rule] })
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200
    return bucket

  def upload_txlog (self, session, fileseg):
    logger.debug('Uploading into s3: %r', fileseg)
    assert fileseg.range_bytes[1] <= self.max_s3_size, 'Filesize too big'

    obj_name = os.path.filename(fileseg.fileout)
    s3_object = self.bucket.Object(obj_name)
    byte_array = read_fileseg(fileseg)

    kwargs = { 
      'Body' : byte_array, 
      'StorageClass' : 'STANDARD_IA', 
      'ContentMD5' : calculate_md5_base64_encoded(byte_array),
    }
    response = retry_operation (
      lambda : lifecycle.put(LifecycleConfiguration={ 'Rules' : [rule] })
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200

    fileseg.archive_id = s3_object.key
    session.save_atomic_txlog_s3_upload(fileseg)
    return s3_object  

  def download_txlog_to_file (self, fileseg):
    retry_operation (
      lambda : self.bucket.download_file(fileseg.archive_id, fileseg.fileout),
      botoex.ClientError
    )
    return fileseg

## END AwsS3Manager

