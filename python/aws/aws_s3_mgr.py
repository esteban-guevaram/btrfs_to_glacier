import botocore.exceptions as botoex
from common import *
from file_utils import *
logger = logging.getLogger(__name__)

class AwsS3Manager:

  def __init__ (self, boto_session):
    self.max_s3_size = get_conf().aws.chunk_size_in_mb * 1024**2
    self.s3_object_ttl_days = get_conf().aws.s3_object_ttl_days
    self.bucket = self._idenpotent_bucket_creation(boto_session)

  def upload_txlog (self, fileseg):
    logger.debug('Uploading into s3: %r', fileseg)
    assert fileseg.range_bytes[1] <= self.max_s3_size, 'Filesize too big %r' % (fileseg.range_bytes,)

    obj_name = os.path.basename(fileseg.fileout)
    s3_object = self.bucket.Object(obj_name)
    byte_array = read_fileseg(fileseg)

    kwargs = { 
      'Body' : byte_array, 
      'StorageClass' : 'STANDARD_IA', 
      'ContentMD5' : calculate_md5_base64_encoded(byte_array),
    }
    response = retry_operation (
      lambda : s3_object.put(**kwargs),
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200
    return s3_object  

  def download_most_recent_txlog (self, back_logfile):
    fileseg = None
    obj_summaries = retry_operation (
      lambda : list( self.bucket.objects.all() ),
      botoex.ClientError
    )
    logger.info('Found %d objects in %s', len(obj_summaries), self.bucket.name)
    obj_summaries.sort( key=lambda x:x.last_modified )

    if obj_summaries and len(obj_summaries):
      last_txlog = obj_summaries[-1]
      logger.debug('Retrieving %s modified %r', last_txlog.key, last_txlog.last_modified)

      retry_operation (
        lambda : self.bucket.download_file(last_txlog.key, back_logfile),
        botoex.ClientError
      )
      fileseg = Fileseg.build_from_fileout(back_logfile)
      fileseg.archive_id = last_txlog.key

    return fileseg

  def _idenpotent_bucket_creation (self, boto_session):
    s3_session = boto_session.resource('s3')
    bucket_name = get_conf().aws.s3_bucket
    logger.debug("Opening bucket : %s", bucket_name)

    for bucket in s3_session.buckets.all():
      if bucket.name == bucket_name:
        logger.debug('%r already exists', bucket_name)
        return bucket
    return self._create_and_configure_bucket(boto_session, s3_session, bucket_name)
  
  def _create_and_configure_bucket (self, boto_session, s3_session, bucket_name):
    logger.info("Creating s3 bucket : %s", bucket_name)
    bucket = s3_session.Bucket(bucket_name)

    kwargs = {
      'ACL':'private', 
      'CreateBucketConfiguration' : { 'LocationConstraint' : boto_session.region_name },
    }
    response = retry_operation (
      lambda : bucket.create(**kwargs),
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200

    lifecycle = bucket.Lifecycle()
    rule = {
      'ID' : bucket.name + '.lifecycle.rule', 
      'Status' : 'Enabled',
      'Prefix' : '',
      'Expiration' : { 'Days' : self.s3_object_ttl_days },
    }
    response = retry_operation (
      lambda : lifecycle.put(LifecycleConfiguration={ 'Rules' : [rule] }),
      botoex.ClientError
    )
    assert int(response['HTTPStatusCode']) == 200, repr(response)
    return bucket

## END AwsS3Manager

