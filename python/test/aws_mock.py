from common import *
import uuid, tempfile, json
assert 'boto3' in sys.modules, 'import this module after boto'
import boto3
logger = logging.getLogger(__name__)

# Change default session so that if any operation reaches AWS it will be rejected
boto3.setup_default_session(
  aws_access_key_id='mr_monkey',
  aws_secret_access_key='likes_bananas',
)

class DummySession:

  def session (self, *args, **kwargs):
    return DummySession.create_dummy_resource(*args, **kwargs)

  def resource (self, *args, **kwargs):
    logger.info("Creating resource : %r, %r", args, kwargs)
    name = args[0]
    if name == 'glacier':
      return DummyGlacier()
    if name == 's3':
      return DummyS3()
    raise Exception('not implemented')

  def client (self, *args, **kwargs):
    raise Exception('not implemented')

  @staticmethod
  def create_dummy_session (*args, **kwargs):
    logger.info("Creating session : %r, %r", args, kwargs)
    return DummySession()

  @staticmethod
  def create_dummy_resource (*args, **kwargs):
    logger.info("Creating session : %r, %r", args, kwargs)
    if args[0] == 'glacier':
      return DummyGlacier()
    if args[0] == 's3':
      return DummyS3()
    raise Exception('not implemented')

  @staticmethod
  def create_dummy_client (*args, **kwargs):
    logger.info("Creating session : %r, %r", args, kwargs)
    raise Exception('not implemented')

  boto3.session = create_dummy_session
  boto3.resource = create_dummy_resource
  boto3.client = create_dummy_client

class DummyResource:
  
  def __init__ (self, name, parent):
    self._created = False
    meta = object()
    meta.data = object()
    self._attributes = { 'meta' : meta }
    self.name = name
    self.parent = parent
  
  def _setattr (self, name, value):
    self._attributes['name'] = value 

  def __getattr__(self, name):
    if not self._created:
      raise DummyException()
    return self._attributes['name']  

  def load (self): pass
  def reload (self): pass
  def get_available_subresources():
    return []

class DummyCollection:
  
  def __init__ (self):
    self.objects = {}
  
  def _has (self, key):
    return key in self.objects

  def _del (self, key):
    del self.objects[key]

  def _get (self, key):
    return self.objects.get(key)

  def _add (self, key, val):
    self.objects[key] = val

  def _clear (self):
    self.objects.clear()

  def all (self):
    return self.objects.values()

class DummyException (Exception):
  pass

class DummyStream:
  def __init__ (self, stream):
    self.closed = False
    self.stream = stream

  def close():
    self.closed = True

  def read():
    assert not self.closed
    return stream

###################################################################################################################

class DummyGlacier (DummyResource):
  
  def __init__ (self, name):
    super().__init__(name, None)
    self.vaults = DummyCollection()

  def Vault (self, account_id, name):
    return DummyVault(name, self)
   
class DummyVault (DummyResource):

  def __init__ (self, name, parent):
    super().__init__(name, parent)
    self.account_id = '-'
    self.vault_name = name
    self._setattr('creation_date', datetime.datetime.now())
    self._setattr('last_inventory_date', datetime.datetime.now())
    self._setattr('number_of_archives', 0)
    self._setattr('size_in_bytes', 0)
    self._setattr('vault_arn', uuid.uuid4())
    # Collections
    #self.failed_jobs = DummyCollection()
    #self.succeeded_jobs = DummyCollection()
    #self.jobs = DummyCollection()
    #self.completed_jobs = DummyCollection()
    self.jobs_in_progress = DummyCollection()
    self.multipart_uplaods = DummyCollection()
    self.archives = {}

  def create (self):
    '''
    {'location': '/843392324993/vaults/dummy_vault', 'ResponseMetadata': {'HTTPStatusCode': 201, 'HTTPHeaders': {'content-length': '2', 'x-amzn-requestid': 'kClaL7QvT4LR_slpdu6rXi0jrBH6XjjUV7tgBA2b_peySko', 'location': '/843392324993/vaults/dummy_vault', 'date': 'Sat, 27 Aug 2016 17:51:31 GMT', 'content-type': 'application/json'}, 'RequestId': 'kClaL7QvT4LR_slpdu6rXi0jrBH6XjjUV7tgBA2b_peySko'}}
    '''
    self._created = True
    self.parent._add(self.name, self)

  def _fail_job (self, job):
    self.jobs_in_progress._del(job.name)
    job.status_code = DummyJob.Failed

  def _transition_jobs_to_complete (self):
    for k,job in self.jobs_in_progress.all():
      job.completed = True
    self.jobs_in_progress._clear()

  def initiate_inventory_retrieval (self):
    '''
    {'InventoryDate': '2016-08-21T00:46:31Z', 'ArchiveList': [{'SHA256TreeHash': '1acac939ccb8d55467ecaa6bb413ae59434b6ef5008f45adbf16c397d74da15c', 'ArchiveId': 'T8ecmVSHxb1pcW31Ia0C4Q4mbSb6c3ERw9lm4q-Erhc562Qo9r8XqAa8aFUjoIcrnbL7h58Z_FzxI7lNevXenmR9d3vvXWd5KezsL07Akf6Mt9hVGhhFVaO0GzRxzRGiyP94OXqSqA', 'Size': 71641, 'CreationDate': '2016-01-06T18:42:57Z', 'ArchiveDescription': 'test random file upload'}, {'SHA256TreeHash': '2ecfdbd275e1e00bf050b4954fa6d1467190a6c1719cffaa22d96f0c2e69188b', 'ArchiveId': 'poti53ZqdyE4LYfYUyG7x4Zahb-4Ux6Ry98rma1FZJqcOg2FwrZriGZkNHAZGSYrDI2Al1CZygJj4UqLzJ9B0r2h88UHR5b5Xi_Vi56VtZnkku2FQ0mA3mK16uhVOw3kg9x2vXXizQ', 'Size': 12582912, 'CreationDate': '2016-08-20T13:44:27Z', 'ArchiveDescription': 'to_glacier_single_part_upl'}, {'SHA256TreeHash': '4d4d2e2dea23db2978753ff4c522d52f52a7ca78f4c0b2acc8b446caccc3a3b3', 'ArchiveId': 'S-CStPFbgPAdXh8QVEkd9OKamTIkRdHHshwbiA8O6-84in4SBVj8zHFAxh65mjr3dD_6dRUn1NaCZr9EBuiH8ZuPQ4yqc7qPNA6ZwB8Zq-ofWiat66CCnBWPNiCWAMm0-GeA7f5pzg', 'Size': 12582912, 'CreationDate': '2016-08-20T14:44:34Z', 'ArchiveDescription': 'to_glacier_multipart_upl'}], 'VaultARN': 'arn:aws:glacier:eu-west-1:843392324993:vaults/dummy_vault'}
    '''
    job = DummyJob('retrieval_job', self, DummyJob.InventoryRetrieval)
    assert not self.jobs_in_progress._has('retrieval_job')
    self.jobs_in_progress._add('retrieval_job', job)
    return job

  def upload_archive (self, **kwargs):
    assert 'body' in kwargs
    assert 'archiveDescription' in kwargs
    assert 'checksum' in kwargs
    archive = DummyArchive(kwargs['archiveDescription'], self)
    archive._addPart(kwargs['body'])
    self.archives[archive.name] = archive
    return archive

  def initiate_multipart_upload (self):
    assert 'archiveDescription' in kwargs
    assert 'partSize' in kwargs
    job = DummyMultiPart(kwargs['archiveDescription'], self, int(kwargs['partSize']))
    assert not self.multipart_uplaods._has(kwargs['archiveDescription'])
    self.multipart_uplaods._add(kwargs['archiveDescription'], job)
    return job
  

class DummyMultiPart (DummyResource):
  def __init__ (self, name, parent, partSize):
    super().__init__(name, parent)
    self._archive = DummyArchive(kwargs['archiveDescription'], parent)
    self.archive_description = name
    self.creation_date = datetime.datetime.now()
    self.multipart_upload_id = uuid.uuid4()
    self.part_size_in_bytes = partSize
    self_parts = []

  def abort(self):
    self.parent.multipart_uplaods._del(self.name)

  def complete(self):
    '''
    {'archiveId': 'N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'checksum': '9491cb2ed1d4e7cd53215f4017c23ec4ad21d7050a1e6bb636c4f67e8cddb844', 'location': '/843392324993/vaults/dummy_vault/archives/N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'ResponseMetadata': {'HTTPStatusCode': 201, 'HTTPHeaders': {'content-length': '2', 'x-amzn-requestid': 'UJTbl83S4ym5pcNhC_7wltlSq_ixBPv8T4y_rJvwMRqneaA', 'location': '/843392324993/vaults/dummy_vault/archives/N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'x-amz-sha256-tree-hash': '9491cb2ed1d4e7cd53215f4017c23ec4ad21d7050a1e6bb636c4f67e8cddb844', 'date': 'Sat, 27 Aug 2016 18:45:00 GMT', 'content-type': 'application/json', 'x-amz-archive-id': 'N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ'}, 'RequestId': 'UJTbl83S4ym5pcNhC_7wltlSq_ixBPv8T4y_rJvwMRqneaA'}}
    '''
    assert 'archiveSize' in kwargs
    assert 'checksum' in kwargs
    assert int(kwargs['archiveSize']) == self._archive._size

  def parts(self):
    '''
    {'ArchiveDescription': 'botoUpload2', 'Parts': [{'RangeInBytes': '0-2097151', 'SHA256TreeHash': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa'}], 'ResponseMetadata': {'HTTPStatusCode': 200, 'HTTPHeaders': {'content-length': '427', 'x-amzn-requestid': 'MThC24wf2PVcIMid4Zy3ik724FVSb0slbsKljIWWVq4ulik', 'date': 'Sun, 28 Aug 2016 09:01:14 GMT', 'content-type': 'application/json'}, 'RequestId': 'MThC24wf2PVcIMid4Zy3ik724FVSb0slbsKljIWWVq4ulik'}, 'CreationDate': '2016-08-28T09:00:52.061Z', 'PartSizeInBytes': 2097152, 'MultipartUploadId': 'XfiA5PM122LLolN5gu___oRss20GeXhvM0ZFJiAmvBhZXRSN1Py8ZNQHxkeFk24Vm0zOjMUodpYJ02WFJ7Hp7i7EACwF', 'VaultARN': 'arn:aws:glacier:eu-west-1:843392324993:vaults/dummy_vault'}
    '''
    return self._parts

  def upload_part(self):
    '''
    {'checksum': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa', 'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {'x-amzn-requestid': 'vtUGut6f4b7v4LnX1QxYACThE7-2Rbcj8f0EgLFbdtb9k8M', 'x-amz-sha256-tree-hash': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa', 'date': 'Sat, 27 Aug 2016 18:39:44 GMT'}, 'RequestId': 'vtUGut6f4b7v4LnX1QxYACThE7-2Rbcj8f0EgLFbdtb9k8M'}}
    '''
    assert 'body' in kwargs
    assert 'range' in kwargs
    assert 'checksum' in kwargs
    size = int( re.search(r'-(\d+)', kwargs['range']).group(1) )
    assert size == len(kwargs['body'])
    self._parts.append({})
    self._archive._addPart(kwargs['body'])
    return {}

class DummyJob (DummyResource):
  ArchiveRetrieval, InventoryRetrieval = 'ArchiveRetrieval','InventoryRetrieval'
  Failed = 'Failed'
  InProgress = 'InProgress'

  def __init__ (self, name, parent, action, start=None, end=None):
    super().__init__(name, parent)
    self._start = start
    self._end = end
    self.job_id = uuid.uuid4()
    self.action = action
    self.job_description = self.name
    self.creation_date = datetime.datetime.now()
    self.archive_id = None
    self.archive_sha256_tree_hash = None
    self.archive_size_in_bytes = None
    self.completed = False
    self.status_code = DummyJob.InProgress
    self.completion_date = None
    self.inventory_retrieval_parameters = {}
    self.inventory_size_in_bytes = None
    self.retrieval_byte_range = None
    self.sha256_tree_hash = None

  @staticmethod
  def min (a,b):
    if a == None: return b
    if b == None: return a
    if a > b: return b
    return a

  @staticmethod
  def max (a,b):
    if a == None: return b
    if b == None: return a
    if a > b: return a
    return b

  def get_output(self, **kwargs):
    '''
    {'body': <botocore.response.StreamingBody object at 0x7f57552a3e10>, 'checksum': '4d4d2e2dea23db2978753ff4c522d52f52a7ca78f4c0b2acc8b446caccc3a3b3', 'status': 200, 'ResponseMetadata': {'HTTPStatusCode': 200, 'HTTPHeaders': {'content-length': '12582912', 'x-amzn-requestid': '3Rqh31vlsE3a02DI2dCXXblBpgQc-qsVi9LVgaqHy6B0D1Q', 'x-amz-sha256-tree-hash': '4d4d2e2dea23db2978753ff4c522d52f52a7ca78f4c0b2acc8b446caccc3a3b3', 'x-amz-archive-description': 'to_glacier_multipart_upl', 'content-type': 'application/octet-stream', 'accept-ranges': 'bytes', 'date': 'Sun, 28 Aug 2016 08:01:31 GMT'}, 'RequestId': '3Rqh31vlsE3a02DI2dCXXblBpgQc-qsVi9LVgaqHy6B0D1Q'}, 'archiveDescription': 'to_glacier_multipart_upl', 'acceptRanges': 'bytes', 'contentType': 'application/octet-stream'}
    '''
    assert self.completed and self.status_code != DummyJob.Failed
    arg_start = None
    arg_end = None

    if 'range' in kwargs:
      arg_start = int( re.search(r'(\d+)', kwargs['range']).group(1) )
      arg_end = int( re.search(r'-(\d+)', kwargs['range']).group(1) ) + 1

    arg_start = DummyJob.min(self._start, arg_start)
    arg_end = DummyJob.max(self._end, arg_end)

    if arg_start != None and arg_end != None:
      response = {'body' : self._archive._chunk(start, end) }
    else:
      response = {'body' : self._archive._chunkFull() }
    return response  

class DummyArchive (DummyResource):
  def __init__ (self, name, parent):
    super().__init__(uuid.uuid4(), parent)
    self.description = name
    self.id = self.name
    self._content = tempfile.TemporaryFile()
    self._size = 0

  def initiate_archive_retrieval (self, **kwargs):
    '''
    jobParameters={'RetrievalByteRange':'0-1048575', "Type":'archive-retrieval', 'ArchiveId': 'S-CStPFbgPAdXh8QVEkd9OKamTIkRdHHshwbiA8O6-84in4SBVj8zHFAxh65mjr3dD_6dRUn1NaCZr9EBuiH8ZuPQ4yqc7qPNA6ZwB8Zq-ofWiat66CCnBWPNiCWAMm0-GeA7f5pzg'}
    '''
    start = None 
    end = None 
    if 'jobParameters' in kwargs:
      start = int( re.search(r'(\d+)', kwargs['jobParameters']['RetrievalByteRange']).group(1) )
      end = int( re.search(r'-(\d+)', kwargs['jobParameters']['RetrievalByteRange']).group(1) ) + 1
      
    job = DummyJob(self.id, self, DummyJob.InventoryRetrieval, start, end)
    assert not self.jobs_in_progress._has(self.id)
    self.jobs_in_progress._add(self.id, job)
    return job

  def _chunkFull (self):
    self._content.seek(0, os.SEEK_SET)
    chunk = DummyStream( self._content.read() )
    self._content.seek(0, os.SEEK_END)
    return chunk

  def _chunk (self, start, end):
    self._content.seek(start, os.SEEK_SET)
    chunk = DummyStream( self._content.read(end-start) )
    self._content.seek(0, os.SEEK_END)
    return chunk

  def _addPart (self, part):
    self._content.write(part)
    self._size += len(part)
  
###################################################################################################################


class DummyS3 (DummyResource):
  def __init__ (self, name):
    super().__init__(name, None)
    self._created = True
    self.buckets = DummyCollection()
  
  def Bucket(self, name):
    return DummyBucket(name, self)

class DummyBucket (DummyResource):

  def __init__ (self, name, parent):
    super().__init__(name, parent)
    self.object_versions = DummyCollection()
    self.objects = DummyCollection()
    self._setattr('creation_date', datetime.datetime.now())

  def create (self, **kwargs):
    '''
    botocore.exceptions.ClientError: An error occurred (BucketAlreadyOwnedByYou) when calling the CreateBucket operation: Your previous request to create the named bucket succeeded and you already own it.
    '''
    logger.info('Creating bucket %s', self.name)
    assert 'LocationConstraint' in kwargs['CreateBucketConfiguration']
    assert not self.parent.objects._has(name)
    self._created = True
    self.buckets.add(name, self)

  def download_file (self, key, filename):
    logger.info('Downloading %s to %s', key, filename)
    with open(filename, 'wb') as fileobj:
      fileobj.write(self.objects[key]._bytes)

  def put_object (self, **kwargs):
    assert 'Body' in kwargs
    assert 'Key' in kwargs
    obj = self.objects._get(kwargs['Key'])
    if not obj:
      obj = DummyS3Object(kwargs['Key'], self)
      self.objects._add( kwargs['Key'], obj )
    obj._bytes = kwargs['Body']

  def upload_file (self, filename, key):
    obj = self.objects._get(key)
    if not obj:
      obj = DummyS3Object(key, self)
      self.objects._add( kwargs['Key'], obj )
    with open(filename, 'rb') as fileobj:
      obj._bytes = fileobj.read()

  def Object (self, key):
    return DummyS3Object(key, self)

class DummyS3Object (DummyResource):
  def __init__ (self, name, parent):
    super().__init__(name, parent)
    self._bytes = b''
    self.key = name
    self.bucket_name =  parent.name
    self.last_modified = datetime.datetime.now()
    self.metadata = {}
    self.version_id = None

  def delete (self):
    assert self.parent.objects._has(self.name)
    self.parent.objects._del(self.name)
    self._created = False

  def download_file (self, filename):
    self.parent.download_file(self.name, filename)

  def get (self):
    '''
    {'Metadata': {}, 'Body': <botocore.response.StreamingBody object at 0x7f57553d7ba8>, 'ResponseMetadata': {'HostId': '/ymvGE8jdY8JDw1zXQEYzfs8zuynQNeJeGDNwfLPOLW8QBQaJd+B7vhFYS9ggaSXNK4+r1lsXLs=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '/ymvGE8jdY8JDw1zXQEYzfs8zuynQNeJeGDNwfLPOLW8QBQaJd+B7vhFYS9ggaSXNK4+r1lsXLs=', 'content-length': '14', 'last-modified': 'Wed, 24 Aug 2016 21:44:55 GMT', 'x-amz-request-id': '51187B034C395B3A', 'date': 'Sat, 27 Aug 2016 13:41:41 GMT', 'content-type': 'binary/octet-stream', 'accept-ranges': 'bytes', 'x-amz-expiration': 'expiry-date="Sat, 03 Dec 2016 00:00:00 GMT", rule-id="test_clean_rule"', 'etag': '"618cfda8848cdfba75913a9779d03bdf"', 'server': 'AmazonS3'}, 'RequestId': '51187B034C395B3A'}, 'ContentType': 'binary/octet-stream', 'LastModified': datetime.datetime(2016, 8, 24, 21, 44, 55, tzinfo=tzutc()), 'ETag': '"618cfda8848cdfba75913a9779d03bdf"', 'Expiration': 'expiry-date="Sat, 03 Dec 2016 00:00:00 GMT", rule-id="test_clean_rule"', 'ContentLength': 14, 'AcceptRanges': 'bytes'}
    '''
    self._created = True
    return { 'Body' : DummyStream(self._bytes) }

  def put (self, **kwargs):
    assert 'Body' in kwargs
    self._bytes = kwargs['Body']

  def upload_file (self, filename):
    with open(filename, 'rb') as fileobj:
      self._bytes = fileobj.read()


