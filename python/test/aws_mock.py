from common import *
from file_utils import TreeHasher
import uuid, tempfile, json
import boto3
import botocore.exceptions as botoex
logger = logging.getLogger(__name__)

# Change default session so that if any operation reaches AWS it will be rejected
boto3.setup_default_session(
  aws_access_key_id='mr_monkey',
  aws_secret_access_key='likes_bananas',
)

#####################################################################################
# Behaviour

RESP_VOID = 'RESP_VOID'

def build_ok_response (src_type, method_name, answer=None, **kwargs):
  key = src_type.__name__ + method_name.__name__
  resp = None
  if answer:
    resp = answer
  elif answer != RESP_VOID:
    resp = {}
    resp.update(kwargs)
    resp['__debug__'] = key
    resp['HTTPStatusCode'] = 200
    resp['ResponseMetadata'] = { 'HTTPStatusCode': 200 }
  return (True, resp)

def build_ko_response (src_type, method_name, answer=None, **kwargs):
  key = src_type.__name__ + method_name.__name__
  if DummySession.blowup_on_fail or answer == RESP_VOID:
    raise DummyException(key)

  resp = {}
  resp.update(kwargs)
  resp['__debug__'] = key
  resp['HTTPStatusCode'] = 400
  resp['ResponseMetadata'] = { 'HTTPStatusCode': 400 }
  return (False, resp)

def always_ok_behaviour ():
  def inner_helper (src_type, method_name, answer=None, **kwargs):
    return build_ok_response(src_type, method_name, answer, **kwargs)
  return inner_helper

def always_ko_behaviour (white=[], black=[]):
  def inner_helper (src_type, method_name, answer=None, **kwargs):
    if decide_if_type_fails(src_type, white, black):
      return build_ko_response(src_type, method_name, answer, **kwargs)
    return build_ok_response(src_type, method_name, answer, **kwargs)
  return inner_helper

def fail_at_random_with_limit (perc_fail, limit, white=[], black=[]):
  streak = {}
  def inner_helper (src_type, method_name, answer=None, **kwargs):
    chance = random.randrange(100)
    key = src_type.__name__ + method_name.__name__
    streak.setdefault(key, 0)
    if (chance >= perc_fail or streak[key] >= limit) and not decide_if_type_fails(src_type, white, black):
      streak[key] = 0
      return build_ok_response(src_type, method_name, answer, **kwargs)
    streak[key] += 1
    return build_ko_response(src_type, method_name, answer, **kwargs)
  return inner_helper

def fail_at_random (perc_fail, white=[], black=[]):
  def inner_helper (src_type, method_name, answer=None, **kwargs):
    chance = random.randrange(100)
    if chance >= perc_fail and not decide_if_type_fails(src_type, white, black):
      return build_ok_response(src_type, method_name, answer, **kwargs)
    return build_ko_response(src_type, method_name, answer, **kwargs)
  return inner_helper

def fail_at_first_then_ok (fail_count, white=[], black=[]):
  count = {}
  def inner_helper (src_type, method_name, answer=None, **kwargs):
    key = src_type.__name__ + method_name.__name__
    count.setdefault(key, 0)
    if count[key] < fail_count and decide_if_type_fails(src_type, white, black):
      count[key] += 1
      return build_ko_response(src_type, method_name, answer, **kwargs)
    return build_ok_response(src_type, method_name, answer, **kwargs)
  return inner_helper

def decide_if_type_fails(src_type, white, black):
  if not black: return src_type not in white
  if not white: return src_type in black
  return src_type in black and src_type not in white

#####################################################################################
# Root objects

class DummySession:
  region_name = 'banana_land'
  behaviour = always_ok_behaviour()
  blowup_on_fail = True

  def __init__ (self):
    self.glacier = DummyGlacier( str(uuid.uuid4()) )
    self.s3 = DummyS3( str(uuid.uuid4()) )

  def session (self, *args, **kwargs):
    return DummySession.create_dummy_resource(*args, **kwargs)

  def resource (self, *args, **kwargs):
    return self.create_dummy_resource(*args, **kwargs)

  def client (self, *args, **kwargs):
    return self.create_dummy_client(*args, **kwargs)

  @staticmethod
  def create_dummy_session (*args, **kwargs):
    logger.info("Creating session : %r, %r", args, kwargs)
    return DummySession()

  def create_dummy_resource (self, *args, **kwargs):
    logger.info("Creating resource : %r, %r", args, kwargs)
    if args[0] == 'glacier':
      return self.glacier
    if args[0] == 's3':
      return self.s3
    raise Exception('not implemented')

  def create_dummy_client (self, *args, **kwargs):
    logger.info("Creating client : %r, %r", args, kwargs)
    raise Exception('not implemented')


boto3.session.Session = DummySession.create_dummy_session
boto3.resource        = None
boto3.client          = None


class DummyResource:
  
  class ObjWithDict: pass

  def __init__ (self, name, parent):
    self._created = False
    meta = DummyResource.ObjWithDict()
    meta.data = DummyResource.ObjWithDict()
    self._attributes = { 'meta' : meta }
    self.name = name
    self.id = self.name
    self.parent = parent
  
  def _setattr (self, name, value):
    self._attributes[name] = value 

  def __getattr__(self, name):
    if not self._created:
      raise Exception('not self._created : %r / %s' % (type(self), name))
    return self._attributes[name]  

  def load (self): pass
  def reload (self): pass
  def get_available_subresources():
    return []

class DummyCollection (dict):
  def all (self):
    return self.values()

class DummyException (botoex.ClientError):
  def __init__ (self, msg):
    self.msg = msg

  def __repr__ (self):
    return '%s : %s' % (type(self).__name__, self.msg)

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
    vault = self.vaults.get(name)
    if vault:
      return vault
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
    self._setattr('vault_arn', str(uuid.uuid4()) )
    # Collections
    #self.failed_jobs = DummyCollection()
    #self.succeeded_jobs = DummyCollection()
    #self.jobs = DummyCollection()
    #self.completed_jobs = DummyCollection()
    self.jobs_in_progress = DummyCollection()
    self.multipart_uploads = DummyCollection()
    self._archives = DummyCollection()

  def _clear (self):
    for arch in self._archives.values():
      arch._close()
    self.jobs_in_progress = DummyCollection()
    self.multipart_uploads = DummyCollection()
    self._archives = DummyCollection()

  def create (self):
    '''
    {'location': '/843392324993/vaults/dummy_vault', 'ResponseMetadata': {'HTTPStatusCode': 201, 'HTTPHeaders': {'content-length': '2', 'x-amzn-requestid': 'kClaL7QvT4LR_slpdu6rXi0jrBH6XjjUV7tgBA2b_peySko', 'location': '/843392324993/vaults/dummy_vault', 'date': 'Sat, 27 Aug 2016 17:51:31 GMT', 'content-type': 'application/json'}, 'RequestId': 'kClaL7QvT4LR_slpdu6rXi0jrBH6XjjUV7tgBA2b_peySko'}}
    '''
    self._created = True
    self.parent.vaults[self.name] = self

  def _fail_job (self, job):
    del self.jobs_in_progress[job.name]
    job.status_code = DummyJob.Failed

  def _transition_jobs_to_complete (self):
    for k,job in self.jobs_in_progress.all():
      job.completed = True
    self.jobs_in_progress.clear()

  def initiate_inventory_retrieval (self, **kwargs):
    '''
    jobParameters={'RetrievalByteRange':'0-1048575', "Type":'archive-retrieval', 'ArchiveId': 'S-CStPFbgPAdXh8QVEkd9OKamTIkRdHHshwbiA8O6-84in4SBVj8zHFAxh65mjr3dD_6dRUn1NaCZr9EBuiH8ZuPQ4yqc7qPNA6ZwB8Zq-ofWiat66CCnBWPNiCWAMm0-GeA7f5pzg'}
    {'InventoryDate': '2016-08-21T00:46:31Z', 'ArchiveList': [{'SHA256TreeHash': '1acac939ccb8d55467ecaa6bb413ae59434b6ef5008f45adbf16c397d74da15c', 'ArchiveId': 'T8ecmVSHxb1pcW31Ia0C4Q4mbSb6c3ERw9lm4q-Erhc562Qo9r8XqAa8aFUjoIcrnbL7h58Z_FzxI7lNevXenmR9d3vvXWd5KezsL07Akf6Mt9hVGhhFVaO0GzRxzRGiyP94OXqSqA', 'Size': 71641, 'CreationDate': '2016-01-06T18:42:57Z', 'ArchiveDescription': 'test random file upload'}, {'SHA256TreeHash': '2ecfdbd275e1e00bf050b4954fa6d1467190a6c1719cffaa22d96f0c2e69188b', 'ArchiveId': 'poti53ZqdyE4LYfYUyG7x4Zahb-4Ux6Ry98rma1FZJqcOg2FwrZriGZkNHAZGSYrDI2Al1CZygJj4UqLzJ9B0r2h88UHR5b5Xi_Vi56VtZnkku2FQ0mA3mK16uhVOw3kg9x2vXXizQ', 'Size': 12582912, 'CreationDate': '2016-08-20T13:44:27Z', 'ArchiveDescription': 'to_glacier_single_part_upl'}, {'SHA256TreeHash': '4d4d2e2dea23db2978753ff4c522d52f52a7ca78f4c0b2acc8b446caccc3a3b3', 'ArchiveId': 'S-CStPFbgPAdXh8QVEkd9OKamTIkRdHHshwbiA8O6-84in4SBVj8zHFAxh65mjr3dD_6dRUn1NaCZr9EBuiH8ZuPQ4yqc7qPNA6ZwB8Zq-ofWiat66CCnBWPNiCWAMm0-GeA7f5pzg', 'Size': 12582912, 'CreationDate': '2016-08-20T14:44:34Z', 'ArchiveDescription': 'to_glacier_multipart_upl'}], 'VaultARN': 'arn:aws:glacier:eu-west-1:843392324993:vaults/dummy_vault'}
    '''
    assert 'Type' in kwargs
    assert 'Tier' in kwargs
    job = DummyJob('retrieval_job', self, DummyJob.InventoryRetrieval)
    resp = DummySession.behaviour(type(self), self.initiate_inventory_retrieval, job)
    if resp[0]:
      assert not 'retrieval_job' in self.jobs_in_progress
      self.jobs_in_progress['retrieval_job'] = job
    return resp[1]

  def upload_archive (self, **kwargs):
    assert 'body' in kwargs
    assert 'archiveDescription' in kwargs
    assert 'checksum' in kwargs
    archive = DummyArchive(kwargs['archiveDescription'], self)
    resp = DummySession.behaviour(type(self), self.upload_archive, archive)
    if resp[0]:
      archive._addPart(kwargs['body'])
      archive._create()
    return resp[1]

  def initiate_multipart_upload (self, **kwargs):
    assert 'archiveDescription' in kwargs
    assert 'partSize' in kwargs
    assert not kwargs['archiveDescription'] in self.multipart_uploads
    job = DummyMultiPart(kwargs['archiveDescription'], self, int(kwargs['partSize']))
    resp = DummySession.behaviour(type(self), self.initiate_multipart_upload, job)
    if resp[0]:
      self.multipart_uploads[kwargs['archiveDescription']] = job
    return resp[1]
  
  def Archive(self, archiveId):
    return DummyArchive(archiveId, self)

class DummyMultiPart (DummyResource):
  def __init__ (self, name, parent, partSize):
    super().__init__(name, parent)
    self._archive = DummyArchive(name, parent)
    self.archive_description = name
    self.creation_date = datetime.datetime.now()
    self.multipart_upload_id = str(uuid.uuid4())
    self.part_size_in_bytes = partSize
    self._parts = []

  def abort(self):
    del self.parent.multipart_uploads[self.name]

  def complete(self, **kwargs):
    '''
    {'archiveId': 'N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'checksum': '9491cb2ed1d4e7cd53215f4017c23ec4ad21d7050a1e6bb636c4f67e8cddb844', 'location': '/843392324993/vaults/dummy_vault/archives/N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'ResponseMetadata': {'HTTPStatusCode': 201, 'HTTPHeaders': {'content-length': '2', 'x-amzn-requestid': 'UJTbl83S4ym5pcNhC_7wltlSq_ixBPv8T4y_rJvwMRqneaA', 'location': '/843392324993/vaults/dummy_vault/archives/N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ', 'x-amz-sha256-tree-hash': '9491cb2ed1d4e7cd53215f4017c23ec4ad21d7050a1e6bb636c4f67e8cddb844', 'date': 'Sat, 27 Aug 2016 18:45:00 GMT', 'content-type': 'application/json', 'x-amz-archive-id': 'N64VJz1d7VbGhR0We5n_6mAHhResPszfx4oQpp1d2LOoSTFeFPiw4-vKsLI5D1nl54Syc2Wdp_XPyAVHf-Am_j7C5TxsqRuMVykUBK7SvgE1HgiVEEIdma5AMdKslJ-mP2gkBDMZzQ'}, 'RequestId': 'UJTbl83S4ym5pcNhC_7wltlSq_ixBPv8T4y_rJvwMRqneaA'}}
    '''
    assert 'archiveSize' in kwargs
    assert 'checksum' in kwargs
    assert self.name in self.parent.multipart_uploads
    assert int(kwargs['archiveSize']) == self._archive._size
    arch_checksum = TreeHasher().digest_fileobj_as_hexstr(self._archive._content)
    assert kwargs['checksum'] == arch_checksum

    ok_resp = {
      'archiveId' : self._archive.id,
      'checksum' : arch_checksum,
      'ResponseMetadata': { 'HTTPStatusCode': 201 },
    }
    resp = DummySession.behaviour(type(self), self.complete, ok_resp)
    if resp[0]:
      self._archive._create()
      del self.parent.multipart_uploads[self.name]
    return resp[1]

  def parts(self):
    '''
    {'ArchiveDescription': 'botoUpload2', 'Parts': [{'RangeInBytes': '0-2097151', 'SHA256TreeHash': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa'}], 'ResponseMetadata': {'HTTPStatusCode': 200, 'HTTPHeaders': {'content-length': '427', 'x-amzn-requestid': 'MThC24wf2PVcIMid4Zy3ik724FVSb0slbsKljIWWVq4ulik', 'date': 'Sun, 28 Aug 2016 09:01:14 GMT', 'content-type': 'application/json'}, 'RequestId': 'MThC24wf2PVcIMid4Zy3ik724FVSb0slbsKljIWWVq4ulik'}, 'CreationDate': '2016-08-28T09:00:52.061Z', 'PartSizeInBytes': 2097152, 'MultipartUploadId': 'XfiA5PM122LLolN5gu___oRss20GeXhvM0ZFJiAmvBhZXRSN1Py8ZNQHxkeFk24Vm0zOjMUodpYJ02WFJ7Hp7i7EACwF', 'VaultARN': 'arn:aws:glacier:eu-west-1:843392324993:vaults/dummy_vault'}
    '''
    assert self.name in self.parent.multipart_uploads
    resp = {
      'Parts' : self._parts,
      'PartSizeInBytes' : self.part_size_in_bytes,
      'MultipartUploadId' : self.id,
      'ResponseMetadata' : { 'HTTPStatusCode': 200 },
    }
    return resp

  def upload_part(self, **kwargs):
    '''
    {'checksum': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa', 'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {'x-amzn-requestid': 'vtUGut6f4b7v4LnX1QxYACThE7-2Rbcj8f0EgLFbdtb9k8M', 'x-amz-sha256-tree-hash': '560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa', 'date': 'Sat, 27 Aug 2016 18:39:44 GMT'}, 'RequestId': 'vtUGut6f4b7v4LnX1QxYACThE7-2Rbcj8f0EgLFbdtb9k8M'}}
    '''
    assert 'body' in kwargs
    assert 'range' in kwargs
    assert 'checksum' in kwargs
    assert self.name in self.parent.multipart_uploads
    size = len_range_from_mime(kwargs['range'])
    assert size == len(kwargs['body']), "size %r != len(boby) %r" % (size, len(kwargs['body']))

    ok_resp = {
      'checksum' : TreeHasher().digest_single_shot_as_hexstr(kwargs['body']),
      'ResponseMetadata' : { 'HTTPStatusCode': 204 },
    }
    resp = DummySession.behaviour(type(self), self.upload_part, ok_resp)
    if resp[0]:
      self._parts.append({
        'SHA256TreeHash' : TreeHasher().digest_single_shot_as_hexstr(kwargs['body']),
        'RangeInBytes' : kwargs['range'],
      })
      self._archive._addPart(kwargs['body'], self.part_size_in_bytes)
    return resp[1]

class DummyJob (DummyResource):
  ArchiveRetrieval, InventoryRetrieval = 'ArchiveRetrieval','InventoryRetrieval'
  Failed = 'Failed'
  InProgress = 'InProgress'

  def __init__ (self, name, parent, action, start=None, end=None):
    super().__init__(name, parent)
    self._start = start
    self._end = end
    self.job_id = str(uuid.uuid4())
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
    return DummySession.behaviour(type(self), self.get_output, response)

class DummyArchive (DummyResource):
  def __init__ (self, name, parent):
    super().__init__( str(uuid.uuid4()) , parent)
    self._description = name
    self._content = tempfile.TemporaryFile()
    self._size = 0

  def _create (self):
    self.parent._archives[self.id] = self

  def _close(self):
    if self._content:
      self._content.close()

  def initiate_archive_retrieval (self, **kwargs):
    return self.parent.initiate_inventory_retrieval(**kwargs)

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

  def _addPart (self, part, check_start_aligns=0):
    assert not check_start_aligns or self._size % check_start_aligns == 0
    self._content.write(part)
    self._size += len(part)
  
###################################################################################################################


class DummyS3 (DummyResource):
  def __init__ (self, name):
    super().__init__(name, None)
    self._created = True
    self.buckets = DummyCollection()
  
  def Bucket(self, name):
    bucket = self.buckets.get(name)
    if bucket:
      return bucket
    return DummyBucket(name, self)

class DummyBucket (DummyResource):

  def __init__ (self, name, parent):
    super().__init__(name, parent)
    self.object_versions = DummyCollection()
    self.objects = DummyCollection()
    self._setattr('creation_date', datetime.datetime.now())

  def create (self, **kwargs):
    '''
    botocore.exceptions.ClientError: An error occurred (BucketAlreadyOwnedByYou) when calling the CreateBucket operation: 
                                     Your previous request to create the named bucket succeeded and you already own it.
    '''
    logger.info('Creating bucket %s', self.name)
    assert 'LocationConstraint' in kwargs['CreateBucketConfiguration']
    assert not self.name in self.parent.buckets
    resp = DummySession.behaviour(type(self), self.create)
    if not resp[0]: return resp[1]
    self.parent.buckets[self.name] = self
    self._created = True
    return resp[1]

  def download_file (self, key, filename):
    logger.info('Downloading %s to %s', key, filename)
    resp = DummySession.behaviour(type(self), self.download_file, RESP_VOID)
    if not resp[0]: return resp[1]

    with open(filename, 'wb') as fileobj:
      fileobj.write(self.objects[key]._bytes)
    return resp[1]

  def put_object (self, **kwargs):
    assert 'Body' in kwargs
    assert 'Key' in kwargs
    obj = self.objects.get(kwargs['Key'])
    if not obj:
      obj = DummyS3Object(kwargs['Key'], self)
      self.objects[ kwargs['Key']] = obj 
    obj._bytes = kwargs['Body']

  def upload_file (self, filename, key):
    obj = self.objects.get(key)
    if not obj:
      obj = DummyS3Object(key, self)
      self.objects[ kwargs['Key']] = obj 
    with open(filename, 'rb') as fileobj:
      obj._bytes = fileobj.read()

  def Lifecycle (self):
    return DummyLifecycle(self)

  def Object (self, key):
    return DummyS3Object(key, self)

class DummyLifecycle (DummyResource):
  def __init__ (self, parent):
    super().__init__(str(uuid.uuid4()), parent)

  def put (self, **kwargs):
    return DummySession.behaviour(type(self), self.put)

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
    assert self.name in self.parent.objects
    resp = DummySession.behaviour(type(self), self.delete)
    if not resp[0]: return resp[1]
    del self.parent.objects[self.name]
    self._created = False
    return resp[1]

  def download_file (self, filename):
    return self.parent.download_file(self.name, filename)

  def get (self):
    '''
    {'Metadata': {}, 'Body': <botocore.response.StreamingBody object at 0x7f57553d7ba8>, 'ResponseMetadata': {'HostId': '/ymvGE8jdY8JDw1zXQEYzfs8zuynQNeJeGDNwfLPOLW8QBQaJd+B7vhFYS9ggaSXNK4+r1lsXLs=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '/ymvGE8jdY8JDw1zXQEYzfs8zuynQNeJeGDNwfLPOLW8QBQaJd+B7vhFYS9ggaSXNK4+r1lsXLs=', 'content-length': '14', 'last-modified': 'Wed, 24 Aug 2016 21:44:55 GMT', 'x-amz-request-id': '51187B034C395B3A', 'date': 'Sat, 27 Aug 2016 13:41:41 GMT', 'content-type': 'binary/octet-stream', 'accept-ranges': 'bytes', 'x-amz-expiration': 'expiry-date="Sat, 03 Dec 2016 00:00:00 GMT", rule-id="test_clean_rule"', 'etag': '"618cfda8848cdfba75913a9779d03bdf"', 'server': 'AmazonS3'}, 'RequestId': '51187B034C395B3A'}, 'ContentType': 'binary/octet-stream', 'LastModified': datetime.datetime(2016, 8, 24, 21, 44, 55, tzinfo=tzutc()), 'ETag': '"618cfda8848cdfba75913a9779d03bdf"', 'Expiration': 'expiry-date="Sat, 03 Dec 2016 00:00:00 GMT", rule-id="test_clean_rule"', 'ContentLength': 14, 'AcceptRanges': 'bytes'}
    '''
    resp = DummySession.behaviour(type(self), self.get, Body=DummyStream(self._bytes))
    if resp[0]:
      self._created = True
    return resp[1]

  def put (self, **kwargs):
    assert 'Body' in kwargs
    resp = DummySession.behaviour(type(self), self.put)
    if not resp[0]: return resp[1]
    self._bytes = kwargs['Body']
    return resp[1]

  def upload_file (self, filename):
    resp = DummySession.behaviour(type(self), self.upload_file)
    if not resp[0]: return resp[1]
    with open(filename, 'rb') as fileobj:
      self._bytes = fileobj.read()
    return resp[1]


