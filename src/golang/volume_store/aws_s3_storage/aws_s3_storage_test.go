package aws_s3_storage

import (
  "context"
  "fmt"
  "testing"

  "btrfs_to_glacier/encryption"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"
  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
  "github.com/google/uuid"
)

type ChunkIoForTestImpl struct { *ChunkIoImpl }

func (self *ChunkIoForTestImpl) MockClient() *s3_common.MockS3Client { return self.Parent.Client.(*s3_common.MockS3Client) }
func (self *ChunkIoForTestImpl) Get(uuid string) ([]byte, bool) { return self.MockClient().GetData(uuid) }
func (self *ChunkIoForTestImpl) Set(uuid string, data []byte) {
  self.MockClient().SetData(uuid, data, s3_types.StorageClassStandard, false)
}
func (self *ChunkIoForTestImpl) Len() int { return len(self.MockClient().Data) }
func (self *ChunkIoForTestImpl) SetCodecFp(fp string) {
  self.Parent.Codec.(*mocks.Codec).Fingerprint = types.PersistableString{fp}
}
func (self *ChunkIoForTestImpl) GetCodecFp() types.PersistableString {
  return self.Parent.Codec.(*mocks.Codec).CurrentKeyFingerprint()
}
func (self *ChunkIoForTestImpl) AlwaysReturnErr(storage types.BackupContent, err error) {
  base_storage := storage.(*s3Storage).BaseStorage
  base_storage.ChunkIo = mocks.AlwaysErrChunkIo(storage, err)
}

func buildTestAdminStorage(t *testing.T) (*s3StorageAdmin, *s3_common.MockS3Client) {
  conf := util.LoadTestConf()
  return buildTestStorageWithConf(t, conf)
}

func buildTestStorage(t *testing.T) (*s3Storage, *s3_common.MockS3Client) {
  del_storage, client := buildTestAdminStorage(t)
  return del_storage.s3Storage, client
}

func buildTestAdminStorageWithChunkLen(t *testing.T, chunk_len uint64) (*s3StorageAdmin, *s3_common.MockS3Client) {
  conf := util.LoadTestConf()
  conf.Backups[0].Aws.S3.ChunkLen = chunk_len
  return buildTestStorageWithConf(t, conf)
}

func buildTestStorageWithChunkLen(t *testing.T, chunk_len uint64) (*s3Storage, *s3_common.MockS3Client) {
  del_storage, client := buildTestAdminStorageWithChunkLen(t, chunk_len)
  return del_storage.s3Storage, client
}

func buildTestStorageWithConf(t *testing.T, conf *pb.Config) (*s3StorageAdmin, *s3_common.MockS3Client) {
  client := &s3_common.MockS3Client {
    AccountId: "some_random_string",
    Data: make(map[string][]byte),
    Class: make(map[string]s3_types.StorageClass),
    RestoreStx: make(map[string]string),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  codec := new(mocks.Codec)
  aws_conf, err := encryption.TestOnlyAwsConfFromPlainKey(conf, "", "", "")
  if err != nil { t.Fatalf("Failed aws config: %v", err) }
  common, err := s3_common.NewS3Common(conf, aws_conf, conf.Backups[0].Name, client)
  if err != nil { t.Fatalf("Failed build common setup: %v", err) }
  common.BucketWait = util.TestTimeout
  common.AccountId = client.AccountId

  inner_storage := &mem_only.BaseStorage{
    Conf: conf,
    Codec: codec,
  }
  storage := &s3Storage{
    BaseStorage: inner_storage,
    Client: client,
    Uploader: client,
    aws_conf: aws_conf,
    common: common,
  }
  storage.ChunkIo = &ChunkIoImpl{ Parent:storage, }
  storage.injectConstants()
  del_storage := &s3StorageAdmin{ s3Storage:storage, }
  del_storage.injectConstants()
  return del_storage, client
}

func TestAllS3Storage(t *testing.T) {
  admin_ctor := func(t *testing.T, chunk_len uint64) (types.AdminBackupContent, mem_only.ChunkIoForTest) {
    storage,_ := buildTestAdminStorageWithChunkLen(t, chunk_len)
    for_test := &ChunkIoForTestImpl{ ChunkIoImpl: storage.ChunkIo.(*ChunkIoImpl) }
    return storage, for_test
  }
  storage_ctor := func(t *testing.T, chunk_len uint64) (types.BackupContent, mem_only.ChunkIoForTest) {
    del_storage,_ := buildTestAdminStorageWithChunkLen(t, chunk_len)
    for_test := &ChunkIoForTestImpl{ ChunkIoImpl: del_storage.ChunkIo.(*ChunkIoImpl) }
    return del_storage.s3Storage, for_test
  }
  fixture := &mem_only.Fixture{
    StorageCtor: storage_ctor,
    AdminCtor:   admin_ctor,
  }
  mem_only.RunAllTestStorage(t, fixture)
}

//////////////////////////////////// Tests tailored to implementation /////////////////////////

// Restore testing with aws-cli
//
// aws s3api list-objects-v2 --bucket candide.test.bucket.1
// {
//     "Contents": [
//         { "Key": "s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2", "StorageClass": "STANDARD" },
//         { "Key": "s3_obj_983680415d7ec9ccf80c88df8e3d4d7e", "StorageClass": "DEEP_ARCHIVE" },
//         { "Key": "s3_obj_ded6568a3a377ba99e41d06053fc00ce", "StorageClass": "GLACIER" },
//         { "Key": "to_archive/", "StorageClass": "DEEP_ARCHIVE" },
//         { "Key": "to_archive/s3_obj_3cfa5e4cecc6576a1dac67b193c72b13", "StorageClass": "DEEP_ARCHIVE" }
//     ]
// }
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// (no output)
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"true\"",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_983680415d7ec9ccf80c88df8e3d4d7e
// {
//     "Metadata": {},
//     "StorageClass": "DEEP_ARCHIVE"
// }
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// An error occurred (RestoreAlreadyInProgress) when calling the RestoreObject operation: Object restore is already in progress
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2 --restore-request Days=3
// An error occurred (InvalidObjectState) when calling the RestoreObject operation: Restore is not allowed for the object's current storage class
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2
// { "Metadata": {} } # Note: no mention of storage class nor restore status
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce /tmp/ded6568a3a377ba99e41d06053fc00ce
// An error occurred (InvalidObjectState) when calling the GetObject operation: The operation is not valid for the object's storage class
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2 /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
// {
//     "ContentLength": 524288,
//     "ContentType": "binary/octet-stream",
//     "Metadata": {}
// }
// md5sum /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
// 4d93c3eab6d1cfc9d7043b8cc45ea7d2  /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"false\", expiry-date=\"Thu, 09 Sep 2021 00:00:00 GMT\"",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce /tmp/ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"false\", expiry-date=\"Thu, 09 Sep 2021 00:00:00 GMT\"",
//     "ContentLength": 524288,
//     "ContentType": "binary/octet-stream",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// md5sum /tmp/ded6568a3a377ba99e41d06053fc00ce 
// ded6568a3a377ba99e41d06053fc00ce  /tmp/ded6568a3a377ba99e41d06053fc00ce
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// (no output) # extend restore lifetime
func testQueueRestoreObjects_Helper(
    t *testing.T, keys []string, class s3_types.StorageClass, ongoing bool, expect_obj types.ObjRestoreOrErr, restore_err error) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,client := buildTestStorage(t)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys {
    client.SetData(k, []byte{}, class, ongoing)
    expect[k] = expect_obj
  }
  client.RestoreObjectErr = restore_err
  res := storage.QueueRestoreObjects(ctx, keys)
  util.EqualsOrFailTest(t, "Bad queue result", res, expect)
  t.Logf("Error? %v", res)
}

func TestQueueRestoreObjects_Simple(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Pending, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, true, expect_obj, nil)
}

func TestQueueRestoreObjects_AlreadyRestored(t *testing.T) {
  keys := []string{"k1"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, false, expect_obj, nil)
}

func TestQueueRestoreObjects_RestoreOngoing(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := s3_common.StrToApiErr(RestoreAlreadyInProgress)
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Pending, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_NotArchived(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := new(s3_types.InvalidObjectState)
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassStandard, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_NoSuchObject(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := new(s3_types.NoSuchKey)
  expect_obj := types.ObjRestoreOrErr{ Err:types.ErrChunkFound, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassStandard, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_HeadFail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,_ := buildTestStorage(t)
  keys := []string{"k1", "k2"}
  res := storage.QueueRestoreObjects(ctx, keys)
  for k,s := range res {
    if s.Err == nil { t.Errorf("Expected error for %v:%v", k, s) }
  }
}

func testStorageListAll_Helper(t *testing.T, total int, fill_size int32) {
  const blob_len = 32
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage, client := buildTestStorage(t)
  storage.ChunkIo.(*ChunkIoImpl).IterBufLen = fill_size
  expect_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    obj := &pb.SnapshotChunks_Chunk{ Uuid:key, Size:blob_len, }
    client.SetData(key, util.GenerateRandomTextData(blob_len),
                     s3_types.StorageClassStandard, false)
    expect_objs[key] = obj
  }

  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { t.Fatalf("failed while iterating: %v", it.Err()) }

  util.EqualsOrFailTest(t, "Bad len", len(got_objs), len(expect_objs))
  for key,expect := range expect_objs {
    util.EqualsOrFailTest(t, "Bad obj", got_objs[key], expect)
  }
}

func TestListAllChunks_SingleFill(t *testing.T) {
  const fill_size = 10
  const total = 3
  testStorageListAll_Helper(t, total, fill_size)
}
func TestListAllChunks_MultipleFill(t *testing.T) {
  const fill_size = 3
  const total = 10
  testStorageListAll_Helper(t, total, fill_size)
}
func TestListAllChunks_EmptyNonFinalFill(t *testing.T) {
  const fill_size = 3
  const total = 2 * fill_size
  const blob_len = 32
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  storage, client := buildTestStorage(t)
  storage.ChunkIo.(*ChunkIoImpl).IterBufLen = fill_size

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    client.SetData(key, util.GenerateRandomTextData(blob_len),
                     s3_types.StorageClassStandard, false)
  }

  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { t.Fatalf("failed while iterating: %v", it.Err()) }
  util.EqualsOrFailTest(t, "Bad len", len(got_objs), total)
}
func TestListAllChunks_ErrDuringIteration(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage, client := buildTestStorage(t)
  client.Err = fmt.Errorf("iteration fail")
  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  if it.Next(ctx, obj) { t.Errorf("should not have returned any object") }
  if it.Err() == nil { t.Fatalf("expected error") }
}

