package aws_s3_storage

import (
  "context"
  "fmt"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
)

func buildTestAdminStorage(t *testing.T) (*s3AdminStorage, *mockS3Client) {
  conf := util.LoadTestConf()
  storage,client := buildTestStorageWithConf(t, conf)
  del_storage := &s3AdminStorage{ s3Storage:storage, }
  return del_storage, client
}

func TestBucketCreation_Immediate(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  err := storage.createBucket(ctx)
  if err != nil { t.Fatalf("Failed aws create bucket: %v", err) }
  if _,ok := client.Buckets[storage.conf.Aws.S3.BucketName]; !ok {
    t.Fatalf("Create bucket did not do a thing: %v", err)
  }
  block_conf := client.LastPublicAccessBlockIn.PublicAccessBlockConfiguration
  if !block_conf.BlockPublicAcls { t.Fatalf("Malformed request: %v", *(client.LastPublicAccessBlockIn)) }
}

func TestBucketCreation_Timeout(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  client.HeadAlwaysEmpty = true
  err := storage.createBucket(ctx)
  if err == nil { t.Fatalf("Expected create bucket to timeout") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_NoBucket(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestAdminStorage(t)
  exists, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if exists { t.Fatalf("there should have been no bucket") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_BadOwner(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  client.HeadAlwaysAccessDenied = true
  client.Buckets[storage.conf.Aws.S3.BucketName] = true
  _, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err == nil { t.Fatalf("Expected wrong bucket owner") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_Exists(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  client.Buckets[storage.conf.Aws.S3.BucketName] = true
  exists, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if !exists { t.Fatalf("there should have been an existing bucket") }
}

func TestCreateLifecycleRule(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  err := storage.createLifecycleRule(ctx)
  if err != nil { t.Fatalf("Failed lifecycle creation: %v", err) }
  lf_conf := client.LastLifecycleIn.LifecycleConfiguration
  if len(lf_conf.Rules) != 1 { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
}

func TestSetupStorage(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestAdminStorage(t)
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

func TestSetupStorage_Fail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  client.Err = fmt.Errorf("an unfortunate error")
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err == nil { t.Errorf("Expected error in SetupStorage") }
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

func testDeleteChunks_Helper(t *testing.T, obj_count int) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  storage, client := buildTestAdminStorage(t)
  chunks := make([]*pb.SnapshotChunks_Chunk, obj_count)
  for i,_ := range chunks {
    chunks[i] = &pb.SnapshotChunks_Chunk{
      Uuid: uuid.NewString(),
    }
    client.setObject(chunks[i].Uuid, []byte("value"), s3_types.StorageClassStandard, false)
  }

  done := storage.DeleteChunks(ctx, chunks)
  util.WaitForClosure(t, ctx, done)

  for _,chunk := range chunks {
    _,found := client.Data[chunk.Uuid]
    if found { t.Errorf("Failed deletion of: %s", chunk.Uuid) }
  }
}

func TestDeleteChunks_SingleBatch(t *testing.T) {
  testDeleteChunks_Helper(t, 3)
}

func TestDeleteChunks_MultiBatch(t *testing.T) {
  testDeleteChunks_Helper(t, delete_objects_max + 3)
  testDeleteChunks_Helper(t, 2*delete_objects_max)
}

func TestDeleteChunks_NoKeysErr(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  storage,_ := buildTestAdminStorage(t)
  chunks := make([]*pb.SnapshotChunks_Chunk, 0, 1)
  done := storage.DeleteChunks(ctx, chunks)

  select {
    case err := <-done:
      if err == nil { t.Errorf("expecting error for empty chunks") }
    case <-ctx.Done(): t.Fatalf("timeout")
  }
}

