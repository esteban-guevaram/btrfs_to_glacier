package aws_s3_storage

import (
  "context"
  "fmt"
  "testing"
  "time"

  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
)

func buildTestAdminStorage(t *testing.T) (*s3AdminStorage, *s3_common.MockS3Client) {
  conf := util.LoadTestConf()
  storage,client := buildTestStorageWithConf(t, conf)
  del_storage := &s3AdminStorage{ s3Storage:storage, }
  del_storage.injectConstants()
  return del_storage, client
}

func TestCreateLifecycleRule(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  bucket := storage.conf.Aws.S3.StorageBucketName
  client.Buckets[bucket] = true
  err := storage.createLifecycleRule(ctx, bucket)
  if err != nil { t.Fatalf("Failed lifecycle creation: %v", err) }
  lf_conf := client.LastLifecycleIn.LifecycleConfiguration
  if len(lf_conf.Rules) != 1 { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
}

func TestSetupStorage(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  bucket := storage.conf.Aws.S3.StorageBucketName
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      if client.LastPublicAccessBlockIn == nil { t.Errorf("did not block ppublic access: %v", err) }
      if len(client.Buckets) != 1 { t.Errorf("Bad bucket creation: %v", err) } 
      if _,found := client.Buckets[bucket]; !found { t.Errorf("Bad bucket name: %v", err) } 
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
    client.SetObject(chunks[i].Uuid, []byte("value"), s3_types.StorageClassStandard, false)
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

