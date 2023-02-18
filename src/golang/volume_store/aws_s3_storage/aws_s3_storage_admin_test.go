package aws_s3_storage

import (
  "context"
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
)

func TestCreateLifecycleRule(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  bucket := storage.common.BackupConf.StorageBucketName
  client.Buckets[bucket] = true
  err := storage.createLifecycleRule(ctx, bucket)
  if err != nil { t.Fatalf("Failed lifecycle creation: %v", err) }
  lf_conf := client.LastLifecycleIn.LifecycleConfiguration
  if len(lf_conf.Rules) != 1 { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
}

func TestSetupBackupContent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  bucket := storage.common.BackupConf.StorageBucketName
  err := storage.SetupBackupContent(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if client.LastPublicAccessBlockIn == nil { t.Errorf("did not block ppublic access: %v", err) }
  if len(client.Buckets) != 1 { t.Errorf("Bad bucket creation: %v", err) } 
  if _,found := client.Buckets[bucket]; !found { t.Errorf("Bad bucket name: %v", err) } 
}

func TestSetupBackupContent_Fail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,client := buildTestAdminStorage(t)
  client.Err = fmt.Errorf("an unfortunate error")
  err := storage.SetupBackupContent(ctx)
  if err == nil { t.Errorf("Expected error in SetupBackupContent") }
}

func testDeleteChunks_Helper(t *testing.T, obj_count int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  storage, client := buildTestAdminStorage(t)
  chunks := make([]*pb.SnapshotChunks_Chunk, obj_count)
  for i,_ := range chunks {
    chunks[i] = &pb.SnapshotChunks_Chunk{
      Uuid: uuid.NewString(),
    }
    client.SetData(chunks[i].Uuid, []byte("value"), s3_types.StorageClassStandard, false)
  }

  err := storage.DeleteChunks(ctx, chunks)
  if err != nil { util.Fatalf("storage.DeleteChunks: %v", err) }

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

