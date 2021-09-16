package cloud

import (
  "context"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
)

func buildTestDelStorage(t *testing.T) (*s3DeleteStorage, *mockS3Client) {
  conf := util.LoadTestConf()
  storage,client := buildTestStorageWithConf(t, conf)
  del_storage := &s3DeleteStorage{ s3Storage:storage, }
  return del_storage, client
}

func testDeleteChunks_Helper(t *testing.T, obj_count int) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  storage, client := buildTestDelStorage(t)
  chunks := &pb.SnapshotChunks{
    //KeyFingerprint: "whatever",
    Chunks: make([]*pb.SnapshotChunks_Chunk, obj_count),
  }
  for i,_ := range chunks.Chunks {
    chunks.Chunks[i] = &pb.SnapshotChunks_Chunk{
      Uuid: uuid.NewString(),
    }
    client.setObject(chunks.Chunks[i].Uuid, []byte("value"), s3_types.StorageClassStandard, false)
  }

  done := storage.DeleteChunks(ctx, chunks)
  util.WaitForClosure(t, ctx, done)

  for _,chunk := range chunks.Chunks {
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

  storage,_ := buildTestDelStorage(t)
  chunks := &pb.SnapshotChunks{}
  done := storage.DeleteChunks(ctx, chunks)

  select {
    case err := <-done:
      if err == nil { t.Errorf("expecting error for empty chunks") }
    case <-ctx.Done(): t.Fatalf("timeout")
  }
}

