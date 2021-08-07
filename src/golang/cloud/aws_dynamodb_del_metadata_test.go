package cloud

import (
  "context"
  "errors"
  "testing"
  "time"

  "btrfs_to_glacier/types"
)

func buildTestDelMetadata(t *testing.T) (*dynamoDelMetadata, *mockDynamoDbClient) {
  metadata, client := buildTestMetadata(t)
  del_meta := &dynamoDelMetadata{metadata}
  return del_meta, client
}

func TestDeleteSnapshotSeqHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestDelMetadata(t)
  expect_head := dummySnapshotSeqHead(dummySnapshotSequence("vol_uuid", "seq_uuid"))
  client.putForTest(expect_head.Uuid, expect_head)

  err := metadata.DeleteSnapshotSeqHead(ctx, expect_head.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }

  err = metadata.DeleteSnapshotSeqHead(ctx, expect_head.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    t.Errorf("Returned unexpected error: %v", err)
  }
}

func TestDeleteSnapshotSeq(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestDelMetadata(t)
  expect_seq := dummySnapshotSequence("vol_uuid", "seq_uuid")
  client.putForTest(expect_seq.Uuid, expect_seq)

  err := metadata.DeleteSnapshotSeq(ctx, expect_seq.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }

  err = metadata.DeleteSnapshotSeq(ctx, expect_seq.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    t.Errorf("Returned unexpected error: %v", err)
  }
}

func TestDeleteSnapshot(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestDelMetadata(t)
  expect_snap := dummySnapshot("snap_uuid", "vol_uuid")
  client.putForTest(expect_snap.Uuid, expect_snap)

  err := metadata.DeleteSnapshot(ctx, expect_snap.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }

  err = metadata.DeleteSnapshot(ctx, expect_snap.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    t.Errorf("Returned unexpected error: %v", err)
  }
}

