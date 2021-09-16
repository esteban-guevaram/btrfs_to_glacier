package cloud

import (
  "context"
  "errors"
  "testing"
  "time"

  "btrfs_to_glacier/types"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func buildTestAdminMetadata(t *testing.T) (*dynamoAdminMetadata, *mockDynamoDbClient) {
  metadata, client := buildTestMetadata(t)
  del_meta := &dynamoAdminMetadata{metadata}
  return del_meta, client
}

func TestTableCreation_Immediate(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Immediate timeout")
  }
}

func TestTableCreation_Wait(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusCreating,
  }
  client.DescribeTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Immediate timeout")
  }
}

func TestTableCreation_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.Err = &dyn_types.ResourceInUseException{}
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Wait timeout")
  }
}

func TestDeleteSnapshotSeqHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
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
  metadata, client := buildTestAdminMetadata(t)
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
  metadata, client := buildTestAdminMetadata(t)
  expect_snap := dummySnapshot("snap_uuid", "vol_uuid")
  client.putForTest(expect_snap.Uuid, expect_snap)

  err := metadata.DeleteSnapshot(ctx, expect_snap.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }

  err = metadata.DeleteSnapshot(ctx, expect_snap.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    t.Errorf("Returned unexpected error: %v", err)
  }
}

