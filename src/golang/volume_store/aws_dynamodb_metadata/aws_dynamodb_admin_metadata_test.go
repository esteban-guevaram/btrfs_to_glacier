package aws_dynamodb_metadata

import (
  "context"
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

  "github.com/google/uuid"
)

func buildTestAdminMetadata(t *testing.T) (*dynamoAdminMetadata, *mockDynamoDbClient) {
  metadata, client := buildTestMetadata(t)
  del_meta := &dynamoAdminMetadata{
    dynamoMetadata: metadata,
    delete_batch: delete_batch,
  }
  return del_meta, client
}

func TestTableCreation_Immediate(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  err := metadata.SetupMetadata(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestTableCreation_Wait(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusCreating,
  }
  client.DescribeTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  err := metadata.SetupMetadata(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestTableCreation_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.Err = &dyn_types.ResourceInUseException{}
  err := metadata.SetupMetadata(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func testDeleteMetadataUuids_Helper(t *testing.T, seq_cnt int, snap_cnt int, batch_size int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  metadata.delete_batch = batch_size
  seq_uuids := make([]string, 0, seq_cnt)
  snap_uuids := make([]string, 0, snap_cnt)
  for len(seq_uuids) < seq_cnt || len(snap_uuids) < snap_cnt {
    if len(seq_uuids) < seq_cnt {
      seq := util.DummySnapshotSequence(uuid.NewString(), uuid.NewString())
      seq_uuids = append(seq_uuids, seq.Uuid)
      client.putForTest(seq.Uuid, seq)
    }
    if len(snap_uuids) < snap_cnt {
      snap := util.DummySnapshot(uuid.NewString(), uuid.NewString())
      snap_uuids = append(snap_uuids, snap.Uuid)
      client.putForTest(snap.Uuid, snap)
    }
  }

  err := metadata.DeleteMetadataUuids(ctx, seq_uuids, snap_uuids)
  if err != nil { t.Errorf("metadata.DeleteMetadataUuids: %v", err) }

  for _,uuid := range seq_uuids {
    if client.getForTest(uuid, &pb.SnapshotSequence{}) { t.Errorf("Did not delete: %v", uuid) }
  }
  for _,uuid := range snap_uuids {
    if client.getForTest(uuid, &pb.SubVolume{}) { t.Errorf("Did not delete: %v", uuid) }
  }
}

func TestDeleteMetadataUuids_SingleBatch(t *testing.T) {
  const seq_cnt = 2
  const snap_cnt = 2
  const batch_size = 100
  testDeleteMetadataUuids_Helper(t, seq_cnt, snap_cnt, batch_size)
  // idempotent
  testDeleteMetadataUuids_Helper(t, seq_cnt, snap_cnt, batch_size)
}

func TestDeleteMetadataUuids_MultiBatch(t *testing.T) {
  const seq_cnt = 7
  const snap_cnt = 5
  const batch_size = 3
  testDeleteMetadataUuids_Helper(t, seq_cnt, snap_cnt, batch_size)
}

func TestDeleteMetadataUuids_NoItemNoError(t *testing.T) {
  const batch_size = 100
  testDeleteMetadataUuids_Helper(t, 1, 0, batch_size)
  testDeleteMetadataUuids_Helper(t, 0, 1, batch_size)
}

func TestDeleteMetadataUuids_DynamoError(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  client.Err = fmt.Errorf("fiasco")
  seq_uuids := []string { "uuid1" }
  snap_uuids := []string { "uuid2" }

  err := metadata.DeleteMetadataUuids(ctx, seq_uuids, snap_uuids)
  if err == nil { t.Fatalf("expected error.") }
}

func TestReplaceSnapshotSeqHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata, client := buildTestAdminMetadata(t)
  old_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence("vol", "seq_old"))
  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence("vol", "seq_new"))
  client.putForTest(old_head.Uuid, old_head)
  got_old_head, err := metadata.ReplaceSnapshotSeqHead(ctx, new_head)
  if err != nil { t.Errorf("Returned error: %v", err) }

  got_new_head := &pb.SnapshotSeqHead{}
  client.getForTest(new_head.Uuid, got_new_head)
  util.EqualsOrFailTest(t, "OldSnapshotSeqHead", got_old_head, old_head)
  util.EqualsOrFailTest(t, "NewSnapshotSeqHead", got_new_head, new_head)
}

func TestReplaceSnapshotSeqHead_NoOldHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  metadata,_ := buildTestAdminMetadata(t)
  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence("vol", "seq_new"))
  _, err := metadata.ReplaceSnapshotSeqHead(ctx, new_head)
  if err == nil { t.Errorf("expected error.") }
}

