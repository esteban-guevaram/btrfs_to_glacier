package main

import (
  "context"
  "errors"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

  "github.com/google/uuid"
)

type dynAdminTester struct { *dynReadWriteTester }

func TestDynamoDbMetadataSetup(ctx context.Context, conf *pb.Config, client *dynamodb.Client, metadata types.AdminMetadata) {
  _, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
    TableName: &conf.Aws.DynamoDb.TableName,
  })

  if err != nil {
    apiErr := new(dyn_types.ResourceNotFoundException)
    if !errors.As(err, &apiErr) { util.Fatalf("%v", err) }
    util.Infof("TestMetadataSetup '%s' not exist", conf.Aws.DynamoDb.TableName)
  } else {
    waiter := dynamodb.NewTableNotExistsWaiter(client)
    wait_rq := &dynamodb.DescribeTableInput{ TableName: &conf.Aws.DynamoDb.TableName, }
    err = waiter.Wait(ctx, wait_rq, 30 * time.Second)
    if err != nil { util.Fatalf("%v", err) }
    util.Infof("TestMetadataSetup '%s' deleted", conf.Aws.DynamoDb.TableName)
  }

  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }

  done = metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("Idempotent err: %v", err) }
    case <-ctx.Done():
  }
}

func TestDeleteSnapshot(ctx context.Context, metadata types.AdminMetadata) {
  snap := util.DummySnapshot(timedUuid("snap"), timedUuid("par"))
  chunk_1 := util.DummyChunks(timedUuid("chunk_1"))

  err := metadata.DeleteSnapshot(ctx, snap.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    util.Fatalf("TestDeleteSnapshot %s: %v", snap.Uuid, err)
  }

  _, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_1)
  if err != nil { util.Fatalf("%v", err) }

  err = metadata.DeleteSnapshot(ctx, snap.Uuid)
  if err != nil { util.Fatalf("%v", err) }
}

func TestDeleteSnapshotSeq(ctx context.Context, metadata types.AdminMetadata) {
  vol_uuid := timedUuid("vol")
  snap := util.DummySnapshot(timedUuid("snap"), vol_uuid)
  seq := util.DummySnapshotSequence(vol_uuid, timedUuid("seq"))

  err := metadata.DeleteSnapshotSeq(ctx, seq.Uuid)
  if !errors.Is(err, types.ErrNotFound) {
    util.Fatalf("TestDeleteSnapshotSeq %s: %v", seq.Uuid, err)
  }

  _, err = metadata.AppendSnapshotToSeq(ctx, seq, snap)
  if err != nil { util.Fatalf("%v", err) }

  err = metadata.DeleteSnapshotSeq(ctx, seq.Uuid)
  if err != nil { util.Fatalf("%v", err) }
}

func TestDeleteSnapshotSeqHead(ctx context.Context, metadata types.AdminMetadata) {
  vol_uuid := timedUuid("vol")
  seq := util.DummySnapshotSequence(vol_uuid, timedUuid("seq"))

  err := metadata.DeleteSnapshotSeqHead(ctx, vol_uuid)
  if !errors.Is(err, types.ErrNotFound) {
    util.Fatalf("TestDeleteSnapshotSeq %s: %v", vol_uuid, err)
  }

  _, err = metadata.RecordSnapshotSeqHead(ctx, seq)
  if err != nil { util.Fatalf("%v", err) }

  err = metadata.DeleteSnapshotSeqHead(ctx, vol_uuid)
  if err != nil { util.Fatalf("%v", err) }
}

func (self *dynAdminTester) testDeleteMetadataUuids_Helper(
    ctx context.Context, seq_cnt int, snap_cnt int, missing_cnt int) {
  seq_uuids := make([]string, 0, seq_cnt)
  snap_uuids := make([]string, 0, snap_cnt)
  for len(seq_uuids) < seq_cnt || len(snap_uuids) < snap_cnt {
    if len(seq_uuids) < seq_cnt {
      seq := util.DummySnapshotSequence(uuid.NewString(), uuid.NewString())
      seq_uuids = append(seq_uuids, seq.Uuid)
      self.putItemOrDie(ctx, seq.Uuid, seq)
    }
    if len(snap_uuids) < snap_cnt {
      snap := util.DummySnapshot(uuid.NewString(), uuid.NewString())
      snap_uuids = append(snap_uuids, snap.Uuid)
      self.putItemOrDie(ctx, snap.Uuid, snap)
    }
  }
  for i:=0; i<missing_cnt; i+=1 {
    snap_uuids = append(snap_uuids, uuid.NewString())
    seq_uuids = append(seq_uuids, uuid.NewString())
  }

  done := self.Metadata.DeleteMetadataUuids(ctx, seq_uuids, snap_uuids)
  err := util.WaitForClosureOrDie(ctx, done)
  if err != nil { util.Fatalf("BatchWriteItem error: %v", err) }

  for _,uuid := range seq_uuids {
    err := self.getItem(ctx, uuid, &pb.SnapshotSequence{})
    if !errors.Is(err, types.ErrNotFound) { util.Fatalf("failed to delete %s: %v", uuid, err) }
  }
  for _,uuid := range snap_uuids {
    err := self.getItem(ctx, uuid, &pb.SubVolume{})
    if !errors.Is(err, types.ErrNotFound) { util.Fatalf("failed to delete %s: %v", uuid, err) }
  }
}

func (self *dynAdminTester) TestDeleteMetadataUuids_Simple(ctx context.Context) {
  const seq_cnt = 3
  const snap_cnt = 5
  const missing_cnt = 0
  self.testDeleteMetadataUuids_Helper(ctx, seq_cnt, snap_cnt, missing_cnt)
}

func (self *dynAdminTester) TestDeleteMetadataUuids_MissingKeys(ctx context.Context) {
  const seq_cnt = 3
  const snap_cnt = 0
  const missing_cnt = 5
  self.testDeleteMetadataUuids_Helper(ctx, seq_cnt, snap_cnt, missing_cnt)
}

func (self *dynAdminTester) TestReplaceSnapshotSeqHead_Simple(ctx context.Context) {
  head_uuid := uuid.NewString()
  old_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence(head_uuid, uuid.NewString()))
  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence(head_uuid, uuid.NewString()))
  self.putItemOrDie(ctx, old_head.Uuid, old_head)
  got_old_head, err := self.Metadata.ReplaceSnapshotSeqHead(ctx, new_head)
  if err != nil { util.Fatalf("Returned error: %v", err) }

  got_new_head := &pb.SnapshotSeqHead{}
  self.getItemOrDie(ctx, new_head.Uuid, got_new_head)
  util.EqualsOrDie("OldSnapshotSeqHead", got_old_head, old_head)
  util.EqualsOrDie("NewSnapshotSeqHead", got_new_head, new_head)
}

func (self *dynAdminTester) TestReplaceSnapshotSeqHead_NoHead(ctx context.Context) {
  head_uuid := uuid.NewString()
  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence(head_uuid, uuid.NewString()))
  _, err := self.Metadata.ReplaceSnapshotSeqHead(ctx, new_head)
  if !errors.Is(err, types.ErrNotFound) { util.Fatalf("Should have failed to replace %s: %v", head_uuid, err) }
}

func TestAllDynamoDbDelete(
    ctx context.Context, conf *pb.Config, client *dynamodb.Client, metadata types.AdminMetadata) {
  suite := &dynAdminTester{
    &dynReadWriteTester{ Conf:conf, Client:client, Metadata:metadata, },
  }
  TestDeleteSnapshot(ctx, metadata)
  TestDeleteSnapshotSeq(ctx, metadata)
  TestDeleteSnapshotSeqHead(ctx, metadata)
  suite.TestDeleteMetadataUuids_Simple(ctx)
  suite.TestDeleteMetadataUuids_MissingKeys(ctx)
  suite.TestReplaceSnapshotSeqHead_Simple(ctx)
  suite.TestReplaceSnapshotSeqHead_NoHead(ctx)
}

