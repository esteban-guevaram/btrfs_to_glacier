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
)

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
  snap := dummySnapshot(timedUuid("snap"), timedUuid("par"))
  chunk_1 := dummyChunks(timedUuid("chunk_1"))

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
  snap := dummySnapshot(timedUuid("snap"), vol_uuid)
  seq := dummySnapshotSequence(vol_uuid, timedUuid("seq"))

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
  seq := dummySnapshotSequence(vol_uuid, timedUuid("seq"))

  err := metadata.DeleteSnapshotSeqHead(ctx, vol_uuid)
  if !errors.Is(err, types.ErrNotFound) {
    util.Fatalf("TestDeleteSnapshotSeq %s: %v", vol_uuid, err)
  }

  _, err = metadata.RecordSnapshotSeqHead(ctx, seq)
  if err != nil { util.Fatalf("%v", err) }

  err = metadata.DeleteSnapshotSeqHead(ctx, vol_uuid)
  if err != nil { util.Fatalf("%v", err) }
}

func TestAllDynamoDbDelete(ctx context.Context, metadata types.AdminMetadata) {
  TestDeleteSnapshot(ctx, metadata)
  TestDeleteSnapshotSeq(ctx, metadata)
  TestDeleteSnapshotSeqHead(ctx, metadata)
}

