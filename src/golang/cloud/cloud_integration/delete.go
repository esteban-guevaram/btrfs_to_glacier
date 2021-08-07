package main

import (
  "context"
  "errors"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

func TestDeleteSnapshot(ctx context.Context, metadata types.DeleteMetadata) {
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

func TestDeleteSnapshotSeq(ctx context.Context, metadata types.DeleteMetadata) {
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

func TestDeleteSnapshotSeqHead(ctx context.Context, metadata types.DeleteMetadata) {
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

func TestAllDelete(ctx context.Context, metadata types.DeleteMetadata) {
  TestDeleteSnapshot(ctx, metadata)
  TestDeleteSnapshotSeq(ctx, metadata)
  TestDeleteSnapshotSeqHead(ctx, metadata)
}

