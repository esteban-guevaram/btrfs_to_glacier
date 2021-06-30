package main

import (
  "context"
  "errors"
  "fmt"
  "time"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "google.golang.org/protobuf/proto"
)

func timedUuid(base_uuid string) string {
  return fmt.Sprintf("%s-%d", base_uuid, time.Now().UnixNano())
}

func dummyChunks(chunk_uuid string) *pb.SnapshotChunks {
  chunk := &pb.SnapshotChunks_Chunk {
    Uuid: chunk_uuid,
    Start: 0,
    Size: 3,
  }
  return &pb.SnapshotChunks{
    KeyFingerprint: "fp",
    Chunks: []*pb.SnapshotChunks_Chunk{chunk},
  }
}

func dummySubVolume(vol_uuid string) *pb.SubVolume {
 return &pb.SubVolume{
    Uuid: vol_uuid,
    MountedPath: "/monkey/biz",
    CreatedTs: 666,
    OriginSys: &pb.SystemInfo{
      KernMajor: 1,
      BtrfsUsrMajor: 1,
      ToolGitCommit: "commit_hash",
    },
  }
}

func dummySnapshot(snap_uuid string, vol_uuid string) *pb.SubVolume {
  vol := dummySubVolume(snap_uuid)
  vol.ParentUuid = vol_uuid
  vol.ReadOnly = true
  vol.CreatedTs += 111
  vol.GenAtCreation = 777
  return vol
}

func dummySnapshotSequence(vol_uuid string, seq_uuid string) *pb.SnapshotSequence {
  vol := dummySubVolume(vol_uuid)
  snap := fmt.Sprintf("%s_snap", vol_uuid)
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    SnapUuids: []string{snap},
  }
}

func TestMetadataSetup(ctx context.Context, metadata types.Metadata) {
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }
}

func TestRecordSnapshotSeqHead(ctx context.Context, metadata types.Metadata) {
  var err error
  var head1, head2, head3 *pb.SnapshotSeqHead
  vol_uuid := timedUuid("vol")
  new_seq := dummySnapshotSequence(vol_uuid, timedUuid("seq1"))
  new_seq_2 := dummySnapshotSequence(vol_uuid, timedUuid("seq2"))

  head1, err = metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(head1.Uuid, new_seq.Volume.Uuid)
  util.EqualsOrDie(head1.CurSeqUuid, new_seq.Uuid)

  head2, err = metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(head2, head1)

  head3, err = metadata.RecordSnapshotSeqHead(ctx, new_seq_2)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(head3.CurSeqUuid, new_seq_2.Uuid)

  _, err = metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err == nil { util.Fatalf("Adding an old sequence should be an error") }
}

func TestAppendSnapshotToSeq(ctx context.Context, metadata types.Metadata) {
  var err error
  var seq_1, seq_noop, seq_2 *pb.SnapshotSequence
  vol_uuid := timedUuid("vol")
  seq_uuid := timedUuid("seq")
  snap1_uuid := timedUuid("snap1")
  snap2_uuid := timedUuid("snap2")
  snap_1 := dummySnapshot(snap1_uuid, vol_uuid)
  snap_2 := dummySnapshot(snap2_uuid, vol_uuid)
  expect_seq_0 := dummySnapshotSequence(vol_uuid, seq_uuid)
  expect_seq_0.SnapUuids = nil
  expect_seq_1 := dummySnapshotSequence(vol_uuid, seq_uuid)
  expect_seq_1.SnapUuids = []string{snap1_uuid}
  expect_seq_2 := dummySnapshotSequence(vol_uuid, seq_uuid)
  expect_seq_2.SnapUuids = []string{snap1_uuid, snap2_uuid}

  seq_1, err = metadata.AppendSnapshotToSeq(ctx, expect_seq_0, snap_1)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(seq_1, expect_seq_1)

  seq_noop, err = metadata.AppendSnapshotToSeq(ctx, seq_1, snap_1)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(seq_noop, expect_seq_1)

  seq_2, err = metadata.AppendSnapshotToSeq(ctx, expect_seq_1, snap_2)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(seq_2, expect_seq_2)
}

func TestAppendChunkToSnapshot(ctx context.Context, metadata types.Metadata) {
  snap := dummySnapshot(timedUuid("snap"), timedUuid("par"))
  chunk_1 := dummyChunks(timedUuid("chunk_1"))
  chunk_2 := dummyChunks(timedUuid("chunk_2"))
  chunk_2.Chunks[0].Start = chunk_1.Chunks[0].Size

  expect_first := proto.Clone(snap).(*pb.SubVolume)
  expect_first.Data = proto.Clone(chunk_1).(*pb.SnapshotChunks)
  expect_second := proto.Clone(expect_first).(*pb.SubVolume)
  expect_second.Data.Chunks = append(expect_second.Data.Chunks,
                                     proto.Clone(chunk_2.Chunks[0]).(*pb.SnapshotChunks_Chunk))

  var err error
  var written_snap_1, written_snap_2, written_snap_3, written_snap_4 *pb.SubVolume
  written_snap_1, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_1)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(written_snap_1, expect_first)

  written_snap_2, err = metadata.AppendChunkToSnapshot(ctx, written_snap_1, chunk_1)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(written_snap_2, written_snap_1)

  written_snap_3, err = metadata.AppendChunkToSnapshot(ctx, written_snap_1, chunk_2)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(written_snap_3, expect_second)

  written_snap_4, err = metadata.AppendChunkToSnapshot(ctx, written_snap_3, chunk_2)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(written_snap_4, written_snap_3)
}

func TestReadSnapshotSeqHead(ctx context.Context, metadata types.Metadata) {
  var err error
  var expect_head, head *pb.SnapshotSeqHead
  vol_uuid := timedUuid("vol")
  seq := dummySnapshotSequence(vol_uuid, timedUuid("seq1"))

  _, err = metadata.ReadSnapshotSeqHead(ctx, vol_uuid)
  if !errors.Is(err, types.ErrNotFound) { util.Fatalf("%v", err) }

  expect_head, err = metadata.RecordSnapshotSeqHead(ctx, seq)
  if err != nil { util.Fatalf("%v", err) }

  head, err = metadata.ReadSnapshotSeqHead(ctx, vol_uuid)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(expect_head, head)
}

func TestReadSnapshotSeq(ctx context.Context, metadata types.Metadata) {
  var err error
  var empty_seq, expect_seq, seq *pb.SnapshotSequence
  vol_uuid := timedUuid("vol")
  seq_uuid := timedUuid("seq")
  snap_uuid := timedUuid("snap")
  snap := dummySnapshot(snap_uuid, vol_uuid)
  empty_seq = dummySnapshotSequence(vol_uuid, seq_uuid)

  _, err = metadata.ReadSnapshotSeq(ctx, empty_seq.Uuid)
  if !errors.Is(err, types.ErrNotFound) { util.Fatalf("%v", err) }

  expect_seq, err = metadata.AppendSnapshotToSeq(ctx, empty_seq, snap)
  if err != nil { util.Fatalf("%v", err) }

  seq, err = metadata.ReadSnapshotSeq(ctx, empty_seq.Uuid)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(expect_seq, seq)
}

func TestReadSnapshot(ctx context.Context, metadata types.Metadata) {
  var err error
  var snap *pb.SubVolume
  vol_uuid := timedUuid("vol")
  snap_uuid := timedUuid("snap")
  chunk_uuid := timedUuid("seq")
  empty_snap := dummySnapshot(snap_uuid, vol_uuid)
  expect_snap := dummySnapshot(snap_uuid, vol_uuid)
  expect_snap.Data = dummyChunks(chunk_uuid)

  _, err = metadata.ReadSnapshot(ctx, expect_snap.Uuid)
  if !errors.Is(err, types.ErrNotFound) { util.Fatalf("%v", err) }

  expect_snap, err = metadata.AppendChunkToSnapshot(ctx, empty_snap, dummyChunks(chunk_uuid))
  if err != nil { util.Fatalf("%v", err) }

  snap, err = metadata.ReadSnapshot(ctx, expect_snap.Uuid)
  if err != nil { util.Fatalf("%v", err) }
  util.EqualsOrDie(expect_snap, snap)
}

func main() {
  util.Infof("cloud_integration run")

  var err error
  var aws_conf *aws.Config
  var metadata types.Metadata
  ctx := context.Background()
  conf := util.LoadTestConf()
  codec := new(types.MockCodec)
  aws_conf, err = cloud.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }
  metadata, err = cloud.NewMetadata(conf, aws_conf, codec)
  if err != nil { util.Fatalf("%v", err) }

  TestMetadataSetup(ctx, metadata)
  TestRecordSnapshotSeqHead(ctx, metadata)
  TestAppendSnapshotToSeq(ctx, metadata)
  TestAppendChunkToSnapshot(ctx, metadata)
  TestReadSnapshotSeqHead(ctx, metadata)
  TestReadSnapshotSeq(ctx, metadata)
  TestReadSnapshot(ctx, metadata)
  util.Infof("ALL DONE")
}

