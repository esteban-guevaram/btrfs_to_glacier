package main

import (
  "context"
  "fmt"
  "time"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
)

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
  vol_uuid := fmt.Sprintf("salut-%d", time.Now().UnixNano())
  snap_uuid := "coucou_snap"
  vol := &pb.SubVolume{
    Uuid: vol_uuid,
    MountedPath: "/monkey/biz",
    CreatedTs: 666,
    OriginSys: &pb.SystemInfo{
      KernMajor: 1,
      BtrfsUsrMajor: 1,
      ToolGitCommit: "commit_hash",
    },
  }
  new_seq := &pb.SnapshotSequence{
    Uuid: fmt.Sprintf("coucou-%d", time.Now().UnixNano()),
    Volume: vol,
    SnapUuids: []string{snap_uuid},
  }
  new_seq_2 := &pb.SnapshotSequence{
    Uuid: fmt.Sprintf("coucou2-%d", time.Now().UnixNano()),
    Volume: vol,
    SnapUuids: []string{snap_uuid},
  }
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
  util.Infof("ALL DONE")
}

