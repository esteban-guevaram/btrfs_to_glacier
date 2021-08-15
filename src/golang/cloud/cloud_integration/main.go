package main

import (
  "fmt"
  "time"
  "context"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
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

func main() {
  util.Infof("cloud_integration run")

  var err error
  var aws_conf *aws.Config
  var metadata types.DeleteMetadata
  ctx := context.Background()
  conf := util.LoadTestConf()
  //codec := new(types.MockCodec)

  aws_conf, err = cloud.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }
  metadata, err = cloud.NewDelMetadata(conf, aws_conf)
  if err != nil { util.Fatalf("%v", err) }

  TestMetadataSetup(ctx, metadata)
  TestAllReadWrite(ctx, metadata)
  TestAllDelete(ctx, metadata)
  util.Infof("ALL DONE")
}

