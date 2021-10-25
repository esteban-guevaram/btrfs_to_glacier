package util

import (
  "fmt"
  "hash/adler32"

  pb "btrfs_to_glacier/messages"
)

func DummyChunks(chunk_uuid string) *pb.SnapshotChunks {
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

func DummySubVolume(vol_uuid string) *pb.SubVolume {
 return &pb.SubVolume{
    Uuid: vol_uuid,
    VolId: (uint64)(adler32.Checksum([]byte(vol_uuid))),
    TreePath: vol_uuid,
    MountedPath: fmt.Sprintf("/banana_stashes/%s", vol_uuid),
    CreatedTs: 666,
    OriginSys: &pb.SystemInfo{
      KernMajor: 1,
      BtrfsUsrMajor: 1,
      ToolGitCommit: "commit_hash",
    },
  }
}

func DummySnapshot(snap_uuid string, vol_uuid string) *pb.SubVolume {
  vol := DummySubVolume(snap_uuid)
  vol.ParentUuid = vol_uuid
  vol.TreePath = fmt.Sprintf("snaps/%s", snap_uuid)
  vol.MountedPath = ""
  vol.ReadOnly = true
  vol.CreatedTs += 111
  vol.GenAtCreation = 777
  return vol
}

func DummySnapshotSequence(vol_uuid string, seq_uuid string) *pb.SnapshotSequence {
  vol := DummySubVolume(vol_uuid)
  snap := fmt.Sprintf("%s_snap", vol_uuid)
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    SnapUuids: []string{snap},
  }
}

func DummySnapshotSeqHead(seq *pb.SnapshotSequence, prev ...string) *pb.SnapshotSeqHead {
  return &pb.SnapshotSeqHead{
    Uuid: seq.Volume.Uuid,
    CurSeqUuid: seq.Uuid,
    PrevSeqUuid: prev,
  }
}

