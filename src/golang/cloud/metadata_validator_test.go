package cloud

import (
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
)

func dummySnapshotSequence(vol_uuid string, seq_uuid string) *pb.SnapshotSequence {
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
  snap := &pb.SubVolume {
    Uuid: fmt.Sprintf("%s_snap", vol_uuid),
    ParentUuid: vol_uuid,
  }
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    Snaps: []*pb.SubVolume{snap},
  }
}

func dummySnapshotSeqHead(seq *pb.SnapshotSequence, prev ...string) *pb.SnapshotSeqHead {
  return &pb.SnapshotSeqHead{
    Uuid: seq.Volume.Uuid,
    CurSeqUuid: seq.Uuid,
    PrevSeqUuid: prev,
  }
}

func TestValidateSnapshotSequenceBeforeAddingToHead(t *testing.T) {
  seq := dummySnapshotSequence("vol", "seq")
  bad_seq := &pb.SnapshotSequence{}
  head := dummySnapshotSeqHead(seq, "s1", "s2")
  err := ValidateSnapshotSequenceBeforeAddingToHead(head, seq)
  if err != nil { t.Errorf("this should be ok: %v", seq) }
  err = ValidateSnapshotSequenceBeforeAddingToHead(head, bad_seq)
  if err == nil { t.Errorf("empty sequence should be ko: %v", bad_seq) }
}

