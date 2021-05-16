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
  snap := fmt.Sprintf("%s_snap", vol_uuid)
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    SnapUuids: []string{snap},
  }
}

func dummySnapshotSeqHead(seq *pb.SnapshotSequence, prev ...string) *pb.SnapshotSeqHead {
  return &pb.SnapshotSeqHead{
    Uuid: seq.Volume.Uuid,
    CurSeqUuid: seq.Uuid,
    PrevSeqUuid: prev,
  }
}

func TestValidateSnapshotSeqHead(t *testing.T) {
  seq := dummySnapshotSequence("vol", "seq")
  bad_head := &pb.SnapshotSeqHead{}
  head := dummySnapshotSeqHead(seq, "s1", "s2")
  err := ValidateSnapshotSeqHead(head)
  if err != nil { t.Errorf("this should be ok: %v", head) }
  err = ValidateSnapshotSeqHead(bad_head)
  if err == nil { t.Errorf("empty sequence should be ko: %v", bad_head) }
}

func TestValidateSnapshotSequence(t *testing.T) {
  seq := dummySnapshotSequence("vol", "seq")
  bad_seq := &pb.SnapshotSequence{}
  err := ValidateSnapshotSequence(seq)
  if err != nil { t.Errorf("this should be ok: %v", seq) }
  err = ValidateSnapshotSequence(bad_seq)
  if err == nil { t.Errorf("empty sequence should be ko: %v", bad_seq) }
}

