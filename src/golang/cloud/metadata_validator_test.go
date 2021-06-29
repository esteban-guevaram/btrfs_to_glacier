package cloud

import (
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
)

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

func dummySnapshotSeqHead(seq *pb.SnapshotSequence, prev ...string) *pb.SnapshotSeqHead {
  return &pb.SnapshotSeqHead{
    Uuid: seq.Volume.Uuid,
    CurSeqUuid: seq.Uuid,
    PrevSeqUuid: prev,
  }
}

func TestValidateSnapshotChunks(t *testing.T) {
  chunks := dummyChunks("sldjfsldk")
  bad_chunks := &pb.SnapshotChunks{}
  dupe_chunks := dummyChunks("skdfhk")
  dupe_chunks.Chunks = append(dupe_chunks.Chunks, dupe_chunks.Chunks[0])

  err := ValidateSnapshotChunks(chunks)
  if err != nil { t.Errorf("this should be ok: %v", chunks) }
  err = ValidateSnapshotChunks(bad_chunks)
  if err == nil { t.Errorf("empty snap chunks should be ko: %v", bad_chunks) }
  err = ValidateSnapshotChunks(dupe_chunks)
  if err == nil { t.Errorf("duplicate chunks should be ko: %v", dupe_chunks) }
}

func TestValidateSystemInfo(t *testing.T) {
  sv := dummySubVolume("vol")
  si := sv.OriginSys
  bad_si := &pb.SystemInfo{}

  err := ValidateSystemInfo(si)
  if err != nil { t.Errorf("this should be ok: %v", si) }
  err = ValidateSystemInfo(bad_si)
  if err == nil { t.Errorf("empty system info should be ko: %v", bad_si) }
}

func TestValidateSubVolume(t *testing.T) {
  sv := dummySubVolume("vol")
  bad_sv := &pb.SubVolume{}
  snap := dummySnapshot("snap", "parent")
  bad_snap := &pb.SubVolume{}

  err := ValidateSubVolume(CheckSvInSeq, sv)
  if err != nil { t.Errorf("this should be ok: %v", sv) }
  err = ValidateSubVolume(CheckSvInSeq, bad_sv)
  if err == nil { t.Errorf("empty subvolume should be ko: %v", bad_sv) }

  err = ValidateSubVolume(CheckSnapNoContent, snap)
  if err != nil { t.Errorf("this should be ok: %v", snap) }
  err = ValidateSubVolume(CheckSnapNoContent, bad_snap)
  if err == nil { t.Errorf("empty snapshot should be ko: %v", bad_snap) }

  snap.Data = dummyChunks("sldjkf")
  err = ValidateSubVolume(CheckSnapWithContent, snap)
  if err != nil { t.Errorf("this should be ok: %v", snap) }
  err = ValidateSubVolume(CheckSnapWithContent, bad_snap)
  if err == nil { t.Errorf("empty snapshot should be ko: %v", bad_snap) }
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

