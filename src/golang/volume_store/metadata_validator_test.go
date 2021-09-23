package volume_store

import (
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
)

func TestValidateSnapshotChunks(t *testing.T) {
  chunks := util.DummyChunks("sldjfsldk")
  bad_chunks := &pb.SnapshotChunks{}
  dupe_chunks := util.DummyChunks("skdfhk")
  dupe_chunks.Chunks = append(dupe_chunks.Chunks, dupe_chunks.Chunks[0])

  err := ValidateSnapshotChunks(CheckChunkFromStart, chunks)
  if err != nil { t.Errorf("this should be ok: %v", chunks) }
  err = ValidateSnapshotChunks(CheckChunkFromStart, bad_chunks)
  if err == nil { t.Errorf("empty snap chunks should be ko: %v", bad_chunks) }
  err = ValidateSnapshotChunks(CheckChunkFromStart, dupe_chunks)
  if err == nil { t.Errorf("duplicate chunks should be ko: %v", dupe_chunks) }
}

func TestValidateSystemInfo(t *testing.T) {
  sv := util.DummySubVolume("vol")
  si := sv.OriginSys
  bad_si := &pb.SystemInfo{}

  err := ValidateSystemInfo(si)
  if err != nil { t.Errorf("this should be ok: %v", si) }
  err = ValidateSystemInfo(bad_si)
  if err == nil { t.Errorf("empty system info should be ko: %v", bad_si) }
}

func TestSubVolumeDataLen(t *testing.T) {
  sv := util.DummySubVolume("vol")
  sv.Data = util.DummyChunks("first")
  first_len := SubVolumeDataLen(sv)
  util.EqualsOrFailTest(t, "Bad subvol len", first_len, sv.Data.Chunks[0].Size)

  second := &pb.SnapshotChunks_Chunk {
    Uuid: "second",
    Start: 0,
    Size: 3,
  }
  sv.Data.Chunks = append(sv.Data.Chunks, second)
  data_len := SubVolumeDataLen(sv)
  util.EqualsOrFailTest(t, "Bad subvol len2", data_len, first_len + second.Size)
}

func TestValidateSubVolume(t *testing.T) {
  sv := util.DummySubVolume("vol")
  bad_sv := &pb.SubVolume{}
  snap := util.DummySnapshot("snap", "parent")
  bad_snap := &pb.SubVolume{}

  err := ValidateSubVolume(CheckSvInSeq, sv)
  if err != nil { t.Errorf("this should be ok: %v", sv) }
  err = ValidateSubVolume(CheckSvInSeq, bad_sv)
  if err == nil { t.Errorf("empty subvolume should be ko: %v", bad_sv) }

  err = ValidateSubVolume(CheckSnapNoContent, snap)
  if err != nil { t.Errorf("this should be ok: %v", snap) }
  err = ValidateSubVolume(CheckSnapNoContent, bad_snap)
  if err == nil { t.Errorf("empty snapshot should be ko: %v", bad_snap) }

  snap.Data = util.DummyChunks("sldjkf")
  err = ValidateSubVolume(CheckSnapWithContent, snap)
  if err != nil { t.Errorf("this should be ok: %v", snap) }
  err = ValidateSubVolume(CheckSnapWithContent, bad_snap)
  if err == nil { t.Errorf("empty snapshot should be ko: %v", bad_snap) }
}

func TestValidateSnapshotSeqHead(t *testing.T) {
  seq := util.DummySnapshotSequence("vol", "seq")
  bad_head := &pb.SnapshotSeqHead{}
  head := util.DummySnapshotSeqHead(seq, "s1", "s2")
  err := ValidateSnapshotSeqHead(head)
  if err != nil { t.Errorf("this should be ok: %v", head) }
  err = ValidateSnapshotSeqHead(bad_head)
  if err == nil { t.Errorf("empty sequence should be ko: %v", bad_head) }
}

func TestValidateSnapshotSequence(t *testing.T) {
  seq := util.DummySnapshotSequence("vol", "seq")
  bad_seq := &pb.SnapshotSequence{}
  err := ValidateSnapshotSequence(seq)
  if err != nil { t.Errorf("this should be ok: %v", seq) }
  err = ValidateSnapshotSequence(bad_seq)
  if err == nil { t.Errorf("empty sequence should be ko: %v", bad_seq) }
}

