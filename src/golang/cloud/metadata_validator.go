package cloud

import (
  "errors"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  "google.golang.org/protobuf/proto"
)

type CheckType int
const (
  CheckSvInSeq CheckType = iota
  CheckSnapInSeq CheckType = iota
  CheckSnapWithContent CheckType = iota
)

func PbErrorf(format string, pbs ...proto.Message) error {
  return errors.New(util.PbPrintf(format, pbs...))
}

func ValidateSnapshotChunks(chunks *pb.SnapshotChunks) error {
  if chunks == nil { return errors.New("SnapshotChunks == nil") }
  if len(chunks.Chunks) < 1 { return PbErrorf("No chunks: %v", chunks) }
  if chunks.KeyFingerprint == "" { return PbErrorf("Chunks no key fingerprint: %v", chunks) }

  var start_offset *uint64
  for _, chunk := range chunks.Chunks {
    if chunk.Uuid == "" { return PbErrorf("No chunk uuid: %v", chunk) }
    if chunk.Size == 0 { return PbErrorf("No chunk size: %v", chunk) }
    if start_offset == nil && chunk.Start != 0 { return PbErrorf("Bad first chunk: %v", chunk) }
    if start_offset != nil && chunk.Start <= *start_offset { return PbErrorf("Bad chunk order: %v", chunk) }
    start_offset = &chunk.Start
  }
  return nil
}

func ValidateSystemInfo(si *pb.SystemInfo) error {
  if si == nil { return errors.New("SystemInfo == nil") }
  if si.KernMajor == 0 { return PbErrorf("SystemInfo no kernel version: %v", si) }
  if si.BtrfsUsrMajor == 0 { return PbErrorf("SystemInfo no btrfs user lib version: %v", si) }
  if si.ToolGitCommit == "" { return PbErrorf("SystemInfo no git hash: %v", si) }
  return nil
}

func ValidateSubVolume(check CheckType, sv *pb.SubVolume) error {
  if sv == nil { return errors.New("SubVolume == nil") }
  if sv.Uuid == "" { return PbErrorf("Volume no uuid: %v", sv) }
  switch check {
    case CheckSvInSeq:
      if sv.CreatedTs == 0 { return PbErrorf("Volume no creation timestamp: %v", sv) }
      if sv.MountedPath == "" { return PbErrorf("Volume no mount path: %v", sv) }
      return ValidateSystemInfo(sv.OriginSys)
    case CheckSnapInSeq:
      if sv.ParentUuid == "" { return PbErrorf("Snapshot no parent uuid: %v", sv) }
      return nil
    case CheckSnapWithContent:
      if sv.ParentUuid == "" { return PbErrorf("Snapshot no parent uuid: %v", sv) }
      if sv.GenAtCreation == 0 { return PbErrorf("Snapshot no creation generation: %v", sv) }
      if sv.CreatedTs == 0 { return PbErrorf("Snapshot no creation timestamp: %v", sv) }
      if sv.ReadOnly == false { return PbErrorf("Snapshot cannot be writable: %v", sv) }
      err := ValidateSnapshotChunks(sv.Chunks)
      if err != nil { return err }
      return ValidateSystemInfo(sv.OriginSys)
    default:
      return nil
  }
}

// * Head's volume is the same as the sequence.
// * all snapshots are ordered by gen and belong to same parent.
// * volume backed up is correctly formed.
// * not addding an old sequence as the current one.
func ValidateSnapshotSequenceBeforeAddingToHead(head *pb.SnapshotSeqHead, seq *pb.SnapshotSequence) error {
  if head == nil { return errors.New("SnapshotSeqHead == nil") }
  if seq == nil { return errors.New("SnapshotSequence == nil") }
  err := ValidateSubVolume(CheckSvInSeq, seq.Volume)
  if err != nil { return err }
  if head.Uuid != seq.Volume.Uuid { return PbErrorf("Mismatched volume uuid: %v, %v", head, seq) }
  if len(seq.Snaps) < 1 { return PbErrorf("Sequence has no snapshots: %v", seq) }

  var last_gen uint64 = 0
  for _, snap := range seq.Snaps {
    err := ValidateSubVolume(CheckSnapInSeq, snap)
    if err != nil { return err }
    if snap.ParentUuid != head.Uuid { return PbErrorf("Snapshot bad parent: %v", snap) }
    if last_gen > snap.GenAtCreation { return PbErrorf("Snapshots not ordered by gen: %v", seq) }
    last_gen = snap.GenAtCreation
  }
  for _, seq_uuid := range head.PrevSeqUuid {
    if seq.Uuid == seq_uuid { return PbErrorf("Sequence to add is old: %v, %v", seq, head) }
  }
  return nil
}

