package mocks

import (
  "context"
  "fmt"
  fpmod "path/filepath"
  "os"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type BackupManager struct {
  SrcVols     map[string]*pb.SubVolume
  PairsByCall [][]types.BackupPair
  CloneSnaps  []*pb.SubVolume
}

func NewBackupManager() *BackupManager {
  mgr := &BackupManager{ SrcVols:make(map[string]*pb.SubVolume), }
  _  = (types.BackupManager)(mgr)
  return mgr
}

func (self *BackupManager) InitFromConfSource(src *pb.Source) {
  for _,ppair := range src.Paths {
    sv := util.DummySubVolume(uuid.NewString())
    sv.MountedPath = ppair.VolPath
    sv.Data = nil
    self.SrcVols[sv.Uuid] = sv
  }
}

func (self *BackupManager) SeqForUuid(vol_uuid string) (*pb.SubVolume, []*pb.SubVolume) {
  var sv *pb.SubVolume
  seq := []*pb.SubVolume{}
  for _,pairs := range self.PairsByCall {
    for _,pair := range pairs {
      if pair.Sv.Uuid != vol_uuid { continue }
      sv = pair.Sv
      seq = append(seq, proto.Clone(pair.Snap).(*pb.SubVolume))
    }
  }
  return proto.Clone(sv).(*pb.SubVolume), seq
}

func (self *BackupManager) BackupAllToCurrentSequences(
    ctx context.Context) ([]types.BackupPair, error) {
  res := make([]types.BackupPair, 0, len(self.SrcVols))
  clone := make([]types.BackupPair, 0, len(self.SrcVols))
  for _,sv := range self.SrcVols {
    snap := util.DummySnapshot(uuid.NewString(), sv.Uuid)
    p := types.BackupPair{
      Sv: proto.Clone(sv).(*pb.SubVolume),
      Snap: snap,
    }
    p2 := types.BackupPair{
      Sv: proto.Clone(p.Sv).(*pb.SubVolume),
      Snap: proto.Clone(p.Snap).(*pb.SubVolume),
    }
    res = append(res, p)
    clone = append(clone, p2)
  }
  self.PairsByCall = append(self.PairsByCall, clone)
  return res, nil
}

func (self *BackupManager) BackupAllToNewSequences(
    ctx context.Context) ([]types.BackupPair, error) {
  return self.BackupAllToCurrentSequences(ctx)
}

func (self *BackupManager) BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, sv *pb.SubVolume, dst_uuid string) (*pb.SubVolume, error) {
  sv, found := self.SrcVols[dst_uuid]
  if !found { return nil, fmt.Errorf("%s not found in SrcVols", dst_uuid) }
  if sv.Uuid == dst_uuid { return nil, fmt.Errorf("expected a clone") }
  snap := util.DummySnapshot(uuid.NewString(), dst_uuid)
  self.CloneSnaps = append(self.CloneSnaps, proto.Clone(snap).(*pb.SubVolume))
  return snap, nil
}

type RestoreManager struct {
  RestoreRoot string
  BackupMgr   *BackupManager
}

func NewRestoreManager(bck *BackupManager) *RestoreManager {
  mgr := &RestoreManager{
    // This path does not exist, it should be replaced in `InitFromConfRestore`.
    RestoreRoot: fpmod.Join(os.TempDir(), uuid.NewString()),
    BackupMgr: bck,
  }
  _  = (types.RestoreManager)(mgr)
  return mgr
}

func (self *RestoreManager) InitFromConfRestore(dst *pb.Restore) {
  self.RestoreRoot = dst.RootRestorePath
}

func (self *RestoreManager) ReadHeadAndSequenceMap(
    ctx context.Context) (types.HeadAndSequenceMap, error) {
  heads := make(types.HeadAndSequenceMap)
  for _,sv := range self.BackupMgr.SrcVols {
    _, snaps := self.BackupMgr.SeqForUuid(sv.Uuid)
    if len(snaps) == 0 { continue } // only return a HeadAndSequence if there are snaps
    seq := &pb.SnapshotSequence{
      Uuid: uuid.NewString(),
      Volume: proto.Clone(sv).(*pb.SubVolume),
    }
    for _,s := range snaps { seq.SnapUuids = append(seq.SnapUuids, s.Uuid) }
    head := util.DummySnapshotSeqHead(seq)
    heads[head.Uuid] = types.HeadAndSequence{ Head:head, Cur:seq, }
  }
  return heads, nil
}

func (self *RestoreManager) RestoreCurrentSequence(
    ctx context.Context, vol_uuid string) ([]types.RestorePair, error) {
  sv, seq := self.BackupMgr.SeqForUuid(vol_uuid)
  if sv == nil { return nil, fmt.Errorf("%s not found in SrcVols", vol_uuid) }
  pairs := make([]types.RestorePair, 0, len(seq))
  last_uuid := ""
  for _,src := range seq {
    dst := util.DummySnapshot(uuid.NewString(), last_uuid)
    dst.MountedPath = fpmod.Join(self.RestoreRoot, dst.Uuid)
    dst.ReceivedUuid = src.Uuid
    dst.Data = nil
    pair := types.RestorePair{ Src:src, Dst:dst, }
    pairs = append(pairs, pair)
    last_uuid = dst.Uuid
  }
  return pairs, nil
}

