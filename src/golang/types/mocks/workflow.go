package mocks

import (
  "context"
  "fmt"
  "io/fs"
  fpmod "path/filepath"
  "os"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type BackupManager struct {
  ErrBase
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

func (self *BackupManager) OrigForUuid(vol_uuid string) (*pb.SubVolume, error) {
  sv, found := self.SrcVols[vol_uuid]
  if !found { return nil, fmt.Errorf("%s not found in SrcVols", vol_uuid) }
  return sv, nil
}

func (self *BackupManager) SeqForUuid(vol_uuid string) []*pb.SubVolume {
  snap_seq := []*pb.SubVolume{}
  for _,pairs := range self.PairsByCall {
    for _,pair := range pairs {
      if pair.Sv.Uuid != vol_uuid { continue }
      snap_seq = append(snap_seq, proto.Clone(pair.Snap).(*pb.SubVolume))
    }
  }
  util.Debugf("SeqForUuid: %s -> %d", vol_uuid, len(snap_seq))
  return snap_seq
}

// We return twice the exact same value for the caller and mock call history.
func (self *BackupManager) BackupSingleHelper(
    sv *pb.SubVolume) (types.BackupPair, types.BackupPair) {
  snap := util.DummySnapshot(uuid.NewString(), sv.Uuid)
  snap.Data = util.DummyChunks(uuid.NewString(), uuid.NewString())
  ret_pair := types.BackupPair{
    Sv: proto.Clone(sv).(*pb.SubVolume),
    Snap: snap,
  }
  pair_clone := types.BackupPair{
    Sv: proto.Clone(ret_pair.Sv).(*pb.SubVolume),
    Snap: proto.Clone(ret_pair.Snap).(*pb.SubVolume),
  }
  return ret_pair, pair_clone
}

func (self *BackupManager) BackupAllToCurrentSequences(
    ctx context.Context) ([]types.BackupPair, error) {
  res := make([]types.BackupPair, 0, len(self.SrcVols))
  clone := make([]types.BackupPair, 0, len(self.SrcVols))
  for _,sv := range self.SrcVols {
    p, p2 := self.BackupSingleHelper(sv)
    res = append(res, p)
    clone = append(clone, p2)
  }
  self.PairsByCall = append(self.PairsByCall, clone)
  return res, self.ErrInject(self.BackupAllToCurrentSequences)
}

func (self *BackupManager) BackupAllToNewSequences(
    ctx context.Context) ([]types.BackupPair, error) {
  util.Fatalf("This mock is too simple?")
  pairs,_ := self.BackupAllToCurrentSequences(ctx)
  return pairs, self.ErrInject(self.BackupAllToNewSequences)
}

func (self *BackupManager) BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, sv *pb.SubVolume, dst_uuid string) (*pb.SubVolume, error) {
  sv, err := self.OrigForUuid(dst_uuid)
  if err != nil { return nil, err }
  if sv.Uuid == dst_uuid { return nil, fmt.Errorf("expected a clone") }
  snap := util.DummySnapshot(uuid.NewString(), dst_uuid)
  self.CloneSnaps = append(self.CloneSnaps, proto.Clone(snap).(*pb.SubVolume))
  return snap, self.ErrInject(self.BackupToCurrentSequenceUnrelatedVol)
}

type PopulateRestoreF = func(orig *pb.SubVolume, pairs []types.RestorePair) error
type RestoreManager struct {
  RestoreRoot      string
  BackupMgr        *BackupManager
  PopulateRestore  PopulateRestoreF
}

func DelDir(sv *pb.SubVolume) string {
  return fpmod.Join(sv.MountedPath, types.KCanaryDelDir)
}
func NewDir(sv *pb.SubVolume) string {
  return fpmod.Join(sv.MountedPath, types.KCanaryNewDir)
}
func UuidFile(sv *pb.SubVolume) string {
  return fpmod.Join(sv.MountedPath, types.KCanaryUuidFile)
}

func PopulateRestoreCorrect(orig *pb.SubVolume, pairs []types.RestorePair) error {
  dst := pairs[len(pairs) - 1].Dst
  if err := os.Mkdir(DelDir(dst), fs.ModePerm); err != nil { return err }
  if err := os.Mkdir(NewDir(dst), fs.ModePerm); err != nil { return err }
  f_uuid, err := os.OpenFile(UuidFile(dst),
                             os.O_APPEND|os.O_CREATE|os.O_WRONLY,
                             0666)
  if err != nil { return err }
  defer f_uuid.Close()
  if len(pairs) == 1 { return nil }

  for i:=0; i<len(pairs)-1; i+=1 {
    sv := pairs[i].Src

    _, err := f_uuid.WriteString(fmt.Sprintf("%s\n", sv.Uuid))
    if err != nil { return err }

    new_file := fpmod.Join(NewDir(dst), sv.Uuid)
    new_hash := []byte(util.HashFromSv(sv, ""))
    err = os.WriteFile(new_file, new_hash, 0666)
    if err != nil { return err }
  }

  prev_hash, del_hash := "", ""
  for i:=0; i<len(pairs)-1; i+=1 {
    prev_hash = del_hash
    del_hash = util.HashFromSv(pairs[i].Src, prev_hash)
  }
  del_file := fpmod.Join(DelDir(dst), pairs[len(pairs) - 1].Src.Uuid)
  err = os.WriteFile(del_file, []byte(del_hash), 0666)
  return err
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
  util.Debugf("RestoreRoot: '%s'", dst.RootRestorePath)
  self.RestoreRoot = dst.RootRestorePath
}

func (self *RestoreManager) ReadHeadAndSequenceMap(
    ctx context.Context) (types.HeadAndSequenceMap, error) {
  heads := make(types.HeadAndSequenceMap)
  for _,sv := range self.BackupMgr.SrcVols {
    snaps := self.BackupMgr.SeqForUuid(sv.Uuid)
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
  seq := self.BackupMgr.SeqForUuid(vol_uuid)
  orig, err := self.BackupMgr.OrigForUuid(vol_uuid)
  if err != nil { return nil, err }
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

    if err := os.Mkdir(dst.MountedPath, fs.ModePerm); err != nil { return nil, err }
    if self.PopulateRestore != nil {
      if err := self.PopulateRestore(orig, pairs); err != nil { return nil, err }
    }
    //util.PbDebugf("RestoreCurrentSequence:\n%s\n%s", pair.Src, pair.Dst)
  }
  return pairs, nil
}

