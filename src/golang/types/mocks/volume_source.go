package mocks

import (
  "context"
  "fmt"
  "io"
  fpmod "path/filepath"
  "os"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type BtrfsPathJuggler struct {
  Err error
  PathToFs map[string]*types.Filesystem
  PathToMnt map[string]*types.MountEntry
  UuidToFs map[string]*types.Filesystem
  UuidToMnt map[string]*types.MountEntry
  CheckResult []*types.Filesystem
}

func (self *BtrfsPathJuggler) FindFsAndTighterMountOwningPath(
    path string) (*types.Filesystem, *types.MountEntry, uint64, error) {
  fs, found := self.PathToFs[path]
  if !found { return nil, nil, 0, fmt.Errorf("FindFsAndTighterMountOwningPath not found") }
  mnt, found := self.PathToMnt[path]
  if !found { return nil, nil, 0, fmt.Errorf("FindFsAndTighterMountOwningPath not found") }
  return fs, mnt, mnt.BtrfsVolId, self.Err
}
func (self *BtrfsPathJuggler) FindTighterMountForSubVolume(
    fs_list []*types.Filesystem, sv *pb.SubVolume) (*types.Filesystem, *types.MountEntry, string, error) {
  mnt, found := self.UuidToMnt[sv.Uuid]
  if !found { return nil, nil, "", fmt.Errorf("FindTighterMountForSubVolume mnt not found") }
  fs, found := self.UuidToFs[sv.Uuid]
  if !found { return nil, nil, "", fmt.Errorf("FindTighterMountForSubVolume fs not found") }
  path := mnt.MountedPath
  if mnt.BtrfsVolId != sv.VolId { path = fpmod.Join(mnt.MountedPath, sv.TreePath) }
  return fs, mnt, path, self.Err
}
func (self *BtrfsPathJuggler) CheckSourcesAndReturnCorrespondingFs(
    sources []*pb.Source) ([]*types.Filesystem, error) {
  return self.CheckResult, self.Err
}
func (self *BtrfsPathJuggler) LoadFilesystem(fs *types.Filesystem) {
  if fs == nil { util.Fatalf("fs == nil") }
  if self.PathToFs == nil { self.PathToFs = make(map[string]*types.Filesystem) }
  if self.PathToMnt == nil { self.PathToMnt = make(map[string]*types.MountEntry) }
  for _,mnt := range fs.Mounts {
    self.PathToFs[mnt.MountedPath] = fs
    self.PathToMnt[mnt.MountedPath] = mnt
  }
  self.CheckResult = append(self.CheckResult, fs)
}

func (self *BtrfsPathJuggler) LoadSubVolume(
    fs *types.Filesystem, mnt *types.MountEntry, sv_list ...*pb.SubVolume) {
  if self.UuidToFs == nil { self.UuidToFs = make(map[string]*types.Filesystem) }
  if self.UuidToMnt == nil { self.UuidToMnt = make(map[string]*types.MountEntry) }
  for _,sv := range sv_list {
    if sv == nil || mnt == nil { util.Fatalf("sv == nil || mnt == nil") }
    self.UuidToFs[sv.Uuid] = fs
    self.UuidToMnt[sv.Uuid] = mnt
  }
}

type VolumeManager struct {
  SnapRoot string
  Err     error
  Vols    map[string]*pb.SubVolume
  Snaps   map[string][]*pb.SubVolume
  Changes map[[2]string]*pb.SnapshotChanges
}

func NewVolumeManager() *VolumeManager {
  return &VolumeManager{
    SnapRoot: fpmod.Join(os.TempDir(), "snap_root"),
    Err: nil,
    Vols: make(map[string]*pb.SubVolume),
    Snaps: make(map[string][]*pb.SubVolume),
    Changes: make(map[[2]string]*pb.SnapshotChanges),
  }
}

func CloneSnaps(snaps []*pb.SubVolume) []*pb.SubVolume {
  clone_snaps := []*pb.SubVolume{}
  for _,snap := range snaps {
    clone := proto.Clone(snap).(*pb.SubVolume)
    clone_snaps = append(clone_snaps, clone)
  }
  return clone_snaps
}

func (self *VolumeManager) ObjCounts() []int {
  all_snaps := 0
  for _,seq := range self.Snaps { all_snaps += len(seq) }
  return []int{ len(self.Vols), len(self.Snaps), all_snaps, }
}

func (self *VolumeManager) ClearSnaps() {
  self.Snaps = make(map[string][]*pb.SubVolume)
}

func (self *VolumeManager) AllVols() []*pb.SubVolume {
  all := []*pb.SubVolume{}
  for _,sv := range self.Vols { all = append(all, sv) }
  return all
}

func (self *VolumeManager) GetVolume(path string) (*pb.SubVolume, error) {
  sv, found := self.Vols[path]
  if !found { return nil, fmt.Errorf("No sv for '%s'", path) }
  return sv, self.Err
}
func (self *VolumeManager) FindVolume(
    fs_path string, matcher func(*pb.SubVolume) bool) (*pb.SubVolume, error) {
  for _,sv := range self.Vols {
    clone := proto.Clone(sv).(*pb.SubVolume)
    if matcher(sv) { return clone, self.Err }
  }
  return nil, self.Err
}
func (self *VolumeManager) GetSnapshotSeqForVolume(subvol *pb.SubVolume) ([]*pb.SubVolume, error) {
  for vol_uuid,snaps := range self.Snaps {
    if vol_uuid != subvol.Uuid { continue }
    return CloneSnaps(snaps), self.Err
  }
  return nil, self.Err
}
func (self *VolumeManager) GetChangesBetweenSnaps(
    ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (*pb.SnapshotChanges, error) {
  key := [2]string{ from.Uuid, to.Uuid }
  for k,changes := range self.Changes {
    if k != key { continue }
    clone := proto.Clone(changes).(*pb.SnapshotChanges)
    return clone, self.Err
  }
  return nil, fmt.Errorf("No changes for %v", key)
}
func (self *VolumeManager) CreateSnapshot(subvol *pb.SubVolume) (*pb.SubVolume, error) {
  snap := util.DummySnapshot(uuid.NewString(), subvol.Uuid)
  snap.MountedPath = fpmod.Join(self.SnapRoot, snap.Uuid)
  snap.CreatedTs = uint64(time.Now().Unix())
  snap.Data = nil
  snaps := self.Snaps[subvol.Uuid]
  snaps = append(snaps, snap)
  self.Snaps[subvol.Uuid] = snaps
  clone := proto.Clone(snap).(*pb.SubVolume)
  return clone, self.Err
}
func (self *VolumeManager) GetSnapshotStream(
    ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (types.ReadEndIf, error) {
  if from != nil && from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("uuid: '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if _,found := self.Snaps[to.ParentUuid]; !found {
    return nil, fmt.Errorf("self.Snaps[%s] not found", to.ParentUuid)
  }
  pipe := NewPreloadedPipe(util.GenerateRandomTextData(32))
  return pipe.ReadEnd(), self.Err
}
func (self *VolumeManager) ReceiveSendStream(
    ctx context.Context, root_path string, rec_uuid string, read_pipe types.ReadEndIf) (*pb.SubVolume, error) {
  done := make(chan error)
  go func() {
    defer close(done)
    defer read_pipe.Close()
    _, err := io.Copy(io.Discard, read_pipe)
    done <- err
  }()
  util.WaitForNoErrorOrDie(ctx, done)
  // We always consider this is the first restore in a chain (aka has no parent)
  snap := util.DummySnapshot(uuid.NewString(), "")
  snap.ReceivedUuid = rec_uuid
  self.Snaps[snap.Uuid] = []*pb.SubVolume{snap,}
  clone := proto.Clone(snap).(*pb.SubVolume)
  return clone, util.Coalesce(read_pipe.GetErr(), self.Err)
}
func (self *VolumeManager) DeleteSnapshot(snap *pb.SubVolume) error {
  if _,found := self.Snaps[snap.ParentUuid]; !found {
    return fmt.Errorf("delete unexisting snap: %s", util.AsJson(snap))
  }
  delete(self.Snaps, snap.Uuid)
  return self.Err
}
func (self *VolumeManager) TrimOldSnapshots(
    src_subvol *pb.SubVolume, dry_run bool) ([]*pb.SubVolume, error) {
  if _,found := self.Snaps[src_subvol.Uuid]; !found {
    return nil, self.Err
  }
  self.Snaps[src_subvol.Uuid] = []*pb.SubVolume{}
  return nil, self.Err
}

