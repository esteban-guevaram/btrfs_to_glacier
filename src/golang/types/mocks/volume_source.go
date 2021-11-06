package mocks

import (
  "fmt"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
)

type BtrfsPathJuggler struct {
  Err error
  PathToFs map[string]*types.Filesystem
  PathToMnt map[string]*types.MountEntry
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
    fs *types.Filesystem, sv *pb.SubVolume) (*types.MountEntry, error) {
  mnt, found := self.UuidToMnt[sv.Uuid]
  if !found { return nil, fmt.Errorf("FindTighterMountForSubVolume not found") }
  return mnt, self.Err
}
func (self *BtrfsPathJuggler) CheckSourcesAndReturnCorrespondingFs(
    sources []*pb.Source) ([]*types.Filesystem, error) {
  return self.CheckResult, self.Err
}

