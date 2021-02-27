package local

import (
  "fmt"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
)

type btrfsVolumeManager struct {
  btrfsutil types.Btrfsutil
  linuxutil types.Linuxutil
  conf      types.Config
}

func NewVolumeManager(conf types.Config) (types.VolumeManager, error) {
  var btrfsutil types.Btrfsutil
  var linuxutil types.Linuxutil
  var err error
  linuxutil, err = shim.NewLinuxutil(conf)
  if err != nil || !linuxutil.IsCapSysAdmin() {
    return nil, fmt.Errorf("To manage btrfs volumes you need CAP_SYS_ADMIN")
  }
  btrfsutil, err = shim.NewBtrfsutil(conf)
  mgr := btrfsVolumeManager{ btrfsutil, linuxutil, conf }
  return &mgr, err
}

func (self *btrfsVolumeManager) GetVolume(path string) (*pb.SubVolume, error) {
  return nil, nil
}

func (self *btrfsVolumeManager) GetChangesBetweenSnaps() (*pb.SnapshotChanges, error) {
  return nil, nil
}

func (self *btrfsVolumeManager) GetSnapshotSeqForVolume() (*pb.SnapshotSeq, error) {
  return nil, nil
}

