package types

import (
  pb "btrfs_to_glacier/messages"
)

type Linuxutil interface {
  IsCapSysAdmin() bool
  LinuxKernelVersion() (uint32, uint32)
  BtrfsProgsVersion() (uint32, uint32)
  ProjectVersion() string
}

type Btrfsutil interface {
  SubvolumeInfo(path string, subvol_id int) (*pb.SubVolume, error)
}

