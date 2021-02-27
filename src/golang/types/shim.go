package types

import (
  pb "btrfs_to_glacier/messages"
)

type Btrfsutil interface {
  SubvolumeInfo(path string, subvol_id int) (*pb.SubVolume, error)
}

