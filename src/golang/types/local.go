package types

import (
  pb "btrfs_to_glacier/messages"
)

type VolumeManager interface {
  GetVolume(path string) (*pb.Volume, error)
  GetChangesBetweenSnaps() (*pb.SnapshotChanges, error)
  GetSnapshotSeqForVolume() (*pb.SnapshotSeq, error)
}

// Implementation questions
// * Can I use btrfs receive dump mode for something ? 
type VolumeSource interface {
  VolumeManager
  ReceiveSnapshotStream() error
  GetSnapshotStream() error
}

type VolumeDestination interface {
  VolumeManager
  CreateVolume() error
  DeleteVolume() error
}

