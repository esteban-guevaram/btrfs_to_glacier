package types

import (
  pb "btrfs_to_glacier/messages"
)

type VolumeManager interface {
  // `path` must be the root of the volume.
  // `origin_sys` will be set to the host environment.
  GetVolume(path string) (*pb.SubVolume, error)
  GetChangesBetweenSnaps() (*pb.SnapshotChanges, error)
  // Returns all snapshots whose parent is `subvol`.
  // Returned snaps are soted by creation generation (oldest first).
  // `received_uuid` will only be set if the snapshot was effectibely received.
  GetSnapshotSeqForVolume(subvol *pb.SubVolume) (*pb.SnapshotSeq, error)
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

