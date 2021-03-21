package types

import (
  "context"
  pb "btrfs_to_glacier/messages"
)

type SnapshotChangesOrError struct {
  Val *pb.SnapshotChanges
  Err error
}

type VolumeManager interface {
  // `path` must be the root of the volume.
  // `origin_sys` will be set to the host environment.
  GetVolume(path string) (*pb.SubVolume, error)
  // Returns all snapshots whose parent is `subvol`.
  // Returned snaps are soted by creation generation (oldest first).
  // `received_uuid` will only be set if the snapshot was effectibely received.
  GetSnapshotSeqForVolume(subvol *pb.SubVolume) (*pb.SnapshotSeq, error)
  // Returns the changes between 2 snapshots of the same subvolume.
  // Both snaps must come from the same parent and `from` must be from a previous gen than `to`.
  GetChangesBetweenSnaps(ctx context.Context, from *pb.Snapshot, to *pb.Snapshot) (<-chan SnapshotChangesOrError, error)
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

