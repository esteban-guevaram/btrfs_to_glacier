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
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  GetVolume(path string) (*pb.Snapshot, error)
  // Returns all snapshots whose parent is `subvol`.
  // Returned snaps are soted by creation generation (oldest first).
  // `received_uuid` will only be set if the snapshot was effectibely received.
  GetSnapshotSeqForVolume(subvol *pb.SubVolume) (*pb.SnapshotSeq, error)
  // Returns the changes between 2 snapshots of the same subvolume.
  // Both snaps must come from the same parent and `from` must be from a previous gen than `to`.
  GetChangesBetweenSnaps(ctx context.Context, from *pb.Snapshot, to *pb.Snapshot) (<-chan SnapshotChangesOrError, error)
}

type VolumeSource interface {
  VolumeManager
  // Creates a read-only snapshot of `subvol`.
  // The path for the new snapshot will be determined by configuration.
  CreateSnapshot(subvol *pb.SubVolume) (*pb.Snapshot, error)
  // Create a pipe with the data from the delta between `from` and `to` snapshots.
  // `from` can be nil to get the full snapshot content.
  GetSnapshotStream(ctx context.Context, from *pb.Snapshot, to *pb.Snapshot) (PipeReadEnd, error)
}

type VolumeDestination interface {
  VolumeManager
  // Deletes a snapshot. Returns an error if attempting to delete a write snapshot or subvolume.
  DeleteSnapshot(snap *pb.SubVolume) error
  ReceiveSnapshotStream() error
}

