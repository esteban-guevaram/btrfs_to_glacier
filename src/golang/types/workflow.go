package types

import (
  "context"
  pb "btrfs_to_glacier/messages"
)

// Maintins a small filesystem that can be restored from scratch and validated.
// Ensures that all stored volumes are still compatible and can be restored.
type BackupRestoreCanary interface {}

// Handles volume backup from a particular source.
type BackupManager interface {
  BackupToCurrentSequence(ctx context.Context, vol_uuid string) (*pb.SubVolume, error)
}

type BackupManagerAdmin interface {
  BackupManager
  // Used to append to a sequence a snapshot which is not related to the original subvolume.
  // Takes a snapshot from `src_uuid` and appends it to the current sequence for `dst_uuid`.
  // This is an advanced operation the caller is responsible for `src_uuid` to be compatible.
  // For example `src_uuid` is a clone from a restore chain from `dst_uuid`.
  BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, src_uuid string, dst_uuid string) (*pb.SubVolume, error)
}

// Handles volume restores to a particular destination.
type RestoreManager interface {
  // Restores all of the snapshots for the most recent sequence corresponding to `vol_uuid`.
  // If some snapshots are already present at the destination, then only the new ones are restored.
  // Returns the list of snapshots (from the source) actually restored to the destination, in the same order as they got restored.
  RestoreCurrentSequence(ctx context.Context, vol_uuid string) ([]*pb.SubVolume, error)
}

