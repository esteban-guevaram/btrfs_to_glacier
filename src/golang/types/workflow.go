package types

import (
  "context"
  pb "btrfs_to_glacier/messages"
)

type SnapshotSeqHead struct {
  Head *pb.SnapshotSeqHead
  Cur  *pb.SnapshotSequence
}
type BackupPair struct {
  Sv   *pb.SubVolume
  Snap *pb.SubVolume
}
// Maps the subvolue uuid to the current snapshot sequence.
type SnapshotSeqHeadMap = map[string]SnapshotSeqHead

// Maintains a small filesystem that can be restored from scratch and validated.
// Ensures that all stored volumes are still compatible and can be restored.
type BackupRestoreCanary interface {}

// Handles volume backup from a particular source.
type BackupManager interface {
  // For all subvolumes in a source performs an **incremental** backup.
  // For each source subvolume `vol_uuid` checks:
  // * If `vol_uuid` has a recent enough snapshot then an incremental backup of it will be stored.
  // * If `vol_uuid` has no recent snapshots, a new one will be created on the fly.
  // If metadata does not contain any data for a given subvolume it will be created on the fly.
  // This operation is a noop for a given subvolume if a recent snapshot has already been backep-up.
  // Returns the pairs of (subvolume, incremental snapshot) in unspecified order.
  // An error indicates at least 1 subvolume could not be backep-up but other may have been.
  // (Given the idempotency of this operation, you can simply try again)
  BackupAllToCurrentSequences(ctx context.Context) ([]BackupPair, error)
  // Like `BackupToCurrentSequence` except that:
  // * A **full** backup of the source subvolumes will be stored.
  // * Every call will always create a new `SnapshotSequence`.
  BackupAllToNewSequences(ctx context.Context) ([]BackupPair, error)
}

// Who is responsible for this ?
// After a successful backup, old volumes will be removed from the source according to the config.

type BackupManagerAdmin interface {
  BackupManager
  // Used to append to a sequence a snapshot which is not related to the original subvolume.
  // Takes a snapshot from `sv` and appends it to the current sequence for SnapshotSeqHead `dst_uuid`.
  // This method expects a SnapshotSequence for `dst_uuid` in the metadata, otherwise it will return an error.
  // A new snapshot will always be created, this method cannot be a noop.
  // This is an advanced operation, the caller is responsible for `sv` to be compatible with the sequence.
  // For example `sv` is a clone from a restore of the original subvolume in `dst_uuid`.
  BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, sv *pb.SubVolume, dst_uuid string) (BackupPair, error)
}

// Handles volume restores to a particular destination.
type RestoreManager interface {
  // Reads all snapshot heads and their current sequence from Metadata.
  ReadSnapshotSeqHeadMap(ctx context.Context) (SnapshotSeqHeadMap, error)
  // Restores all of the snapshots for the most recent sequence corresponding to `vol_uuid`.
  // If some snapshots are already present at the destination, then only the new ones are restored.
  // Returns the list of snapshots (from the source) actually restored to the destination, in the same order as they got restored.
  RestoreCurrentSequence(ctx context.Context, vol_uuid string) ([]*pb.SubVolume, error)
}

