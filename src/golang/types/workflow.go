package types

import (
  "context"
  pb "btrfs_to_glacier/messages"
)

type HeadAndSequence struct {
  Head *pb.SnapshotSeqHead
  Cur  *pb.SnapshotSequence
}
type BackupPair struct {
  Sv   *pb.SubVolume
  // `Snap` should contain information about the chunks stored.
  Snap *pb.SubVolume
}
type RestorePair struct {
  // `Src` should contain information about the chunks stored.
  Src *pb.SubVolume
  Dst *pb.SubVolume
}
// Maps the subvolue uuid to the current snapshot sequence.
type HeadAndSequenceMap = map[string]HeadAndSequence
const KCanaryDelDir = "deleted"
const KCanaryNewDir = "new"
const KCanaryUuidFile = "uuids"

// Maintains a small filesystem that can be restored from scratch and validated.
// Ensures that all stored volumes are still compatible and can be restored.
type BackupRestoreCanary interface {
  // Creates the canary filesystem.
  // Creates dummy subvolume if there is no data in the Metadata under test.
  // Calling this method twice is a noop.
  Setup(ctx context.Context) error
  // Destroys the canary filesystem.
  // Calling this method twice or before `Setup()` is a noop.
  TearDown(ctx context.Context) error
  // Restores the whole snapshot sequence into the canary filesystem.
  // Validates the most recent snapshot according to its predecessors.
  // Calling this method twice is an error since validation will not see the whole history.
  // Returns the restored pairs from storage that were validated.
  RestoreChainAndValidate(ctx context.Context) ([]RestorePair, error)
  // Modifies the restored subvolume (by making a clone) and adds another snapshot to the sequence.
  // Backups the new snapshot into the current sequence.
  // Must be called after the sequence has been restored. Can be called several times, adds a new snapshot each time.
  // Returns the snapshot that was backed up and its corresponding entry in metadata.
  AppendSnapshotToValidationChain(ctx context.Context) (BackupPair, error)
}

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
  // Note that several calls to this method may NOT create each a snapshot in metadata or storage.
  // Rather the created sequences may point to an already stored recent snapshot.
  BackupAllToNewSequences(ctx context.Context) ([]BackupPair, error)
}

// Who is responsible for this ?
// After a successful backup, old volumes will be removed from the source according to the config.

type BackupManagerAdmin interface {
  BackupManager
  // Used to append to a sequence a snapshot which is not related to the original subvolume.
  // Takes a snapshot from `sv` and appends it to the current sequence for SnapshotSeqHead `dst_uuid`.
  // This method expects a SnapshotSequence for `dst_uuid` in the metadata, otherwise it will return an error.
  // Returns the snapshot created for `sv`, however it will look like it is a snaphost of `dst_uuid`.
  // NON idempotent, the unrelated volume cannot have any pre-existing child snapshots.
  //
  // This is an advanced operation, the caller is responsible for `sv` to be compatible with the sequence.
  // For example `sv` is a clone from a restore of the original subvolume in `dst_uuid`.
  BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, sv *pb.SubVolume, dst_uuid string) (*pb.SubVolume, error)
}

// Handles volume restores to a particular destination.
type RestoreManager interface {
  // Reads all snapshot heads and their current sequence from Metadata.
  ReadHeadAndSequenceMap(ctx context.Context) (HeadAndSequenceMap, error)
  // Restores all of the snapshots for the most recent sequence corresponding to `vol_uuid`.
  // If some snapshots are already present at the destination, then only the new ones are restored.
  // If all snapshots have been restored this is a noop.
  // Returns both the snapshot from Metadata with its corresponding snapshot in the restored filesystem.
  // The return list follows the order in which each snapshot got restored.
  RestoreCurrentSequence(ctx context.Context, vol_uuid string) ([]RestorePair, error)
}

