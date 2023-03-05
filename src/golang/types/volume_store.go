package types

import "context"
import "errors"
import pb "btrfs_to_glacier/messages"

var ErrNotFound = errors.New("key_not_found_in_metadata")
var ErrChunkFound = errors.New("chunk_not_found_in_content")

type RestoreStatus int
const (
  Unknown  RestoreStatus = iota
  Pending  RestoreStatus = iota
  Restored RestoreStatus = iota
)

type DeletedItems struct {
  Snaps  []*pb.SubVolume
  Seqs   []*pb.SnapshotSequence
  Chunks []*pb.SnapshotChunks_Chunk
}

type ObjRestoreOrErr struct {
  Stx RestoreStatus
  Err error
}
type RestoreResult = map[string]ObjRestoreOrErr

// Usage example:
// for it.Next(ctx, val) { ... }
// if it.Err() != nil { ... }
type SnapshotSeqHeadIterator interface {
  Next(context.Context, *pb.SnapshotSeqHead) bool
  Err() error
}
type SnapshotSequenceIterator interface {
  Next(context.Context, *pb.SnapshotSequence) bool
  Err() error
}
type SnapshotIterator interface {
  Next(context.Context, *pb.SubVolume) bool
  Err() error
}
type SnapshotChunksIterator interface {
  Next(context.Context, *pb.SnapshotChunks_Chunk) bool
  Err() error
}

type Metadata interface {
  // Sets `new_seq` as the snaps sequence that will be appended when backing up the corresponding volume.
  // If there is not already a sequence head, a new one will be created.
  // Otherwise updates the current head and archives the previously current.
  // If `new_seq` is already the current sequence, this is a noop.
  // This should be called **after** the snapshot sequence has been persisted.
  // Returns the new SnapshotSeqHead just persisted.
  RecordSnapshotSeqHead(ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error)

  // Adds `snap` to `seq` and persists `seq`.
  // Returns the new state of the snapshot sequence.
  // This should be called **after** the snapshot has been persisted.
  // If `snap` is already the last snapshot in the sequence this is a noop.
  AppendSnapshotToSeq(ctx context.Context, seq *pb.SnapshotSequence, snap *pb.SubVolume) (*pb.SnapshotSequence, error)

  // Adds all of `data` chunks into the subvolume `snap` and persists it.
  // If there are aready chunks in `snap` then `data` must match the previous fingerprint.
  // Returns the new state of the subvolume metadata.
  // If `data` chunks are a subset of `snap` chunks then this is a noop.
  AppendChunkToSnapshot(ctx context.Context, snap *pb.SubVolume, data *pb.SnapshotChunks) (*pb.SubVolume, error)

  // Reads sequence head for subvolume with `uuid`.
  // If there is no head, returns `ErrNotFound`.
  ReadSnapshotSeqHead(ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error)

  // Reads snapshot sequence with key `uuid`.
  // If there is no sequence, returns `ErrNotFound`.
  ReadSnapshotSeq(ctx context.Context, uuid string) (*pb.SnapshotSequence, error)

  // Reads subvolume with `uuid`.
  // If there is no subvolume, returns `ErrNotFound`.
  ReadSnapshot(ctx context.Context, uuid string) (*pb.SubVolume, error)

  // Returns all heads in no particular order.
  ListAllSnapshotSeqHeads(ctx context.Context) (SnapshotSeqHeadIterator, error)

  // Returns all sequences in no particular order.
  ListAllSnapshotSeqs(ctx context.Context) (SnapshotSequenceIterator, error)

  // Returns all snapshots in no particular order.
  ListAllSnapshots(ctx context.Context) (SnapshotIterator, error)

  // If this method returns successfully, all of the writes to the metadata done until that point will be persisted.
  // That version of the metadata should be protected from later modifications to prevent corruption.
  // Returns an identifier for the metadata version persisted.
  PersistCurrentMetadataState(ctx context.Context) (string, error)
}

// Separate from `Metadata` since it contains dangerous operations that should only be invoked by admins.
type AdminMetadata interface {
  Metadata

  // Creates the infrastructure (depend on implementation) that will contain the metadata.
  // Calling this method twice is a noop.
  SetupMetadata(ctx context.Context) error

  // Performs the cleanups (depend on implementation) after the backups.
  // `PersistCurrentMetadataState` should be called BEFORE if something was written to the Metadata.
  // Calling this method twice is a noop.
  // Calling this method before setup is an error.
  TearDownMetadata(ctx context.Context) error

  // Deletes all items corresponding to the uuids.
  // Uuids not existing will be ignored.
  // Even if an error is returned, some items may have been deleted.
  DeleteMetadataUuids(ctx context.Context, seq_uuids []string, snap_uuids []string) error

  // Replaces a head with a different one.
  // Returns the old head that got replaced.
  // If there was no old head, returns `ErrNotFound` and no item will be written.
  ReplaceSnapshotSeqHead(ctx context.Context, head *pb.SnapshotSeqHead) (*pb.SnapshotSeqHead, error)
}

type BackupContent interface {
  // Reads `read_pipe` and uploads its content in equally sized chunks.
  // If `offset` > 0 then the first part of the stream is dropped and the rest will be uploaded.
  // Data may be filtered by a codec depending on the implementation.
  // Chunk length is determined by configuration.
  // Returns the ids of all chunks uploaded. If some error prevented all pipe content from being uploaded,
  // then both a non nil error and the chunks that got uploaded willb e returned.
  // Takes ownership of `read_pipe` and will close it once done.
  WriteStream(ctx context.Context, offset uint64, read_pipe ReadEndIf) (*pb.SnapshotChunks, error)

  // Request all objects identified by `uuids` to be restored so they can be downloaded.
  // Restoration can take several hours, this method will return sooner, after all object restore requests
  // have been sent.
  // Restoring an already restored object is a noop (or it can extend the restored lifetime).
  // Restoring an object which is not archived is a noop.
  // Restoring an object which does not exist should return `ErrChunkFound`.
  // Clients can use this method to poll and get the status of their pending restores.
  // Returns a result per object, for some the restore may have failed.
  QueueRestoreObjects(ctx context.Context, uuids []string) RestoreResult

  // Reads all `chunks` in order and outputs them to a stream.
  // Data may be filtered by a codec depending on the implementation.
  // A permanent error while reading a chunk will close the stream.
  ReadChunksIntoStream(ctx context.Context, chunks *pb.SnapshotChunks) (ReadEndIf, error)

  // Returns all chunks in no particular order.
  // Not all chunk fields may be filled.
  ListAllChunks(ctx context.Context) (SnapshotChunksIterator, error)
}

// Separate from `BackupContent` since it contains dangerous operations that should only be invoked by admins.
type AdminBackupContent interface {
  BackupContent

  // Creates the infrastructure (depend on implementation) that will contain the backup content.
  // Calling this method twice is a noop.
  SetupBackupContent(ctx context.Context) error

  // Performs the cleanups (depend on implementation) after the backups.
  // Calling this method twice is a noop.
  // Calling this method before setup is an error.
  TearDownBackupContent(ctx context.Context) error

  // Deletes all objects in `chunks`.
  // Objects not found (already deleted?) should be a noop.
  // Returns a non-nil error if at least 1 chunk could not be deleted.
  DeleteChunks(ctx context.Context, chunks []*pb.SnapshotChunks_Chunk) error
}

type GarbageCollector interface {
  // Remove from the backup all chunks that are not linked to any snapshot in the metadata.
  // Returns the list of **potentially** deleted chunks and any error that may have interrupted the cleaning.
  // In dry-run mode, nothing will be deleted.
  CleanUnreachableChunks(context.Context, bool) ([]*pb.SnapshotChunks_Chunk, error)

  // Removes any metadata objects that cannot be reached starting from a SnapshotSeqHead.
  // Returns the list of **potentially** deleted metadata objects and any error that may have interrupted the cleaning.
  // In dry-run mode, nothing will be deleted.
  //
  // Note: you can clean the orphan chunks calling CleanUnreachableChunks after.
  // This has the advantage of also cleaning the chunks that were never mentioned in the metadata to begin with.
  CleanUnreachableMetadata(context.Context, bool) (*DeletedItems, error)

  // Cascade removal of all objects reachable from a SnapshotSequence.
  // Returns the list of **potentially** deleted metadata and content objects and any error that may have interrupted the cleaning.
  // In dry-run mode, nothing will be deleted. If the sequence does not exists this is a noop.
  DeleteSnapshotSequence(context.Context, bool, string) (*DeletedItems, error)
}

