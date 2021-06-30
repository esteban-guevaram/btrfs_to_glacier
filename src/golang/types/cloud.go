package types

import "context"
import "errors"
import pb "btrfs_to_glacier/messages"

var ErrNotFound = errors.New("key_not_found_in_metadata")

type Metadata interface {
  // Creates the storage objects (depend on implementation) that will contain the metadata.
  // Creation can take some time so it is done asynchronously.
  // If the channel contains a null error then the storage has been created ok and is ready to use.
  // It is a noop if they are already created.
  SetupMetadata(ctx context.Context) (<-chan error)

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

  // Adds `chunk` and records it into the subvolume `snap`.
  // If `chunk` is not the first, it must match the previous fingerprint.
  // Returns the new state of the subvolume metadata.
  // If `chunk` is already the last recorded in `snap` this is a noop.
  AppendChunkToSnapshot(ctx context.Context, snap *pb.SubVolume, chunk *pb.SnapshotChunks) (*pb.SubVolume, error)

  // Reads sequence head for subvolume with `uuid`.
  // If there is no head, returns `ErrNotFound`.
  ReadSnapshotSeqHead(ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error)

  // Reads snapshot sequence with key `uuid`.
  // If there is no sequence, returns `ErrNotFound`.
  ReadSnapshotSeq(ctx context.Context, uuid string) (*pb.SnapshotSequence, error)

  // Reads subvolume with `uuid`.
  // If there is no subvolume, returns `ErrNotFound`.
  ReadSnapshot(ctx context.Context, uuid string) (*pb.SubVolume, error)
}

type DangerMetadata interface {
  Metadata

  DeleteAllMetadata() error
  DeleteSnapshotSeqHead() error
  DeleteSnapshotSeq() error
  DeleteSnapshotChunks() error
}

// Implementation questions
// * Use s3 with storage classes Vs glacier direct api
//   * Same pricing (but s3 has cheaper deep class)
//   * Direct glacier has eternal free tier
//   * S3 implementation can use standard tier for testing
type Storage interface {
  WriteStream() error
  SendRestoreRq() error
  ReadStream() error
  DeleteBlob() error
}

type GarbageCollector interface {
  DeleteUnreachableFromSeqHead() error

  //IterateAllSnapshotSeqHeads() error
  //IterateAllSnapshotSeqs() error
  //IterateAllSnapshotChunks() error
}

