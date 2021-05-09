package types

import "context"
import pb "btrfs_to_glacier/messages"

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

  //AppendSnapshotToSeq() error
  //AppendChunkToSnapshot() error

  //ReadSnapshotSeqHead() error
  //ReadSnapshotSeq() error

  //IterateAllSnapshotSeqHeads() error
  //IterateAllSnapshotSeqs() error
  //IterateAllSnapshotChunks() error
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
}

