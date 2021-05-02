package types

import "context"
import pb "btrfs_to_glacier/messages"

type Metadata interface {
  // Creates the storage objects (depend on implementation) that will contain the metadata.
  // Creation can take some time so it is done asynchronously.
  // If the channel contains a null error then the storage has been created ok and is ready to use.
  // It is a noop if they are already created.
  SetupMetadata(ctx context.Context) (<-chan error)

  //RecordSnapshotSeqHead(ctx context.Context, new_seq *pb.SnapshotSeq) error
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
  DeleteSnapshotSeq(seq *pb.SnapshotSeq) error
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

