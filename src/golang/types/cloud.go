package types

// Implementation questions
// * Use dynamodb for storing metadata ?
//   * Can be used as KV store but blobs limited to 400KB
//   * Eternal free tier should cover all storage needs
//   * Which replication settings to get 99.999 durability ? (already replicated in the aws region)
type Metadata interface {
  RecordSnapshotSeqHead() error
  AppendSnapshotToSeq() error
  AppendChunkToSnapshot() error

  ReadSnapshotSeqHead() error
  ReadSnapshotSeq() error

  IterateAllSnapshotSeqHeads() error
  IterateAllSnapshotSeqs() error
  IterateAllSnapshotChunks() error
}

type DangerMetadata interface {
  Metadata

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

