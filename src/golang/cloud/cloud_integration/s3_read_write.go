package main

import (
  "context"
  "io"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ReadWriteTester struct {
  Conf *pb.Config
  Client *s3.Client
  Storage types.Storage
}

func (self *s3ReadWriteTester) getObject(ctx context.Context, key string) ([]byte, error) {
  in := &s3.GetObjectInput{
    Bucket: &self.Conf.Aws.S3.BucketName,
    Key: &key,
  }
  out, err := self.Client.GetObject(ctx, in)
  if err != nil { return nil, err }
  defer out.Body.Close()
  var data []byte
  data, err = io.ReadAll(out.Body)
  if err == nil { util.Infof("Got object '%s' size=%d", key, len(data)) }
  return data, err
}

func (self *s3ReadWriteTester) getObjectOrDie(ctx context.Context, key string) []byte {
  data, err := self.getObject(ctx, key)
  if err != nil { util.Fatalf("Failed to get object '%s': %v", key, err) }
  return data
}

func (self *s3ReadWriteTester) helperWrite(
    ctx context.Context, offset uint64, total_len uint64) *pb.SnapshotChunks {
  chunk_len := self.Conf.Aws.S3.ChunkLen
  data := util.GenerateRandomTextData(int(total_len))
  expect_obj := make([]byte, total_len - offset)
  copy(expect_obj, data[offset:])
  pipe := types.NewMockBigPreloadedPipe(ctx, data)

  var chunk_or_err types.ChunksOrError
  done, err := self.Storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { util.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err = <-done:
      if chunk_or_err.Err != nil { util.Fatalf("failed after done: %v", chunk_or_err.Err) }
      chunks := chunk_or_err.Val.Chunks
      if len(chunk_or_err.Val.KeyFingerprint) < 1 || len(chunks) < 1 || len(chunks[0].Uuid) < 1 {
        util.Fatalf("Malformed chunks: %v", chunks)
      }
      for idx,chunk := range chunks {
        if len(chunk.Uuid) < 1 { util.Fatalf("Malformed chunk: %v", chunk) }
        object := self.getObjectOrDie(ctx, chunk.Uuid)
        start := int(chunk_len) * idx
        end := start + int(chunk_len)
        if end > len(expect_obj) { end = len(expect_obj) }
        util.EqualsOrDie("Bad object data", object, expect_obj[start:end])
      }
    case <-ctx.Done(): util.Fatalf("timedout")
  }
  return chunk_or_err.Val
}

func (self *s3ReadWriteTester) TestWriteObjectLessChunkLen(ctx context.Context) {
  const offset = 0
  var total_len = self.Conf.Aws.S3.ChunkLen/3
  expect_chunk := &pb.SnapshotChunks_Chunk{
    Uuid: "some_uuid",
    Start: offset,
    Size: total_len-offset,
  }

  snap_chunks := self.helperWrite(ctx, offset, total_len)
  first_chunk := snap_chunks.Chunks[0]
  expect_chunk.Uuid = first_chunk.Uuid //intended since uuid is random
  util.EqualsOrDie("Bad SnapshotChunk[0]", first_chunk, expect_chunk)
}

func (self *s3ReadWriteTester) TestWriteObjectMoreChunkLen(ctx context.Context) {
  const offset = 0
  var total_len = self.Conf.Aws.S3.ChunkLen*3 + 1
  snap_chunks := self.helperWrite(ctx, offset, total_len)
  util.EqualsOrDie("Bad number of chunks", len(snap_chunks.Chunks), 4)
}

func (self *s3ReadWriteTester) TestWriteObjectMultipleChunkLen(ctx context.Context) {
  const offset = 0
  var total_len = self.Conf.Aws.S3.ChunkLen * 2
  snap_chunks := self.helperWrite(ctx, offset, total_len)
  util.EqualsOrDie("Bad number of chunks", len(snap_chunks.Chunks), 2)
}

func (self *s3ReadWriteTester) TestWriteEmptyObject(ctx context.Context) {
  const offset = 0
  const total_len = 0
  snap_chunks := self.helperWrite(ctx, offset, total_len)
  util.EqualsOrDie("Bad number of chunks", len(snap_chunks.Chunks), 0)
}

// we do not test with offsets, that should be covered by the unittests
func TestAllS3ReadWrite(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.Storage) {
  suite := s3ReadWriteTester{
    Conf: conf, Client: client, Storage: storage,
  }

  suite.TestWriteObjectLessChunkLen(ctx)
  suite.TestWriteObjectMoreChunkLen(ctx)
  //suite.TestWriteObjectMultipleChunkLen(ctx)
  //suite.TestWriteEmptyObject(ctx)
}

