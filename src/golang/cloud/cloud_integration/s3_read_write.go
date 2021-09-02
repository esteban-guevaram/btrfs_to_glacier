package main

import (
  "context"
  "io"
  "time"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
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

func deleteBucketOrDie(ctx context.Context, conf *pb.Config, client *s3.Client) {
  err := deleteBucket(ctx, conf, client)
  if err != nil { util.Fatalf("Failed bucket delete: %v", err) }
}

func deleteBucket(ctx context.Context, conf *pb.Config, client *s3.Client) error {
  list_in := &s3.ListObjectsV2Input{
    Bucket: &conf.Aws.S3.BucketName,
    MaxKeys: 100,
  }
  out, err := client.ListObjectsV2(ctx, list_in)
  if err != nil { return err }
  for _,obj := range out.Contents {
    del_in := &s3.DeleteObjectInput{
      Bucket: &conf.Aws.S3.BucketName,
      Key: obj.Key,
    }
    _, err = client.DeleteObject(ctx, del_in)
    if err != nil { util.Fatalf("Failed delete object: %v", err) }
  }

  _, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
    Bucket: &conf.Aws.S3.BucketName,
  })
  return err
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

func TestS3StorageSetup(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.Storage) {
  err := deleteBucket(ctx, conf, client)

  if err != nil {
    if !cloud.IsS3Error(new(s3_types.NoSuchBucket), err) {
      util.Fatalf("%v", err)
    }
    util.Infof("TestStorageSetup '%s' not exist", conf.Aws.S3.BucketName)
  } else {
    waiter := s3.NewBucketNotExistsWaiter(client)
    wait_rq := &s3.HeadBucketInput{ Bucket: &conf.Aws.S3.BucketName, }
    err = waiter.Wait(ctx, wait_rq, 30 * time.Second)
    if err != nil { util.Fatalf("%v", err) }
    util.Infof("TestStorageSetup '%s' deleted", conf.Aws.S3.BucketName)
  }

  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
      util.Infof("Bucket '%s' created OK", conf.Aws.S3.BucketName)
    case <-ctx.Done():
  }

  done = storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("Not idempotent %v", err) }
    case <-ctx.Done():
  }
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

func TestAllS3Storage(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  new_conf := proto.Clone(conf).(*pb.Config)
  // A bigger chunk, will make tests slow+expensive
  new_conf.Aws.S3.ChunkLen = 128*1024

  codec := new(mocks.Codec)
  codec.Fingerprint = types.PersistableString{"some_fp"}
  storage, err := cloud.NewStorage(new_conf, aws_conf, codec)
  //client := s3.NewFromConfig(*aws_conf)
  client := cloud.TestOnlyGetInnerClientToAvoidConsistencyFails(storage)
  if err != nil { util.Fatalf("%v", err) }

  TestS3StorageSetup(ctx, new_conf, client, storage)
  TestAllS3ReadWrite(ctx, new_conf, client, storage)
  deleteBucketOrDie(ctx, new_conf, client)
}

