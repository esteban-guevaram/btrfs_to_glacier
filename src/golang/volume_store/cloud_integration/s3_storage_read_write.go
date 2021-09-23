package main

import (
  "bytes"
  "context"
  "fmt"
  "io"
  "math/rand"

  store "btrfs_to_glacier/volume_store/aws_s3_storage"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
  "github.com/google/uuid"
)

type s3ReadWriteTester struct {
  Conf *pb.Config
  Client *s3.Client
  Storage types.AdminStorage
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

func (self *s3ReadWriteTester) putRandomObject(ctx context.Context, size int) (string, []byte, error) {
  data := util.GenerateRandomTextData(size)
  reader := bytes.NewReader(data)
  key := uuid.NewString()
  in := &s3.PutObjectInput{
    Bucket: &self.Conf.Aws.S3.BucketName,
    Key: &key,
    Body: reader,
  }
  _, err := self.Client.PutObject(ctx, in)
  return key, data, err
}

func (self *s3ReadWriteTester) getObjectOrDie(ctx context.Context, key string) []byte {
  data, err := self.getObject(ctx, key)
  if err != nil { util.Fatalf("Failed to get object '%s': %v", key, err) }
  return data
}

func (self *s3ReadWriteTester) putRandomObjectOrDie(ctx context.Context, size int) (string, []byte) {
  key, data, err := self.putRandomObject(ctx, size)
  if err != nil { util.Fatalf("Failed to put object '%s': %v", key, err) }
  return key, data
}

func (self *s3ReadWriteTester) helperWrite(
    ctx context.Context, offset uint64, total_len uint64) *pb.SnapshotChunks {
  chunk_len := self.Conf.Aws.S3.ChunkLen
  data := util.GenerateRandomTextData(int(total_len))
  expect_obj := make([]byte, total_len - offset)
  copy(expect_obj, data[offset:])
  pipe := mocks.NewBigPreloadedPipe(ctx, data)

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

func emptyBucketOrDie(ctx context.Context, conf *pb.Config, client *s3.Client) {
  err := emptyBucket(ctx, conf, client)
  if err != nil { util.Fatalf("Failed empty object: %v", err) }
}

func emptyBucket(ctx context.Context, conf *pb.Config, client *s3.Client) error {
  list_in := &s3.ListObjectsV2Input{
    Bucket: &conf.Aws.S3.BucketName,
    MaxKeys: 1000,
  }
  out, err := client.ListObjectsV2(ctx, list_in)
  if err != nil { return err }
  for _,obj := range out.Contents {
    del_in := &s3.DeleteObjectInput{
      Bucket: &conf.Aws.S3.BucketName,
      Key: obj.Key,
    }
    _, err = client.DeleteObject(ctx, del_in)
    if err != nil { return err }
  }
  if out.ContinuationToken != nil { return fmt.Errorf("needs more iterations to delete all") }
  return nil
}

func deleteBucket(ctx context.Context, conf *pb.Config, client *s3.Client) error {
  err := emptyBucket(ctx, conf, client)
  if err != nil { return err }
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
  read_end := io.NopCloser(&bytes.Buffer{})
  done, err := self.Storage.WriteStream(ctx, offset, read_end)
  if err != nil { util.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { util.Fatalf("empty object should return error") }
      if chunk_or_err.Val != nil {
        chunks := chunk_or_err.Val.Chunks
        if len(chunks) > 0 { util.Fatalf("no chunks should have been returned") }
      }
    case <-ctx.Done(): util.Fatalf("timedout")
  }
}

func (self *s3ReadWriteTester) testReadObject_Helper(ctx context.Context, chunk_sizes []uint64) {
  var expect_data bytes.Buffer
  var offset uint64
  chunks := &pb.SnapshotChunks{ KeyFingerprint: "for_giggles", }
  for _,size := range chunk_sizes {
    key, data := self.putRandomObjectOrDie(ctx, int(size))
    expect_data.Write(data)
    chunk := &pb.SnapshotChunks_Chunk{ Uuid:key, Start:offset, Size:size, }
    chunks.Chunks = append(chunks.Chunks, chunk)
    offset += size
  }

  read_end,err := self.Storage.ReadChunksIntoStream(ctx, chunks)
  if err != nil { util.Fatalf("failed: %v", err) }

  var got_data []byte
  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data, err = io.ReadAll(read_end)
    if err != nil { util.Fatalf("failed: %v", err) }
  }()
  util.WaitForClosureOrDie(ctx, done)

  util.EqualsOrDie( "Mismatched concat data", got_data, expect_data.Bytes())
}

func (self *s3ReadWriteTester) TestReadSingleChunkObject(ctx context.Context) {
  sizes := []uint64{4096}
  self.testReadObject_Helper(ctx, sizes)
}

func (self *s3ReadWriteTester) TestReadMultiChunkObject(ctx context.Context) {
  sizes := []uint64{4096, 1024, 666,}
  self.testReadObject_Helper(ctx, sizes)
}

func (self *s3ReadWriteTester) testQueueRestoreObjects_Helper(ctx context.Context, keys []string, expect_obj types.ObjRestoreOrErr) {
  done, err := self.Storage.QueueRestoreObjects(ctx, keys)
  if err != nil { util.Fatalf("failed: %v", err) }

  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys { expect[k] = expect_obj }

  select {
    case res := <-done:
      util.Debugf("result: %v", res)
      util.EqualsOrDie("Bad obj status", res, expect)
    case <-ctx.Done(): util.Fatalf("timedout")
  }
}

func (self *s3ReadWriteTester) TestQueueRestoreObjects_StandardClass(ctx context.Context) {
  key1,_ := self.putRandomObjectOrDie(ctx, 4096)
  key2,_ := self.putRandomObjectOrDie(ctx, 1111)
  keys := []string{key1, key2}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  self.testQueueRestoreObjects_Helper(ctx, keys, expect_obj)
}

func (self *s3ReadWriteTester) TestQueueRestoreObjects_NoSuchObject(ctx context.Context) {
  keys := []string{uuid.NewString()}
  done, err := self.Storage.QueueRestoreObjects(ctx, keys)
  if err != nil { util.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      got_err := res[keys[0]].Err
      if !store.IsS3Error(new(s3_types.NoSuchKey), got_err) {
        util.Fatalf("Expected error status: %v", res)
      }
    case <-ctx.Done(): util.Fatalf("timedout")
  }
}

func (self *s3ReadWriteTester) TestQueueRestoreObjects_Idempotent(ctx context.Context, snowflake_bucket string, snowflake_key string) {
  tmp_conf := proto.Clone(self.Conf).(*pb.Config)
  tmp_conf.Aws.S3.BucketName = snowflake_bucket
  restore_func := store.TestOnlySwapConf(self.Storage, tmp_conf)
  defer restore_func()

  keys := []string{snowflake_key}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Pending, }
  self.testQueueRestoreObjects_Helper(ctx, keys, expect_obj)
  self.testQueueRestoreObjects_Helper(ctx, keys, expect_obj)
}

func (self *s3ReadWriteTester) TestQueueRestoreObjects_AlreadyRestored(ctx context.Context, snowflake_bucket string, snowflake_key string) {
  tmp_conf := proto.Clone(self.Conf).(*pb.Config)
  tmp_conf.Aws.S3.BucketName = snowflake_bucket
  restore_func := store.TestOnlySwapConf(self.Storage, tmp_conf)
  defer restore_func()

  keys := []string{snowflake_key}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  self.testQueueRestoreObjects_Helper(ctx, keys, expect_obj)
}

func (self *s3ReadWriteTester) testListAllChunks_Helper(ctx context.Context, total int, fill_size int32) {
  emptyBucketOrDie(ctx, self.Conf, self.Client)
  restore_f := store.TestOnlyChangeIterationSize(self.Storage, fill_size)
  defer restore_f()
  expect_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)

  for i:=0; i<total; i+=1 {
    key,data := self.putRandomObjectOrDie(ctx, rand.Intn(1024)+16)
    expect_objs[key] = &pb.SnapshotChunks_Chunk{ Uuid:key, Size:uint64(len(data)), }
  }

  it, err := self.Storage.ListAllChunks(ctx)
  if err != nil { util.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { util.Fatalf("failed while iterating: %v", it.Err()) }

  util.EqualsOrDie("Bad len", len(got_objs), len(expect_objs))
  for key,expect := range expect_objs {
    util.EqualsOrDie("Bad obj", got_objs[key], expect)
  }
}

func (self *s3ReadWriteTester) TestListAllChunksSingleFill(ctx context.Context) {
  const fill_size = 10
  const total = 3
  self.testListAllChunks_Helper(ctx, total, fill_size)
}

func (self *s3ReadWriteTester) TestListAllChunksMultiFill(ctx context.Context) {
  const fill_size = 4
  const total = fill_size * 3
  self.testListAllChunks_Helper(ctx, total, fill_size)
}

// we do not test with offsets, that should be covered by the unittests
func TestAllS3ReadWrite(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.AdminStorage) {
  suite := s3ReadWriteTester{
    Conf: conf, Client: client, Storage: storage,
  }

  suite.TestWriteObjectLessChunkLen(ctx)
  suite.TestWriteObjectMoreChunkLen(ctx)
  suite.TestWriteObjectMultipleChunkLen(ctx)
  suite.TestWriteEmptyObject(ctx)
  suite.TestReadSingleChunkObject(ctx)
  suite.TestListAllChunksSingleFill(ctx)
  suite.TestListAllChunksMultiFill(ctx)
  suite.TestReadMultiChunkObject(ctx)
  suite.TestQueueRestoreObjects_StandardClass(ctx)
  suite.TestQueueRestoreObjects_NoSuchObject(ctx)
  // adhoc manual testing given the nature of restores :-(
  //snowflake_bucket := "candide.test.bucket.1"
  //snowflake_key := "s3_obj_983680415d7ec9ccf80c88df8e3d4d7e"
  //suite.TestQueueRestoreObjects_Idempotent(ctx, snowflake_bucket, snowflake_key)
  //suite.TestQueueRestoreObjects_AlreadyRestored(ctx, snowflake_bucket, snowflake_key)
}

func TestAllS3Storage(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  new_conf := proto.Clone(conf).(*pb.Config)
  // A bigger chunk, will make tests slow+expensive
  new_conf.Aws.S3.ChunkLen = 128*1024

  codec := new(mocks.Codec)
  codec.Fingerprint = types.PersistableString{"some_fp"}
  storage, err := store.NewAdminStorage(new_conf, aws_conf, codec)
  //client := s3.NewFromConfig(*aws_conf)
  client := store.TestOnlyGetInnerClientToAvoidConsistencyFails(storage)
  if err != nil { util.Fatalf("%v", err) }

  TestS3StorageSetup(ctx, new_conf, client, storage)
  TestAllS3ReadWrite(ctx, new_conf, client, storage)
  TestAllS3Delete(ctx, new_conf, client, storage)
  deleteBucketOrDie(ctx, new_conf, client)
}

