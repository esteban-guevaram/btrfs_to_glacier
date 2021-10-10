package aws_s3_storage

import (
  "bytes"
  "context"
  "fmt"
  "io"
  "sort"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3mgr "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
  "github.com/google/uuid"
)

type mockS3Client struct {
  Err              error
  RestoreObjectErr error
  DeleteObjsErr    error
  AccountId string
  Data map[string][]byte
  RestoreStx map[string]string
  Class map[string]s3_types.StorageClass
  Buckets map[string]bool
  HeadAlwaysEmpty bool
  HeadAlwaysAccessDenied bool
  FirstListObjEmpty      bool
  LastLifecycleIn *s3.PutBucketLifecycleConfigurationInput
  LastPublicAccessBlockIn *s3.PutPublicAccessBlockInput
}

func (self *mockS3Client) setObject(key string, data []byte, class s3_types.StorageClass, ongoing bool) {
  self.Data[key] = make([]byte, len(data))
  copy(self.Data[key], data)
  //self.Data[key] =  data
  self.Class[key] = class
  if class != s3_types.StorageClassStandard {
    self.RestoreStx[key] = fmt.Sprintf(`ongoing-request="%v"`, ongoing)
  }
}
func (self *mockS3Client) CreateBucket(
    ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
  self.Buckets[*in.Bucket] = true
  return &s3.CreateBucketOutput{}, self.Err
}
func (self *mockS3Client) DeleteObjects(
    ctx context.Context, in *s3.DeleteObjectsInput, opts...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
  out := &s3.DeleteObjectsOutput{}
  for _,obj_id := range in.Delete.Objects {
    delete(self.Data, *obj_id.Key)
    delete(self.RestoreStx, *obj_id.Key)
    delete(self.Class, *obj_id.Key)
  }
  if self.DeleteObjsErr != nil {
    str := self.DeleteObjsErr.Error()
    aws_err := s3_types.Error{ Code:&str, Key:&str, }
    out.Errors = []s3_types.Error{ aws_err }
  }
  return out, self.Err
}
func (self *mockS3Client) GetObject(
    ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
  key := *(in.Key)
  if _,found := self.Data[key]; !found {
    return nil, new(s3_types.NoSuchKey)
  }
  out := &s3.GetObjectOutput{
    Body: io.NopCloser(bytes.NewReader(self.Data[key])),
    ContentLength: int64(len(self.Data[key])),
  }
  return out, self.Err
}
func (self *mockS3Client) ListObjectsV2(
    ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
  if in.Prefix != nil || in.StartAfter != nil { util.Fatalf("not_implemented") }
  var page_count int32
  out := &s3.ListObjectsV2Output{
    ContinuationToken: in.ContinuationToken,
    Contents:make([]s3_types.Object, 0, len(self.Data)),
  }

  if self.FirstListObjEmpty {
    self.FirstListObjEmpty = false
    out.NextContinuationToken = aws.String("1")
    return out, self.Err
  }

  // iteration order is not stable: https://go.dev/blog/maps
  keys := make([]string, 0, len(self.Data))
  for key,_ := range self.Data { keys = append(keys, key) }
  sort.Strings(keys)

  for _,key:= range keys {
    if in.ContinuationToken != nil && key <= *in.ContinuationToken { continue }
    data := self.Data[key]
    item := s3_types.Object{ Key:aws.String(key), Size:int64(len(data)), }
    out.Contents = append(out.Contents, item)
    page_count += 1
    if page_count >= in.MaxKeys { out.NextContinuationToken = aws.String(key); break }
  }
  out.KeyCount = page_count
  return out, self.Err
}
func (self *mockS3Client) HeadBucket(
    ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
  _,found := self.Buckets[*(in.Bucket)]
  if self.HeadAlwaysEmpty || !found {
    return nil, new(s3_types.NoSuchBucket)
  }
  bad_owner := in.ExpectedBucketOwner != nil && *(in.ExpectedBucketOwner) != self.AccountId
  if self.HeadAlwaysAccessDenied || bad_owner {
    // Error model is too complex to mock
    // https://aws.github.io/aws-sdk-go-v2/docs/handling-errors/#api-error-responses
    return nil, StrToApiErr("AccessDenied")
  }
  return &s3.HeadBucketOutput{}, self.Err
}
func (self *mockS3Client) HeadObject(
    ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
  key := *(in.Key)
  if _,found := self.Data[key]; !found {
    return nil, new(s3_types.NoSuchKey)
  }
  out := &s3.HeadObjectOutput{ StorageClass: self.Class[key], }
  if restore,found := self.RestoreStx[key]; found { out.Restore = &restore }
  return out, self.Err
}
func (self *mockS3Client) PutBucketLifecycleConfiguration(
    ctx context.Context, in *s3.PutBucketLifecycleConfigurationInput, opts ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
  self.LastLifecycleIn = in
  rs := &s3.PutBucketLifecycleConfigurationOutput{}
  return rs, self.Err
}
func (self *mockS3Client) PutPublicAccessBlock(
    ctx context.Context, in *s3.PutPublicAccessBlockInput, opts ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error) {
  self.LastPublicAccessBlockIn = in
  return &s3.PutPublicAccessBlockOutput{}, self.Err
}
func (self *mockS3Client) RestoreObject(
  ctx context.Context, in *s3.RestoreObjectInput, opts ...func(*s3.Options)) (*s3.RestoreObjectOutput, error) {
  return &s3.RestoreObjectOutput{}, self.RestoreObjectErr
}
func (self *mockS3Client) Upload(
    ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3mgr.Uploader)) (*s3mgr.UploadOutput, error) {
  if in.Bucket == nil || len(*in.Bucket) < 1 { return nil, fmt.Errorf("malormed request") }
  if in.Key == nil || len(*in.Key) < 1 { return nil, fmt.Errorf("malormed request") }
  if self.Err == nil {
    buf, err := io.ReadAll(in.Body)
    if err != nil { return nil, err }
    if _,found := self.Data[*in.Key]; found { return nil, fmt.Errorf("overwritting key") }
    self.Data[*in.Key] = buf
    return &s3mgr.UploadOutput{}, self.Err
  }
  return nil, self.Err
}

func buildTestStorage(t *testing.T) (*s3Storage, *mockS3Client) {
  conf := util.LoadTestConf()
  return buildTestStorageWithConf(t, conf)
}

func buildTestStorageWithChunkLen(t *testing.T, chunk_len uint64) (*s3Storage, *mockS3Client) {
  conf := util.LoadTestConf()
  conf.Aws.S3.ChunkLen = chunk_len
  return buildTestStorageWithConf(t, conf)
}

func buildTestStorageWithConf(t *testing.T, conf *pb.Config) (*s3Storage, *mockS3Client) {
  client := &mockS3Client {
    AccountId: "some_random_string",
    Data: make(map[string][]byte),
    Class: make(map[string]s3_types.StorageClass),
    RestoreStx: make(map[string]string),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  codec := new(mocks.Codec)
  aws_conf, err := util.NewAwsConfig(context.TODO(), conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }

  storage := &s3Storage{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: client,
    uploader: client,
    bucket_wait: 10 * time.Millisecond,
    account_id: client.AccountId,
  }
  injectConstants(storage)
  return storage, client
}

func TestWriteOneChunk_PipeError(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  const total_len = 48
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageWithChunkLen(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)
  util.ClosePipeWithError(pipe, fmt.Errorf("oopsie"))

  chunk_pb, more, err := storage.writeOneChunk(ctx, offset, pipe.ReadEnd())
  if err == nil { t.Fatalf("expected call to fail") }
  if more { t.Fatalf("should not signal more data") }
  if chunk_pb != nil { t.Fatalf("no chunk should be returned") }
}

func TestWriteStream_PipeError(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  const total_len = 48
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageWithChunkLen(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)
  util.ClosePipeWithError(pipe, fmt.Errorf("oopsie"))

  done, err := storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("expected to fail but not right now: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("expected error") }
      if chunk_or_err.Val == nil { return }
      chunks := chunk_or_err.Val.Chunks
      if len(chunks) > 0 { t.Errorf("no chunks should have been written") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestWriteStream_OffsetTooBig(t *testing.T) {
  const offset = 159
  const chunk_len = 32
  const total_len = 48
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageWithChunkLen(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("expected to fail but not right now: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("expected error") }
      if chunk_or_err.Val != nil { t.Errorf("no chunks should have been written") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func helper_TestWriteOneChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  expect_more := total_len-offset >= chunk_len
  expect_size := chunk_len
  expect_rest_len := total_len - offset - chunk_len
  if chunk_len > (total_len - offset) {
    expect_size = total_len - offset
    expect_rest_len = 0
  }
  storage,client := buildTestStorageWithChunkLen(t, chunk_len)
  // the caller of writeOneChunk is responsible to advance the stream to the right offset
  data := util.GenerateRandomTextData(int(total_len-offset))
  expect_chunk := make([]byte, expect_size)
  expect_rest := make([]byte, expect_rest_len)
  copy(expect_chunk, data)
  copy(expect_rest, data[expect_size:])
  pipe := mocks.NewPreloadedPipe(data)

  chunk_pb, more, err := storage.writeOneChunk(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("writeOneChunk err: %v", err) }
  if more != expect_more { t.Fatalf("more data is wrong") }
  if len(chunk_pb.Uuid) < 1 { t.Fatalf("empty key written") }
  if chunk_pb.Start != offset { t.Fatalf("bad offset written") }
  if chunk_pb.Size != expect_size { t.Fatalf("bad chunk length written") }

  var rest []byte
  rest, err = io.ReadAll(pipe.ReadEnd())
  util.EqualsOrFailTest(t, "Bad remaining data", rest, expect_rest)
  chunk,found := client.Data[chunk_pb.Uuid]
  if !found { t.Errorf("nothing written to S3") }
  util.EqualsOrFailTest(t, "Bad object data", chunk, expect_chunk)
}

func helper_TestWriteEmptyChunk(t *testing.T, offset uint64, chunk_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorageWithChunkLen(t, chunk_len)
  read_end := io.NopCloser(&bytes.Buffer{})

  chunk_pb, more, err := storage.writeOneChunk(ctx, offset, read_end)
  if err != nil { t.Errorf("empty write should return no more data and a nul chunk") }
  if more { t.Errorf("empty write not signal more data") }
  if chunk_pb != nil { t.Errorf("no chunks should have been returned") }
  if len(client.Data) > 0 { t.Errorf("nothing should have been written to S3") }
}

func TestWriteOneChunk_LessThanFullContent(t *testing.T) {
  helper_TestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/48)
}

func TestWriteOneChunk_WithOffset_LessThanFullContent(t *testing.T) {
  helper_TestWriteOneChunk(t, /*offset=*/7, /*chunk_len=*/32, /*total_len=*/48)
}

func TestWriteOneChunk_EqualToFullContent(t *testing.T) {
  helper_TestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/37, /*total_len=*/37)
}

func TestWriteOneChunk_MoreThanFullContent(t *testing.T) {
  helper_TestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/64, /*total_len=*/48)
}

func TestWriteOneChunk_WithOffset_MoreThanFullContent(t *testing.T) {
  helper_TestWriteOneChunk(t, /*offset=*/3, /*chunk_len=*/47, /*total_len=*/48)
}

func TestWriteOneChunk_EmptyContent(t *testing.T) {
  helper_TestWriteEmptyChunk(t, /*offset=*/0, /*chunk_len=*/32)
}

func TestWriteOneChunk_WithOffset_EmptyContent(t *testing.T) {
  helper_TestWriteEmptyChunk(t, /*offset=*/34, /*chunk_len=*/32)
}

func helper_TestWriteStream_SingleChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  const expect_fp = "coco_fp"
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorageWithChunkLen(t, chunk_len)
  storage.codec.(*mocks.Codec).Fingerprint = types.PersistableString{expect_fp}
  data := util.GenerateRandomTextData(int(total_len))
  expect_data := make([]byte, total_len - offset)
  copy(expect_data, data[offset:])
  pipe := mocks.NewPreloadedPipe(data)
  expect_chunks := &pb.SnapshotChunks{
    KeyFingerprint: expect_fp,
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid: "some_uuid", Start: offset, Size: total_len-offset, },
    },
  }

  done, err := storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err != nil { t.Fatalf("failed after done: %v", chunk_or_err.Err) }
      chunks := chunk_or_err.Val.Chunks
      if len(chunks) < 1 || len(chunks[0].Uuid) < 1 { t.Fatalf("Malformed chunks: %v", chunks) }
      expect_chunks.Chunks[0].Uuid = chunks[0].Uuid //intended since uuid is random
      util.EqualsOrFailTest(t, "Bad SnapshotChunks", chunk_or_err.Val, expect_chunks)

      data,found := client.Data[chunks[0].Uuid]
      if !found { t.Errorf("nothing written to S3") }
      util.EqualsOrFailTest(t, "Bad object data", data, expect_data)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func helper_TestWriteStream_EmptyChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorageWithChunkLen(t, chunk_len)
  data := util.GenerateRandomTextData(int(total_len))
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("empty stream should return error") }
      if chunk_or_err.Val != nil {
        chunks := chunk_or_err.Val.Chunks
        if len(chunks) > 0 { t.Errorf("no chunks should have been returned") }
      }
      if len(client.Data) > 0 { t.Errorf("nothing should have been written to S3") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestWriteStream_SingleSmallChunk(t *testing.T) {
  helper_TestWriteStream_SingleChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/31)
}

func TestWriteStream_WithOffset_SingleSmallChunk(t *testing.T) {
  helper_TestWriteStream_SingleChunk(t, /*offset=*/24, /*chunk_len=*/32, /*total_len=*/49)
}

func TODO_TestWriteStream_Empty(t *testing.T) {
  helper_TestWriteStream_EmptyChunk(t, /*offset=*/0, /*chunk_len=*/128, /*total_len=*/0)
}

func TestWriteStream_WithOffset_Empty(t *testing.T) {
  helper_TestWriteStream_EmptyChunk(t, /*offset=*/128, /*chunk_len=*/128, /*total_len=*/128)
}

func helper_TestWriteStream_MultiChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  var chunk_cnt uint64 = (total_len - offset + chunk_len - 1) / chunk_len
  const expect_fp = "loco_fp"
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorageWithChunkLen(t, chunk_len)
  storage.codec.(*mocks.Codec).Fingerprint = types.PersistableString{expect_fp}
  data := util.GenerateRandomTextData(int(total_len))
  expect_data := make([]byte, total_len)
  copy(expect_data, data)
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err != nil { t.Fatalf("failed after done: %v", chunk_or_err.Err) }
      chunks := chunk_or_err.Val.Chunks
      util.EqualsOrFailTest(t, "Bad number of chunks", len(chunks), chunk_cnt)
      util.EqualsOrFailTest(t, "Bad fingerprint", chunk_or_err.Val.KeyFingerprint, expect_fp)

      uuids := make(map[string]bool)
      var next_start uint64 = offset
      for idx,chunk := range chunks {
        data,found := client.Data[chunk.Uuid]
        if !found { t.Errorf("chunk not found: %s", chunk.Uuid) }
        expect_chunk := expect_data[chunk.Start:chunk.Start+chunk.Size]
        util.EqualsOrFailTest(t, "Bad object data", data, expect_chunk)
        util.EqualsOrFailTest(t, "Bad start offset", chunk.Start, next_start)

        last_len := (total_len-offset) % chunk_len
        if last_len == 0 { last_len = chunk_len }
        if uint64(idx) == (chunk_cnt-1) {
          util.EqualsOrFailTest(t, "Bad last chunk len", chunk.Size, last_len)
        } else {
          util.EqualsOrFailTest(t, "Bad chunk len", chunk.Size, chunk_len)
        }
        next_start += chunk_len
        uuids[chunk.Uuid] = true
      }
      util.EqualsOrFailTest(t, "Duplicate uuid", len(uuids), chunk_cnt)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestWriteStream_MultiChunk(t *testing.T) {
  helper_TestWriteStream_MultiChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/132)
}

func TestWriteStream_WithOffset_MultiChunk(t *testing.T) {
  helper_TestWriteStream_MultiChunk(t, /*offset=*/48, /*chunk_len=*/32, /*total_len=*/132)
}

func TestWriteStream_MultipleChunkLen(t *testing.T) {
  helper_TestWriteStream_MultiChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/96)
}

func TestWriteStream_WithOffset_MultipleChunkLen(t *testing.T) {
  helper_TestWriteStream_MultiChunk(t, /*offset=*/3, /*chunk_len=*/32, /*total_len=*/99)
}

// Restore testing with aws-cli
//
// aws s3api list-objects-v2 --bucket candide.test.bucket.1
// {
//     "Contents": [
//         { "Key": "s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2", "StorageClass": "STANDARD" },
//         { "Key": "s3_obj_983680415d7ec9ccf80c88df8e3d4d7e", "StorageClass": "DEEP_ARCHIVE" },
//         { "Key": "s3_obj_ded6568a3a377ba99e41d06053fc00ce", "StorageClass": "GLACIER" },
//         { "Key": "to_archive/", "StorageClass": "DEEP_ARCHIVE" },
//         { "Key": "to_archive/s3_obj_3cfa5e4cecc6576a1dac67b193c72b13", "StorageClass": "DEEP_ARCHIVE" }
//     ]
// }
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// (no output)
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"true\"",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_983680415d7ec9ccf80c88df8e3d4d7e
// {
//     "Metadata": {},
//     "StorageClass": "DEEP_ARCHIVE"
// }
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// An error occurred (RestoreAlreadyInProgress) when calling the RestoreObject operation: Object restore is already in progress
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2 --restore-request Days=3
// An error occurred (InvalidObjectState) when calling the RestoreObject operation: Restore is not allowed for the object's current storage class
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2
// { "Metadata": {} } # Note: no mention of storage class nor restore status
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce /tmp/ded6568a3a377ba99e41d06053fc00ce
// An error occurred (InvalidObjectState) when calling the GetObject operation: The operation is not valid for the object's storage class
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_4d93c3eab6d1cfc9d7043b8cc45ea7d2 /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
// {
//     "ContentLength": 524288,
//     "ContentType": "binary/octet-stream",
//     "Metadata": {}
// }
// md5sum /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
// 4d93c3eab6d1cfc9d7043b8cc45ea7d2  /tmp/4d93c3eab6d1cfc9d7043b8cc45ea7d2
//
// aws s3api head-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"false\", expiry-date=\"Thu, 09 Sep 2021 00:00:00 GMT\"",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// aws s3api get-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce /tmp/ded6568a3a377ba99e41d06053fc00ce
// {
//     "Restore": "ongoing-request=\"false\", expiry-date=\"Thu, 09 Sep 2021 00:00:00 GMT\"",
//     "ContentLength": 524288,
//     "ContentType": "binary/octet-stream",
//     "Metadata": {},
//     "StorageClass": "GLACIER"
// }
//
// md5sum /tmp/ded6568a3a377ba99e41d06053fc00ce 
// ded6568a3a377ba99e41d06053fc00ce  /tmp/ded6568a3a377ba99e41d06053fc00ce
//
// aws s3api restore-object --bucket candide.test.bucket.1 --key s3_obj_ded6568a3a377ba99e41d06053fc00ce --restore-request Days=3
// (no output) # extend restore lifetime
func testQueueRestoreObjects_Helper(
    t *testing.T, keys []string, class s3_types.StorageClass, ongoing bool, expect_obj types.ObjRestoreOrErr, restore_err error) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys {
    client.setObject(k, []byte{}, class, ongoing)
    expect[k] = expect_obj
  }
  client.RestoreObjectErr = restore_err
  done, err := storage.QueueRestoreObjects(ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      util.EqualsOrFailTest(t, "Bad queue result", res, expect)
      t.Logf("Error? %v", res)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestQueueRestoreObjects_Simple(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Pending, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, true, expect_obj, nil)
}

func TestQueueRestoreObjects_AlreadyRestored(t *testing.T) {
  keys := []string{"k1"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, false, expect_obj, nil)
}

func TestQueueRestoreObjects_RestoreOngoing(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := StrToApiErr(RestoreAlreadyInProgress)
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Pending, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassDeepArchive, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_NotArchived(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := new(s3_types.InvalidObjectState)
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassStandard, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_NoSuchObject(t *testing.T) {
  keys := []string{"k1", "k2"}
  restore_err := new(s3_types.NoSuchKey)
  expect_obj := types.ObjRestoreOrErr{ Err:new(s3_types.NoSuchKey), }
  testQueueRestoreObjects_Helper(t, keys, s3_types.StorageClassStandard, false, expect_obj, restore_err)
}

func TestQueueRestoreObjects_HeadFail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorage(t)
  keys := []string{"k1", "k2"}
  done, err := storage.QueueRestoreObjects(ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      for k,s := range res {
        if s.Err == nil { t.Errorf("Expected error for %v:%v", k, s) }
      }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func testReadChunksIntoStream_Helper(t *testing.T, chunks *pb.SnapshotChunks, datas [][]byte) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  var expect_data bytes.Buffer
  storage,client := buildTestStorage(t)
  for i,chunk := range chunks.Chunks {
    expect_data.Write(datas[i])
    client.setObject(chunk.Uuid, datas[i], s3_types.StorageClassStandard, false)
  }

  read_end,err := storage.ReadChunksIntoStream(ctx, chunks)
  if err != nil { t.Fatalf("failed: %v", err) }

  var got_data []byte
  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data, err = io.ReadAll(read_end)
    if err != nil { t.Fatalf("failed: %v", err) }
  }()
  util.WaitForClosure(t, ctx, done)

  util.EqualsOrFailTest(t, "Mismatched concat data", got_data, expect_data.Bytes())
}

func TestReadChunksIntoStream_Single(t *testing.T) {
  datas := [][]byte{
    []byte("hey_mr_monkey"),
  }
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: "for_giggles",
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:uint64(len(datas[0])), },
    },
  }
  testReadChunksIntoStream_Helper(t, chunks, datas)
}

func TestReadChunksIntoStream_Multiple(t *testing.T) {
  datas := [][]byte{
    []byte("hey_mr_monkey"),
    []byte("where_s_the_banana_stash"),
  }
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: "for_giggles",
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:uint64(len(datas[0])), },
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid1", Start:uint64(len(datas[0])), Size:uint64(len(datas[1])), },
    },
  }
  testReadChunksIntoStream_Helper(t, chunks, datas)
}

func TestReadChunksIntoStream_Missing(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorage(t)
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: "for_giggles",
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:66, },
    },
  }

  read_end,err := storage.ReadChunksIntoStream(ctx, chunks)
  if err != nil { t.Fatalf("failed: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data,_ := io.ReadAll(read_end)
    if len(got_data) > 0 { t.Errorf("Expected empty pipe for missing object") }
  }()
  util.WaitForClosure(t, ctx, done)
}

func testStorageListAll_Helper(t *testing.T, total int, fill_size int32) {
  const blob_len = 32
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage, client := buildTestStorage(t)
  storage.iter_buf_len = fill_size
  expect_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    obj := &pb.SnapshotChunks_Chunk{ Uuid:key, Size:blob_len, }
    client.setObject(key, util.GenerateRandomTextData(blob_len),
                     s3_types.StorageClassStandard, false)
    expect_objs[key] = obj
  }

  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { t.Fatalf("failed while iterating: %v", it.Err()) }

  util.EqualsOrFailTest(t, "Bad len", len(got_objs), len(expect_objs))
  for key,expect := range expect_objs {
    util.EqualsOrFailTest(t, "Bad obj", got_objs[key], expect)
  }
}

func TestListAllChunks_SingleFill(t *testing.T) {
  const fill_size = 10
  const total = 3
  testStorageListAll_Helper(t, total, fill_size)
}
func TestListAllChunks_MultipleFill(t *testing.T) {
  const fill_size = 3
  const total = 10
  testStorageListAll_Helper(t, total, fill_size)
}
func TestListAllChunks_NoObjects(t *testing.T) {
  const fill_size = 3
  const total = 0
  testStorageListAll_Helper(t, total, fill_size)
}
func TestListAllChunks_EmptyNonFinalFill(t *testing.T) {
  const fill_size = 3
  const total = 2 * fill_size
  const blob_len = 32
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  storage, client := buildTestStorage(t)
  storage.iter_buf_len = fill_size

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    client.setObject(key, util.GenerateRandomTextData(blob_len),
                     s3_types.StorageClassStandard, false)
  }

  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { t.Fatalf("failed while iterating: %v", it.Err()) }
  util.EqualsOrFailTest(t, "Bad len", len(got_objs), total)
}
func TestListAllChunks_ErrDuringIteration(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage, client := buildTestStorage(t)
  client.Err = fmt.Errorf("iteration fail")
  it, err := storage.ListAllChunks(ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  if it.Next(ctx, obj) { t.Errorf("should not have returned any object") }
  if it.Err() == nil { t.Fatalf("expected error") }
}
