package mem_only

import (
  "bytes"
  "context"
  "fmt"
  "io"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type ChunkIoForTest struct { *ChunkIoImpl }

func (self *ChunkIoForTest) Get(uuid string) ([]byte, bool) { v,ok := self.Chunks[uuid]; return v,ok }
func (self *ChunkIoForTest) Set(uuid string, data []byte)  { self.Chunks[uuid] = data }
func (self *ChunkIoForTest) Len() int { return len(self.Chunks) }

func buildTestStorageWithChunkLen(t *testing.T, chunk_len uint64) (*Storage, *ChunkIoForTest) {
  conf := util.LoadTestConf()
  codec := new(mocks.Codec)
  gen_store,err := NewStorage(conf, codec)
  if err != nil { util.Fatalf("NewStorage: %v", err) }
  storage := gen_store.(*Storage)
  chunkio := &ChunkIoForTest{ storage.ChunkIo.(*ChunkIoImpl) }
  chunkio.ChunkLen = chunk_len
  return storage, chunkio
}

func TestWriteOneChunk_PipeError(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  const total_len = 48
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)
  util.ClosePipeWithError(pipe, fmt.Errorf("oopsie"))

  chunk_pb, more, err := chunkio.WriteOneChunk(ctx, offset, pipe.ReadEnd())
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
  _,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
  // the caller of writeOneChunk is responsible to advance the stream to the right offset
  data := util.GenerateRandomTextData(int(total_len-offset))
  expect_chunk := make([]byte, expect_size)
  expect_rest := make([]byte, expect_rest_len)
  copy(expect_chunk, data)
  copy(expect_rest, data[expect_size:])
  pipe := mocks.NewPreloadedPipe(data)

  chunk_pb, more, err := chunkio.WriteOneChunk(ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("writeOneChunk err: %v", err) }
  if more != expect_more { t.Fatalf("more data is wrong") }
  if len(chunk_pb.Uuid) < 1 { t.Fatalf("empty key written") }
  if chunk_pb.Start != offset { t.Fatalf("bad offset written") }
  if chunk_pb.Size != expect_size { t.Fatalf("bad chunk length written") }

  var rest []byte
  rest, err = io.ReadAll(pipe.ReadEnd())
  util.EqualsOrFailTest(t, "Bad remaining data", rest, expect_rest)

  chunk,found := chunkio.Get(chunk_pb.Uuid)
  if !found { t.Errorf("nothing written") }
  util.EqualsOrFailTest(t, "Bad object data", chunk, expect_chunk)
}

func helper_TestWriteEmptyChunk(t *testing.T, offset uint64, chunk_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
  read_end := io.NopCloser(&bytes.Buffer{})

  chunk_pb, more, err := chunkio.WriteOneChunk(ctx, offset, read_end)
  if err != nil { t.Errorf("empty write should return no more data and a nul chunk") }
  if more { t.Errorf("empty write not signal more data") }
  if chunk_pb != nil { t.Errorf("no chunks should have been returned") }
  if chunkio.Len() > 0 { t.Errorf("nothing should have been written to S3") }
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
  storage,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
  storage.Codec.(*mocks.Codec).Fingerprint = types.PersistableString{expect_fp}
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

      data,found := chunkio.Get(chunks[0].Uuid)
      if !found { t.Errorf("nothing written to S3") }
      util.EqualsOrFailTest(t, "Bad object data", data, expect_data)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func helper_TestWriteStream_EmptyChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
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
      if chunkio.Len() > 0 { t.Errorf("nothing should have been written to S3") }
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
  storage,chunkio := buildTestStorageWithChunkLen(t, chunk_len)
  storage.Codec.(*mocks.Codec).Fingerprint = types.PersistableString{expect_fp}
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
        data,found := chunkio.Get(chunk.Uuid)
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

func TestQueueRestoreObjects_Simple(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,chunkio := buildTestStorageWithChunkLen(t, 32)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys {
    chunkio.Set(k, []byte{})
    expect[k] = expect_obj
  }
  done, err := storage.QueueRestoreObjects(ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      util.EqualsOrFailTest(t, "Bad queue result", res, expect)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestQueueRestoreObjects_NoSuchObject(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Err:types.ErrChunkFound, }
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageWithChunkLen(t, 32)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys { expect[k] = expect_obj }

  done, err := storage.QueueRestoreObjects(ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      util.EqualsOrFailTest(t, "Bad queue result", res, expect)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func testReadChunksIntoStream_Helper(t *testing.T, chunks *pb.SnapshotChunks, datas [][]byte) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  var expect_data bytes.Buffer
  storage,chunkio := buildTestStorageWithChunkLen(t, 32)
  for i,chunk := range chunks.Chunks {
    expect_data.Write(datas[i])
    chunkio.Set(chunk.Uuid, datas[i])
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
  storage,_ := buildTestStorageWithChunkLen(t, 64)
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

func testStorageListAll_Helper(t *testing.T, total int) {
  const blob_len = 32
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage, chunkio := buildTestStorageWithChunkLen(t, blob_len)
  expect_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    obj := &pb.SnapshotChunks_Chunk{ Uuid:key, Size:blob_len, }
    chunkio.Set(key, util.GenerateRandomTextData(blob_len))
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

func TestListAllChunks_Simple(t *testing.T) {
  const total = 3
  testStorageListAll_Helper(t, total)
}
func TestListAllChunks_NoChunks(t *testing.T) {
  const total = 0
  testStorageListAll_Helper(t, total)
}

func TestSetupStorage(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageWithChunkLen(t, 16)
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

func testDeleteChunks_Helper(t *testing.T, obj_count int, add_keys bool) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  storage,chunkio := buildTestStorageWithChunkLen(t, 24)
  chunks := make([]*pb.SnapshotChunks_Chunk, obj_count)
  for i,_ := range chunks {
    chunks[i] = &pb.SnapshotChunks_Chunk{
      Uuid: uuid.NewString(),
    }
    if add_keys { chunkio.Set(chunks[i].Uuid, []byte("value")) }
  }

  done := storage.DeleteChunks(ctx, chunks)
  util.WaitForClosure(t, ctx, done)

  if !add_keys { return }
  for _,chunk := range chunks {
    _,found := chunkio.Get(chunk.Uuid)
    if found { t.Errorf("Failed deletion of: %s", chunk.Uuid) }
  }
}

func TestDeleteChunks_Simple(t *testing.T) {
  testDeleteChunks_Helper(t, 3, /*add_keys*/true)
}

func TestDeleteChunks_NotFoundNoop(t *testing.T) {
  testDeleteChunks_Helper(t, 3, /*add_keys*/false)
}

func TestDeleteChunks_NoKeysErr(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  storage,_ := buildTestStorageWithChunkLen(t, 96)
  chunks := make([]*pb.SnapshotChunks_Chunk, 0, 1)
  done := storage.DeleteChunks(ctx, chunks)

  select {
    case err := <-done:
      if err == nil { t.Errorf("expecting error for empty chunks") }
    case <-ctx.Done(): t.Fatalf("timeout")
  }
}

