package mem_only

import (
  "bytes"
  "context"
  "errors"
  "io"
  "reflect"
  "runtime"
  "strings"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type ChunkIoForTest interface {
  ChunkIoIf
  Get(uuid string) ([]byte, bool)
  Set(uuid string, data []byte)
  AlwaysReturnErr(storage types.Storage, err error)
  Len() int
  SetCodecFp(string)
  GetCodecFp() types.PersistableString
}

type StorageCtor = func(*testing.T, uint64) (types.Storage, ChunkIoForTest)
type AdminCtor   = func(*testing.T, uint64) (types.AdminStorage, ChunkIoForTest)
type TearDown    = func(*testing.T)
type Fixture struct {
  Ctx         context.Context
  StorageCtor StorageCtor
  AdminCtor   AdminCtor
  TearDown    TearDown
}
type TestF = func(*Fixture, *testing.T)

func (self *Fixture) TestWriteOneChunk_PipeClosed(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  const total_len = 48
  _,chunkio := self.StorageCtor(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)
  pipe.ReadEnd().Close()

  chunk_pb, more, _ := chunkio.WriteOneChunk(self.Ctx, offset, pipe.ReadEnd())
  if more { t.Fatalf("should not signal more data") }
  if chunk_pb != nil { t.Fatalf("no chunk should be returned") }
}

func (self *Fixture) TODOTestWriteOneChunk_ErrPropagation(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  _,chunkio := self.StorageCtor(t, chunk_len)
  pipe := mocks.NewErrorPipe()

  chunk_pb, more, err := chunkio.WriteOneChunk(self.Ctx, offset, pipe.ReadEnd())
  if err == nil { t.Errorf("chunkio.WriteOneChunk: expected error") }
  if more { t.Fatalf("should not signal more data") }
  if chunk_pb != nil { t.Fatalf("no chunk should be returned") }
}

func (self *Fixture) TestWriteStream_PipeClosed(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  const total_len = 48
  storage,_ := self.StorageCtor(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)
  pipe.ReadEnd().Close()

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("expected to fail but not right now: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("expected error") }
      if chunk_or_err.Val == nil { return }
      chunks := chunk_or_err.Val.Chunks
      if len(chunks) > 1 { t.Errorf("At most only the IV should have been written") }
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TODOTestWriteStream_ErrPropagation(t *testing.T) {
  const offset = 0
  const chunk_len = 32
  storage,_ := self.StorageCtor(t, chunk_len)
  pipe := mocks.NewErrorPipe()

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("expected to fail but not right now: %v", err) }
  select {
    case chunk_or_err := <-done:
      got_err := chunk_or_err.Err
      if got_err == nil { t.Errorf("storage.WriteStream: expected error") }
      if !errors.Is(got_err, mocks.ErrIoPipe) { t.Errorf("storage.WriteStream bad error: %v", got_err) }
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TestWriteStream_OffsetTooBig(t *testing.T) {
  const offset = 159
  const chunk_len = 32
  const total_len = 48
  storage,_ := self.StorageCtor(t, chunk_len)
  data := util.GenerateRandomTextData(total_len)
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("expected to fail but not right now: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("expected error") }
      if chunk_or_err.Val != nil { t.Errorf("no chunks should have been written") }
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) HelperTestWriteOneChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  expect_more := total_len-offset >= chunk_len
  expect_size := chunk_len
  expect_rest_len := total_len - offset - chunk_len
  if chunk_len > (total_len - offset) {
    expect_size = total_len - offset
    expect_rest_len = 0
  }
  _,chunkio := self.StorageCtor(t, chunk_len)
  // the caller of writeOneChunk is responsible to advance the stream to the right offset
  data := util.GenerateRandomTextData(int(total_len-offset))
  expect_chunk := make([]byte, expect_size)
  expect_rest := make([]byte, expect_rest_len)
  copy(expect_chunk, data)
  copy(expect_rest, data[expect_size:])
  pipe := mocks.NewPreloadedPipe(data)

  chunk_pb, more, err := chunkio.WriteOneChunk(self.Ctx, offset, pipe.ReadEnd())
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

func (self *Fixture) HelperTestWriteEmptyChunk(t *testing.T, offset uint64, chunk_len uint64) {
  _,chunkio := self.StorageCtor(t, chunk_len)
  read_end := util.ReadEndFromBytes(nil)

  chunk_pb, more, err := chunkio.WriteOneChunk(self.Ctx, offset, read_end)
  if err != nil { t.Errorf("empty write should return no more data and a nul chunk") }
  if more { t.Errorf("empty write not signal more data") }
  if chunk_pb != nil { t.Errorf("no chunks should have been returned") }
  if chunkio.Len() > 0 { t.Errorf("nothing should have been written to storage") }
}

func (self *Fixture) TestWriteOneChunk_LessThanFullContent(t *testing.T) {
  self.HelperTestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/48)
}

func (self *Fixture) TestWriteOneChunk_WithOffset_LessThanFullContent(t *testing.T) {
  self.HelperTestWriteOneChunk(t, /*offset=*/7, /*chunk_len=*/32, /*total_len=*/48)
}

func (self *Fixture) TestWriteOneChunk_EqualToFullContent(t *testing.T) {
  self.HelperTestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/37, /*total_len=*/37)
}

func (self *Fixture) TestWriteOneChunk_MoreThanFullContent(t *testing.T) {
  self.HelperTestWriteOneChunk(t, /*offset=*/0, /*chunk_len=*/64, /*total_len=*/48)
}

func (self *Fixture) TestWriteOneChunk_WithOffset_MoreThanFullContent(t *testing.T) {
  self.HelperTestWriteOneChunk(t, /*offset=*/3, /*chunk_len=*/47, /*total_len=*/48)
}

func (self *Fixture) TestWriteOneChunk_EmptyContent(t *testing.T) {
  self.HelperTestWriteEmptyChunk(t, /*offset=*/0, /*chunk_len=*/32)
}

func (self *Fixture) TestWriteOneChunk_WithOffset_EmptyContent(t *testing.T) {
  self.HelperTestWriteEmptyChunk(t, /*offset=*/34, /*chunk_len=*/32)
}

func (self *Fixture) HelperTestWriteStream_SingleChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  const expect_fp = "coco_fp"
  storage,chunkio := self.StorageCtor(t, chunk_len)
  chunkio.SetCodecFp(expect_fp)
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

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err != nil { t.Fatalf("failed after done: %v", chunk_or_err.Err) }
      chunks := chunk_or_err.Val.Chunks
      if len(chunks) < 1 || len(chunks[0].Uuid) < 1 { t.Fatalf("Malformed chunks: %v", chunks) }
      expect_chunks.Chunks[0].Uuid = chunks[0].Uuid //intended since uuid is random
      util.EqualsOrFailTest(t, "Bad SnapshotChunks", chunk_or_err.Val, expect_chunks)

      data,found := chunkio.Get(chunks[0].Uuid)
      if !found { t.Errorf("nothing written to storage") }
      util.EqualsOrFailTest(t, "Bad object data", data, expect_data)
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) HelperTestWriteStream_EmptyChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  storage,chunkio := self.StorageCtor(t, chunk_len)
  data := util.GenerateRandomTextData(int(total_len))
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done:
      if chunk_or_err.Err == nil { t.Errorf("empty stream should return error") }
      if chunk_or_err.Val != nil {
        chunks := chunk_or_err.Val.Chunks
        if len(chunks) > 0 { t.Errorf("no chunks should have been returned") }
      }
      if chunkio.Len() > 0 { t.Errorf("nothing should have been written to storage") }
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TestWriteStream_SingleSmallChunk(t *testing.T) {
  self.HelperTestWriteStream_SingleChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/31)
}

func (self *Fixture) TestWriteStream_WithOffset_SingleSmallChunk(t *testing.T) {
  self.HelperTestWriteStream_SingleChunk(t, /*offset=*/24, /*chunk_len=*/32, /*total_len=*/49)
}

func (self *Fixture) TestWriteStream_Empty(t *testing.T) {
  self.HelperTestWriteStream_EmptyChunk(t, /*offset=*/0, /*chunk_len=*/128, /*total_len=*/0)
}

func (self *Fixture) TestWriteStream_WithOffset_Empty(t *testing.T) {
  self.HelperTestWriteStream_EmptyChunk(t, /*offset=*/128, /*chunk_len=*/128, /*total_len=*/128)
}

func (self *Fixture) HelperTestWriteStream_MultiChunk(t *testing.T, offset uint64, chunk_len uint64, total_len uint64) {
  var chunk_cnt uint64 = (total_len - offset + chunk_len - 1) / chunk_len
  const expect_fp = "loco_fp"
  storage,chunkio := self.StorageCtor(t, chunk_len)
  chunkio.SetCodecFp(expect_fp)
  data := util.GenerateRandomTextData(int(total_len))
  expect_data := make([]byte, total_len)
  copy(expect_data, data)
  pipe := mocks.NewPreloadedPipe(data)

  done, err := storage.WriteStream(self.Ctx, offset, pipe.ReadEnd())
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
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TestWriteStream_MultiChunk(t *testing.T) {
  self.HelperTestWriteStream_MultiChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/132)
}

func (self *Fixture) TestWriteStream_WithOffset_MultiChunk(t *testing.T) {
  self.HelperTestWriteStream_MultiChunk(t, /*offset=*/48, /*chunk_len=*/32, /*total_len=*/132)
}

func (self *Fixture) TestWriteStream_MultipleChunkLen(t *testing.T) {
  self.HelperTestWriteStream_MultiChunk(t, /*offset=*/0, /*chunk_len=*/32, /*total_len=*/96)
}

func (self *Fixture) TestWriteStream_WithOffset_MultipleChunkLen(t *testing.T) {
  self.HelperTestWriteStream_MultiChunk(t, /*offset=*/3, /*chunk_len=*/32, /*total_len=*/99)
}

func (self *Fixture) TestQueueRestoreObjects_Simple(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Stx:types.Restored, }
  storage,chunkio := self.StorageCtor(t, 32)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys {
    chunkio.Set(k, []byte{})
    expect[k] = expect_obj
  }
  done, err := storage.QueueRestoreObjects(self.Ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      util.EqualsOrFailTest(t, "Bad queue result", res, expect)
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TestQueueRestoreObjects_NoSuchObject(t *testing.T) {
  keys := []string{"k1", "k2"}
  expect_obj := types.ObjRestoreOrErr{ Err:types.ErrChunkFound, }
  storage,_ := self.StorageCtor(t, 32)
  expect := make(map[string]types.ObjRestoreOrErr)
  for _,k := range keys { expect[k] = expect_obj }

  done, err := storage.QueueRestoreObjects(self.Ctx, keys)
  if err != nil { t.Fatalf("failed: %v", err) }

  select {
    case res := <-done:
      util.EqualsOrFailTest(t, "Bad queue result", res, expect)
    case <-self.Ctx.Done(): t.Fatalf("timedout")
  }
}

func (self *Fixture) TestReadOneChunk_Ok(t *testing.T) {
  const chunk_len = 32
  _,chunkio := self.StorageCtor(t, chunk_len)
  pipe := util.NewInMemPipe(self.Ctx)
  defer pipe.ReadEnd().Close()
  chunk := &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:chunk_len, }
  data := util.GenerateRandomTextData(chunk_len)
  chunkio.Set(chunk.Uuid, data)
 
  done := make(chan error)
  go func() {
    defer close(done)
    got_data, err := io.ReadAll(pipe.ReadEnd())
    if err != nil { t.Errorf("io.ReadAll: %v", err) }
    util.EqualsOrFailTest(t, "bad data", got_data, data)
  }()

  key_fp := chunkio.GetCodecFp()
  err := chunkio.ReadOneChunk(self.Ctx, key_fp, chunk, pipe.WriteEnd())
  pipe.WriteEnd().Close()
  if err != nil { t.Fatalf("chunkio.ReadOneChunk: %v", err) }
  util.WaitForClosure(t, self.Ctx, done)
}

func (self *Fixture) TestReadOneChunk_PipeClosed(t *testing.T) {
  const chunk_len = 32
  _,chunkio := self.StorageCtor(t, chunk_len)
  pipe := util.NewFileBasedPipe(self.Ctx)
  defer pipe.ReadEnd().Close()
  pipe.WriteEnd().Close()
  chunk := &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:chunk_len, }
  data := util.GenerateRandomTextData(chunk_len)
  chunkio.Set(chunk.Uuid, data)

  key_fp := chunkio.GetCodecFp()
  err := chunkio.ReadOneChunk(self.Ctx, key_fp, chunk, pipe.WriteEnd())
  if err == nil { t.Fatalf("chunkio.ReadOneChunk: expected to fail") }
  got_data, err := io.ReadAll(pipe.ReadEnd())
  if err != nil { t.Errorf("io.ReadAll: %v", err) }
  if len(got_data) > 0 { t.Errorf("no data should have been read: %v", len(got_data)) }
}

func (self *Fixture) TestReadOneChunk_ErrPropagation(t *testing.T) {
  const chunk_len = 32
  _,chunkio := self.StorageCtor(t, chunk_len)
  pipe := mocks.NewErrorPipe()
  chunk := &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:chunk_len, }
  data := util.GenerateRandomTextData(chunk_len)
  chunkio.Set(chunk.Uuid, data)

  key_fp := chunkio.GetCodecFp()
  err := chunkio.ReadOneChunk(self.Ctx, key_fp, chunk, pipe.WriteEnd())
  if err == nil { t.Errorf("chunkio.ReadOneChunk: expected to fail") }
  if !errors.Is(err, mocks.ErrIoPipe) { t.Errorf("chunkio.ReadOneChunk wrong error: %v", err) }
}

func (self *Fixture) TODOTestReadChunksIntoStream_ErrPropagation(t *testing.T) {
  const chunk_len = 32
  expect_err := errors.New("artificial_err")
  storage,chunkio := self.StorageCtor(t, chunk_len)
  chunkio.AlwaysReturnErr(storage, expect_err)

  chunkio.SetCodecFp("some_fp")
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: chunkio.GetCodecFp().S,
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:chunk_len, },
    },
  }

  read_end,err := storage.ReadChunksIntoStream(self.Ctx, chunks)
  if err != nil { t.Fatalf("premature failure: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    _, err = io.ReadAll(read_end)
    if err == nil { t.Errorf("io.ReadAll: expected error") }
    if !errors.Is(err, expect_err) { t.Errorf("io.ReadAll wrong error: %v", err) }
  }()
  util.WaitForClosure(t, self.Ctx, done)
}

func (self *Fixture) HelperReadChunksIntoStream(t *testing.T, chunks *pb.SnapshotChunks, datas [][]byte) {
  var expect_data bytes.Buffer
  storage,chunkio := self.StorageCtor(t, 32)
  for i,chunk := range chunks.Chunks {
    expect_data.Write(datas[i])
    chunkio.Set(chunk.Uuid, datas[i])
  }

  read_end,err := storage.ReadChunksIntoStream(self.Ctx, chunks)
  if err != nil { t.Fatalf("failed: %v", err) }

  var got_data []byte
  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data, err = io.ReadAll(read_end)
    if err != nil { t.Fatalf("failed: %v", err) }
  }()
  util.WaitForClosure(t, self.Ctx, done)

  util.EqualsOrFailTest(t, "Mismatched concat data", got_data, expect_data.Bytes())
}

func (self *Fixture) TestReadChunksIntoStream_Single(t *testing.T) {
  datas := [][]byte{
    []byte("hey_mr_monkey"),
  }
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: "for_giggles",
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:uint64(len(datas[0])), },
    },
  }
  self.HelperReadChunksIntoStream(t, chunks, datas)
}

func (self *Fixture) TestReadChunksIntoStream_Multiple(t *testing.T) {
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
  self.HelperReadChunksIntoStream(t, chunks, datas)
}

func (self *Fixture) TestReadChunksIntoStream_Missing(t *testing.T) {
  storage,_ := self.StorageCtor(t, 64)
  chunks := &pb.SnapshotChunks{
    KeyFingerprint: "for_giggles",
    Chunks: []*pb.SnapshotChunks_Chunk{
      &pb.SnapshotChunks_Chunk{ Uuid:"uuid0", Start:0, Size:66, },
    },
  }

  read_end,err := storage.ReadChunksIntoStream(self.Ctx, chunks)
  if err != nil { t.Fatalf("failed: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data,_ := io.ReadAll(read_end)
    if len(got_data) > 0 { t.Errorf("Expected empty pipe for missing object") }
  }()
  util.WaitForClosure(t, self.Ctx, done)
}

func (self *Fixture) HelperStorageListAll(t *testing.T, total int) {
  const blob_len = 32
  storage, chunkio := self.StorageCtor(t, blob_len)
  expect_objs := make(map[string]*pb.SnapshotChunks_Chunk)
  got_objs := make(map[string]*pb.SnapshotChunks_Chunk)

  for i:=0; i<total; i+=1 {
    key := uuid.NewString()
    obj := &pb.SnapshotChunks_Chunk{ Uuid:key, Size:blob_len, }
    chunkio.Set(key, util.GenerateRandomTextData(blob_len))
    expect_objs[key] = obj
  }

  it, err := storage.ListAllChunks(self.Ctx)
  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  obj := &pb.SnapshotChunks_Chunk{}
  for it.Next(self.Ctx, obj) {
    got_objs[obj.Uuid] = proto.Clone(obj).(*pb.SnapshotChunks_Chunk)
  }
  if it.Err() != nil { t.Fatalf("failed while iterating: %v", it.Err()) }

  util.EqualsOrFailTest(t, "Bad len", len(got_objs), len(expect_objs))
  for key,expect := range expect_objs {
    util.EqualsOrFailTest(t, "Bad obj", got_objs[key], expect)
  }
}

func (self *Fixture) TestListAllChunks_Simple(t *testing.T) {
  const total = 3
  self.HelperStorageListAll(t, total)
}
func (self *Fixture) TestListAllChunks_NoChunks(t *testing.T) {
  const total = 0
  self.HelperStorageListAll(t, total)
}

func (self *Fixture) HelperDeleteChunks(t *testing.T, obj_count int, add_keys bool) {
  storage,chunkio := self.AdminCtor(t, 24)
  chunks := make([]*pb.SnapshotChunks_Chunk, obj_count)
  for i,_ := range chunks {
    chunks[i] = &pb.SnapshotChunks_Chunk{
      Uuid: uuid.NewString(),
    }
    if add_keys { chunkio.Set(chunks[i].Uuid, []byte("value")) }
  }

  done := storage.DeleteChunks(self.Ctx, chunks)
  util.WaitForClosure(t, self.Ctx, done)

  if !add_keys { return }
  for _,chunk := range chunks {
    _,found := chunkio.Get(chunk.Uuid)
    if found { t.Errorf("Failed deletion of: %s", chunk.Uuid) }
  }
}

func (self *Fixture) TestDeleteChunks_Simple(t *testing.T) {
  self.HelperDeleteChunks(t, 3, /*add_keys*/true)
}

func (self *Fixture) TestDeleteChunks_NotFoundNoop(t *testing.T) {
  self.HelperDeleteChunks(t, 3, /*add_keys*/false)
}

func (self *Fixture) TestDeleteChunks_NoKeysErr(t *testing.T) {
  storage,_ := self.AdminCtor(t, 96)
  chunks := make([]*pb.SnapshotChunks_Chunk, 0, 1)
  done := storage.DeleteChunks(self.Ctx, chunks)

  select {
    case err := <-done:
      if err == nil { t.Errorf("expecting error for empty chunks") }
    case <-self.Ctx.Done(): t.Fatalf("timeout")
  }
}


/////////////////////////////////////////////////////////////////////////////////////////////////////

type NameAndTest struct { Name string; Func TestF }
func newEntry(f TestF) *NameAndTest {
  name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
  return &NameAndTest{ name, f }
}
func GetAllTests() []*NameAndTest {
  tests := []*NameAndTest{}
  fix_t := reflect.TypeOf(&Fixture{})
  for i := 0; i < fix_t.NumMethod(); i++ {
    method := fix_t.Method(i)
    if !strings.HasPrefix(method.Name, "Test") { continue }
    test_f := method.Func.Interface().(TestF)
    entry := &NameAndTest{method.Name, test_f}
    tests = append(tests, entry)
  }
  return tests
}

func RunAllTestStorage(t *testing.T, fixture *Fixture) {
  tests := GetAllTests()
  for _,i := range tests {
    if i.Func == nil { t.Fatalf("Bad test entry: %v", i) }
    run_f := func(t *testing.T) {
      ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
      defer cancel()
      fixture.Ctx = ctx
      i.Func(fixture, t)
      if (fixture.TearDown != nil) { fixture.TearDown(t) }
    }
    t.Run(i.Name, run_f)
  }
}

