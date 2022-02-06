package mem_only

import (
  "context"
  "io"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/encryption"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

type ChunkIoForTestImpl struct { *ChunkIoImpl }

func (self *ChunkIoForTestImpl) Get(uuid string) ([]byte, bool) { v,ok := self.Chunks[uuid]; return v,ok }
func (self *ChunkIoForTestImpl) Set(uuid string, data []byte)  { self.Chunks[uuid] = data }
func (self *ChunkIoForTestImpl) Len() int { return len(self.Chunks) }
func (self *ChunkIoForTestImpl) SetCodecFp(fp string) {
  self.ParCodec.(*mocks.Codec).Fingerprint = types.PersistableString{fp}
}

func buildTestStorage(
    t *testing.T, chunk_len uint64, codec types.Codec) (*Storage, *ChunkIoForTestImpl) {
  conf := util.LoadTestConf()
  gen_store,err := NewStorage(conf, codec)
  if err != nil { util.Fatalf("NewStorage: %v", err) }
  storage := gen_store.(*Storage)
  chunkio := &ChunkIoForTestImpl{ storage.ChunkIo.(*ChunkIoImpl) }
  chunkio.ChunkLen = chunk_len
  return storage, chunkio
}

func buildTestStorageWithChunkLen(t *testing.T, chunk_len uint64) (*Storage, *ChunkIoForTestImpl) {
  codec := new(mocks.Codec)
  return buildTestStorage(t, chunk_len, codec)
}

func buildTestCodec(t *testing.T) types.Codec {
  // see encryption/aes_gzip_codec_test.go
  const dummy_pw = "chocolat"
  const persisted_key_1 = "OC0aSSg2woV0bUfw0Ew1+ej5fYCzzIPcTnqbtuKXzk8="

  conf := util.LoadTestConf()
  conf.EncryptionKeys = []string {persisted_key_1,}
  fixed_pw := func() ([]byte, error) { return []byte(dummy_pw), nil }

  codec, err := encryption.NewCodecHelper(conf, fixed_pw)
  if err != nil { t.Fatalf("Could not create codec: %v", err) }
  return codec
}

func buildTestStorageRealCodec(t *testing.T, chunk_len uint64) (*Storage, *ChunkIoForTestImpl) {
  codec := buildTestCodec(t)
  return buildTestStorage(t, chunk_len, codec)
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

func TestAllMemOnlyStorage(t *testing.T) {
  storage_ctor := func(t *testing.T, chunk_len uint64) (types.Storage, ChunkIoForTest) {
    return buildTestStorageWithChunkLen(t, chunk_len)
  }
  admin_ctor := func(t *testing.T, chunk_len uint64) (types.AdminStorage, ChunkIoForTest) {
    return buildTestStorageWithChunkLen(t, chunk_len)
  }
  fixture := &Fixture{
    StorageCtor: storage_ctor,
    AdminCtor:   admin_ctor,
  }
  RunAllTestStorage(t, fixture)
}

func HelperWriteReadWithRealCodec(t *testing.T, chunk_len uint64, total_len uint64) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorageRealCodec(t, chunk_len)
  data := util.GenerateRandomTextData(int(total_len))
  expect_data := make([]byte, total_len)
  copy(expect_data, data)
  pipe := mocks.NewBigPreloadedPipe(ctx, data)

  var chunks_written *pb.SnapshotChunks
  done_write,err := storage.WriteStream(ctx, /*offest*/0, pipe.ReadEnd())
  if err != nil { t.Fatalf("failed: %v", err) }
  select {
    case chunk_or_err := <-done_write:
      if chunk_or_err.Err != nil { t.Fatalf("failed after done: %v", chunk_or_err.Err) }
      chunks_written = chunk_or_err.Val
    case <-ctx.Done(): t.Fatalf("timedout")
  }
  t.Logf("chunks_written: %v", chunks_written)

  var got_data []byte
  read_end,err := storage.ReadChunksIntoStream(ctx, chunks_written)
  if err != nil { t.Fatalf("failed: %v", err) }

  done := make(chan error)
  go func() {
    defer close(done)
    defer read_end.Close()
    got_data, err = io.ReadAll(read_end)
    done <- err
  }()
  t.Logf("ReadAll: %v", util.WaitForClosure(t, ctx, done))

  if len(got_data) < 1 { t.Fatalf("no data read") }
  util.EqualsOrFailTest(t, "Mismatched data", got_data, expect_data)
}

func TestWriteReadWithRealCodec_OneChunk(t *testing.T) {
  const chunk_len = 128
  const total_len = 64
  HelperWriteReadWithRealCodec(t, chunk_len, total_len)
}

func TestWriteReadWithRealCodec_2ChunksWithIv(t *testing.T) {
  const chunk_len = 64
  const total_len = chunk_len - 1
  HelperWriteReadWithRealCodec(t, chunk_len, total_len)
}

func TestWriteReadWithRealCodec_ManyChunks(t *testing.T) {
  const chunk_len = 64
  const total_len = chunk_len * 3
  HelperWriteReadWithRealCodec(t, chunk_len, total_len)
}

