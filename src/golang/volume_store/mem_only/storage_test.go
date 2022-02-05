package mem_only

import (
  "context"
  "testing"
  "time"

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

func buildTestStorageWithChunkLen(t *testing.T, chunk_len uint64) (types.AdminStorage, ChunkIoForTest) {
  conf := util.LoadTestConf()
  codec := new(mocks.Codec)
  gen_store,err := NewStorage(conf, codec)
  if err != nil { util.Fatalf("NewStorage: %v", err) }
  storage := gen_store.(*Storage)
  chunkio := &ChunkIoForTestImpl{ storage.ChunkIo.(*ChunkIoImpl) }
  chunkio.ChunkLen = chunk_len
  return storage, chunkio
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
  fixture := &Fixture{
    StorageCtor: storage_ctor,
    AdminCtor:   buildTestStorageWithChunkLen,
  }
  RunAllTestStorage(t, fixture)
}

