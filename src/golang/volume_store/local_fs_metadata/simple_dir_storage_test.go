package local_fs_metadata


import (
  "context"
  "io/fs"
  "os"
  "testing"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"

  "github.com/google/uuid"
)

type ChunkIoForTestImpl struct { *ChunkIoImpl }

func (self *ChunkIoForTestImpl) Get(key string) ([]byte, bool) {
  data, err := os.ReadFile(ChunkFile(self.ChunkDir, key))
  if util.IsNotExist(err) { return nil, false }
  if err != nil { util.Fatalf("ChunkIoForTestImpl.Get: %v", err) }
  return data, true
}
func (self *ChunkIoForTestImpl) Set(key string, data []byte) {
  err := os.WriteFile(ChunkFile(self.ChunkDir, key), data, fs.ModePerm)
  if err != nil { util.Fatalf("ChunkIoForTestImpl.Set: %v", err) }
  self.ChunkIndex[key] = true
}
func (self *ChunkIoForTestImpl) Len() int {
  chunks,_,err := self.ListChunks(context.TODO(), nil)
  if err != nil { util.Fatalf("ChunkIoForTestImpl.Len: %v", err) }
  real_len := len(chunks)
  idx_len := len(self.ChunkIndex)
  util.Infof("idx_len=%d / real_len=%d", idx_len, real_len)
  return real_len
}
func (self *ChunkIoForTestImpl) SetCodecFp(fp string) {
  self.ParCodec.(*mocks.Codec).Fingerprint = types.PersistableString{fp}
}

func buildTestSimpleDirStorage(
    t *testing.T, chunk_len uint64, codec types.Codec) (*SimpleDirStorage, *ChunkIoForTestImpl) {
  local_fs, _ := util.TestSimpleDirLocalFs()
  conf := util.LoadTestConfWithLocalFs(local_fs)
  gen_store,err := NewSimpleDirStorageAdmin(conf, codec, local_fs.Sinks[0].Partitions[0].FsUuid)
  if err != nil { util.Fatalf("NewStorage: %v", err) }
  storage := gen_store.(*SimpleDirStorage)
  chunkio := &ChunkIoForTestImpl{ storage.ChunkIo.(*ChunkIoImpl) }
  chunkio.ChunkLen = chunk_len
  return storage, chunkio
}

func buildTestSimpleDirStorageWithChunkLen(
    t *testing.T, chunk_len uint64) (*SimpleDirStorage, *ChunkIoForTestImpl) {
  codec := new(mocks.Codec)
  return buildTestSimpleDirStorage(t, chunk_len, codec)
}

func HelperSetupStorage(t *testing.T, chunk_cnt int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  storage,chunkio := buildTestSimpleDirStorageWithChunkLen(t, 16)
  for i:=0; i<chunk_cnt; i+=1 {
    data := util.GenerateRandomTextData(i*32)
    key := uuid.NewString()
    chunkio.Set(key, data)
  }
  chunkio.ChunkIndex = make(map[string]bool)
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      util.EqualsOrFailTest(t, "Bad loaded state", len(chunkio.ChunkIndex), chunk_cnt)
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

func TestSetupStorage_Empty(t *testing.T) {
  HelperSetupStorage(t, 0)
}

func TestSetupStorage_Simple(t *testing.T) {
  HelperSetupStorage(t, 3)
}

func TestSetupStorage_Idempotent(t *testing.T) {
  HelperSetupStorage(t, 3)
  HelperSetupStorage(t, 3)
}

func TestAllSimpleDirStorage(t *testing.T) {
  storage_ctor := func(t *testing.T, chunk_len uint64) (types.Storage, mem_only.ChunkIoForTest) {
    return buildTestSimpleDirStorageWithChunkLen(t, chunk_len)
  }
  admin_ctor := func(t *testing.T, chunk_len uint64) (types.AdminStorage, mem_only.ChunkIoForTest) {
    return buildTestSimpleDirStorageWithChunkLen(t, chunk_len)
  }
  fixture := &mem_only.Fixture{
    StorageCtor: storage_ctor,
    AdminCtor:   admin_ctor,
  }
  mem_only.RunAllTestStorage(t, fixture)
}

