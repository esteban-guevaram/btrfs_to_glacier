package local_fs

import (
  "context"
  "fmt"
  "io"
  "io/fs"
  "os"
  fpmod "path/filepath"
  "strings"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"

  "github.com/google/uuid"
)

const (
  ChunkLen = 128 * 1024 * 1024
  ChunkSuffix = ".chunk"
  ForRead = true
  ForWrite = false
)

type ChunkIoImpl struct {
  ChunkIndex map[string]bool
  ParCodec   types.Codec
  ChunkLen   uint64
  ChunkDir   string
}

type SimpleDirStorage struct {
  *mem_only.BaseStorage
}

func StoreDir(part *pb.LocalFs_Partition) string {
  return fpmod.Join(part.MountRoot, part.StorageDir)
}

func NewChunkIoImpl(part *pb.LocalFs_Partition, codec types.Codec) *ChunkIoImpl {
  return &ChunkIoImpl{
    ChunkIndex: make(map[string]bool),
    ParCodec:   codec,
    ChunkLen:   ChunkLen,
    ChunkDir:   StoreDir(part),
  }
}

func NewSimpleDirStorageAdmin(conf *pb.Config, codec types.Codec, fs_uuid string) (types.AdminBackupContent, error) {
  var part *pb.LocalFs_Partition
  for _,g := range conf.LocalFs.Sinks {
  for _,p := range g.Partitions {
    if p.FsUuid != fs_uuid { continue }
    part = p
  }}
  if part == nil { return nil, fmt.Errorf("Partition '%s' not found", fs_uuid) }

  inner_storage := &mem_only.BaseStorage{
    ChunkIo: NewChunkIoImpl(part, codec),
    Conf: conf,
    Codec: codec,
  }
  return &SimpleDirStorage{ inner_storage }, nil
}

func NewSimpleDirStorage(conf *pb.Config, codec types.Codec, fs_uuid string) (types.BackupContent, error) {
  return NewSimpleDirStorageAdmin(conf, codec, fs_uuid)
}

func ChunkFile(root string, key string) string {
  return fpmod.Join(root, fmt.Sprintf("%s%s", key, ChunkSuffix))
}

func UuidFromDentry(dentry fs.DirEntry) string {
  last := strings.Index(dentry.Name(), ChunkSuffix)
  if last < 0 || last != len(dentry.Name()) - len(ChunkSuffix) { return "" }
  key := dentry.Name()[0:last]
  return key
}

func (self *ChunkIoImpl) OpenChunk(key string, read bool) (*os.File, error) {
  filepath := ChunkFile(self.ChunkDir, key)
  if !read {
    f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0755)
    if err == nil { self.ChunkIndex[key] = true }
    return f, err
  }
  if read && !self.ChunkIndex[key] { return nil, types.ErrChunkFound }
  f, err := os.Open(filepath)
  return f, err
}

func (self *ChunkIoImpl) ReadOneChunk(
    ctx context.Context, key_fp types.PersistableString, chunk *pb.SnapshotChunks_Chunk, output io.WriteCloser) error {
  f, err_closer := self.OpenChunk(chunk.Uuid, ForRead)
  if err_closer != nil { return err_closer }
  defer func() { util.OnlyCloseWhenError(f, err_closer) }()

  finfo, err_closer := f.Stat()
  if err_closer != nil { return err_closer }
  if uint64(finfo.Size()) != chunk.Size {
    err_closer = fmt.Errorf("mismatched length with metadata: %d != %d", finfo.Size(), chunk.Size)
    return err_closer
  }

  read_wrap := util.WrapPlainReaderCloser(f)
  err := self.ParCodec.DecryptStreamLeaveSinkOpen(ctx, key_fp, read_wrap, output)
  return err
}

func (self *ChunkIoImpl) WriteOneChunk(
    ctx context.Context, start_offset uint64, clear_input types.ReadEndIf) (*pb.SnapshotChunks_Chunk, bool, error) {
  key := uuid.NewString()
  f, err := self.OpenChunk(key, ForWrite)
  if err != nil { return nil, false, err }
  defer func() { util.CloseWithError(f, err) }()

  limit_reader := util.NewLimitedReadEnd(clear_input, self.ChunkLen)
  encrypted_reader, err := self.ParCodec.EncryptStream(ctx, limit_reader)
  if err != nil { return nil, false, err }
  defer encrypted_reader.Close()

  cnt, err := io.Copy(f, encrypted_reader)
  if err != nil { return nil, false, err }

  chunk := &pb.SnapshotChunks_Chunk{
    Uuid: key,
    Start: start_offset,
    Size: uint64(cnt),
  }

  codec_hdr := self.ParCodec.EncryptionHeaderLen()
  // We leave the empty chunk in the fs as an orphan
  if mem_only.IsChunkEmpty(self.ChunkLen, codec_hdr, chunk.Size, limit_reader.N) { return nil, false, nil }
  return chunk, mem_only.HasMoreData(self.ChunkLen, codec_hdr, chunk.Size, limit_reader.N), nil
}

func (self *ChunkIoImpl) RestoreSingleObject(ctx context.Context, key string) types.ObjRestoreOrErr {
  if !self.ChunkIndex[key] { return types.ObjRestoreOrErr{ Err:types.ErrChunkFound } }
  return types.ObjRestoreOrErr{ Stx:types.Restored, }
}

func (self *ChunkIoImpl) ListChunks(
    ctx context.Context, continuation *string) ([]*pb.SnapshotChunks_Chunk, *string, error) {
  if continuation != nil { return nil, nil, fmt.Errorf("implementation does not support continuation tokens") }
  chunks := make([]*pb.SnapshotChunks_Chunk, 0, len(self.ChunkIndex))
  fs_dir := os.DirFS(self.ChunkDir)
  entries, err := fs.ReadDir(fs_dir, ".")
  if err != nil { return chunks, nil, err }

  for _,dentry := range entries {
    key := UuidFromDentry(dentry)
    if dentry.IsDir() || len(key) < 1 { continue }

    finfo, err := dentry.Info()
    if err != nil { return chunks, nil, err }
    if finfo.Size() < 1 { continue }
    if !self.ChunkIndex[key] { util.Fatalf("Index is not correct: %v", dentry) }

    c := &pb.SnapshotChunks_Chunk{
      Uuid: key,
      Start: 0, //unknown
      Size: uint64(finfo.Size()),
    }
    chunks = append(chunks, c)
  }
  return chunks, nil, nil
}

func (self *SimpleDirStorage) ChunkDir() string { return self.ChunkIo.(*ChunkIoImpl).ChunkDir }
func (self *SimpleDirStorage) ChunkIndex() map[string]bool { return self.ChunkIo.(*ChunkIoImpl).ChunkIndex }

func (self *SimpleDirStorage) SetupBackupContent(ctx context.Context) error {
  if !util.IsDir(self.ChunkDir()) {
    return fmt.Errorf("'%s' is not a directory", self.ChunkDir())
  }
  if len(self.ChunkIndex()) < 1 {
    return self.LoadPreviousStateFromDir(ctx);
  }
  return nil
}

func (self *SimpleDirStorage) LoadPreviousStateFromDir(ctx context.Context) error {
  fs_dir := os.DirFS(self.ChunkDir())
  entries, err := fs.ReadDir(fs_dir, ".")
  if err != nil { return err }

  for _,dentry := range entries {
    key := UuidFromDentry(dentry)
    if dentry.IsDir() || len(key) < 1 { continue }
    self.ChunkIndex()[key] = true
  }
  util.Infof("SimpleDirStorage.ChunkIndex has %d entries", len(self.ChunkIndex()))
  return nil
}

func (self *SimpleDirStorage) DeleteChunks(
    ctx context.Context, chunks []*pb.SnapshotChunks_Chunk) error {
  if len(chunks) < 1 { return fmt.Errorf("cannot delete 0 keys") }
  for _,chunk := range chunks {
    err := os.Remove(ChunkFile(self.ChunkDir(), chunk.Uuid))
    if err != nil && !util.IsNotExist(err) { return err }
    self.ChunkIndex()[chunk.Uuid] = false
  }
  return nil
}

