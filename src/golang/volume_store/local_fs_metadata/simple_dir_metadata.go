package local_fs_metadata

import (
  "context"
  "errors"
  "fmt"
  "io/fs"
  fpmod "path/filepath"
  "os"
  "sort"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"

  "github.com/google/uuid"
)

const (
  KeepLast = 3
)

type SimpleDirMetadata struct {
  *mem_only.Metadata
  DirInfo    *pb.LocalFs_Partition
  SymLink    string
  KeepLast   int
}

func MetaDir(dir_info *pb.LocalFs_Partition) string {
  return fpmod.Join(dir_info.MountRoot, dir_info.MetadataDir)
}
func SymLink(dir_info *pb.LocalFs_Partition) string {
  return fpmod.Join(dir_info.MountRoot, dir_info.MetadataDir, "metadata.pb.gz")
}

func NewMetadata(ctx context.Context, conf *pb.Config, part_uuid string) (types.Metadata, error) {
  var part *pb.LocalFs_Partition
  for _,p := range conf.LocalFs.Partitions {
    if p.PartitionUuid != part_uuid { continue }
    part = p
  }
  if part == nil { return nil, fmt.Errorf("Partition '%s' not found", part_uuid) }

  metadata := &SimpleDirMetadata{
    Metadata: &mem_only.Metadata{ Conf: conf, },
    DirInfo: part,
    SymLink: SymLink(part),
    KeepLast: KeepLast,
  }
  err := metadata.LoadPreviousStateFromDir(ctx)
  return metadata, err
}

func (self *SimpleDirMetadata) LoadPreviousStateFromDir(ctx context.Context) error {
  if self.State != nil { util.Fatalf("Cannot load state twice") }
  self.State = &pb.AllMetadata{
    CreatedTs: uint64(time.Now().Unix()),
  }
  err := util.UnmarshalGzProto(self.SymLink, self.State)
  if err != nil && !errors.Is(err, os.ErrNotExist) { return err }
  return nil
}

func (self *SimpleDirMetadata) MetaVer(ts uint64) string {
  name := fmt.Sprintf("metadata_%d.pb.gz", ts)
  return fpmod.Join(self.DirInfo.MountRoot, self.DirInfo.MetadataDir, name)
}

func (self *SimpleDirMetadata) CleanOldVersions(ctx context.Context) ([]string, error) {
  var cleaned_files []string
  var filter_entries []fs.DirEntry

  meta_dir := MetaDir(self.DirInfo)
  fs_dir := os.DirFS(meta_dir)
  entries, err := fs.ReadDir(fs_dir, ".")
  if err != nil { return cleaned_files, err }

  for _,e := range entries {
    info, err := e.Info()
    if err != nil { return cleaned_files, err }
    if e.IsDir() || info.Mode() & fs.ModeSymlink != 0 { continue }
    filter_entries = append(filter_entries, e)
  }
  sort.Slice(filter_entries, func(i,j int) bool {
    info_i, err := filter_entries[i].Info()
    if err != nil { util.Fatalf("failed to sort entries: %v", err) }
    info_j, err := filter_entries[j].Info()
    if err != nil { util.Fatalf("failed to sort entries: %v", err) }
    return info_j.ModTime().Before(info_i.ModTime())
  })

  if len(filter_entries) <= self.KeepLast { return cleaned_files, nil }
  for _,e := range filter_entries[self.KeepLast:] {
    path := fpmod.Join(meta_dir, e.Name())
    err := os.Remove(path)
    if err != nil { return cleaned_files, err }
    cleaned_files = append(cleaned_files, path)
  }
  return cleaned_files, nil
}

func (self *SimpleDirMetadata) SaveCurrentStateToDir(ctx context.Context) (string, error) {
  if self.State == nil { util.Fatalf("Cannot store nil state") }
  self.State.CreatedTs = uint64(time.Now().Unix())
  version_id := fmt.Sprintf("%d_%s", self.State.CreatedTs, uuid.NewString())
  prev_path,_ := fpmod.EvalSymlinks(self.SymLink)
  store_path := self.MetaVer(self.State.CreatedTs)

  err := util.MarshalGzProto(store_path, self.State)
  if err != nil { return "", err }

  if util.Exists(self.SymLink) {
    if err := os.Remove(self.SymLink); err != nil { return "", err }
  }
  err = os.Symlink(store_path, self.SymLink)
  if err != nil {
    // Attempt to add back the old path
    if len(prev_path) > 0 { os.Symlink(prev_path, self.SymLink) }
    return "", err
  }

  util.Infof("Saved metadata version: '%s'", version_id)
  _, err = self.CleanOldVersions(ctx)
  return version_id, err
}

func (self *SimpleDirMetadata) PersistCurrentMetadataState(ctx context.Context) (string, error) {
  return self.SaveCurrentStateToDir(ctx)
}

