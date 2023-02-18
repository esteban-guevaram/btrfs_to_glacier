package local_fs

import (
  "context"
  "fmt"
  "os"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

type RoundRobinMetadata struct {
  *SimpleDirMetadata
  Sink        *pb.Backup_RoundRobin
  Conf        *pb.Config
  Linuxutil   types.Linuxutil
  PairStorage types.AdminBackupContent
}

// Does not initialize inner SimpleDirMetadata because filesystem may not be mounted yet.
func NewRoundRobinMetadataAdmin(ctx context.Context,
    conf *pb.Config, lu types.Linuxutil, backup_name string) (types.AdminMetadata, error) {
  var sink *pb.Backup_RoundRobin
  if b,err := util.BackupByName(conf, backup_name); err == nil {
    if len(b.Fs.Sinks) != 1 { fmt.Errorf("Only supports backups with 1 sink: %s", backup_name) }
    sink = b.Fs.Sinks[0]
  } else {
    return nil, err
  }
  if sink == nil {
    return nil, fmt.Errorf("Sink '%s' not found", backup_name)
  }
  if len(sink.Partitions) < 1 {
    return nil, fmt.Errorf("Sink '%s' does not contain any partition", backup_name)
  }

  metadata := &RoundRobinMetadata{
    SimpleDirMetadata: nil,
    Sink: sink,
    Conf: conf, // shadows the Conf field on the base object
    Linuxutil: lu,
    PairStorage: nil,
  }
  return metadata, nil
}

func NewRoundRobinMetadata(ctx context.Context,
    conf *pb.Config, lu types.Linuxutil, backup_name string) (types.Metadata, error) {
  return NewRoundRobinMetadataAdmin(ctx, conf, lu, backup_name)
}

func (self *RoundRobinMetadata) MountAllSinkPartitions(ctx context.Context) error {
  for _,part := range self.Sink.Partitions {
    err := os.MkdirAll(part.MountRoot, 0755)
    if err != nil { return err }
    _, err = self.Linuxutil.Mount(ctx, part.FsUuid, part.MountRoot)
    if err != nil { return err }
    err = os.MkdirAll(MetaDir(part), 0755)
    if err != nil { return err }
    err = os.MkdirAll(StoreDir(part), 0755)
    if err != nil { return err }
  }
  return nil
}

func (self *RoundRobinMetadata) UMountAllSinkPartitions(ctx context.Context) error {
  for _,part := range self.Sink.Partitions {
    err := self.Linuxutil.UMount(ctx, part.FsUuid)
    if err != nil { return err }
  }
  return nil
}

func modTime(finfo os.FileInfo) time.Time {
  if finfo == nil {
    oldest_ts := time.Time{}
    if !oldest_ts.IsZero() { util.Fatalf("Failed to get zero time") }
    return oldest_ts
  }
  return finfo.ModTime()
}

func (self *RoundRobinMetadata) FindOldestPartition() (*pb.Backup_Partition, error) {
  var oldest_part *pb.Backup_Partition
  oldest_ts := time.Now()

  for _,part := range self.Sink.Partitions {
    link := SymLink(part)
    finfo, err := os.Stat(link) // does follow symlinks
    if err != nil && !util.IsNotExist(err) { return nil, err }

    part_time := modTime(finfo)
    if part_time.Before(oldest_ts) || part_time.Equal(oldest_ts) {
      oldest_part = part
      oldest_ts = part_time
    }
  }
  if oldest_part == nil { util.Fatalf("Failed to get oldest partition") }
  return oldest_part, nil
}

func (self *RoundRobinMetadata) SetupMetadata(ctx context.Context) error {
  err := self.MountAllSinkPartitions(ctx)
  if err != nil { return err }
  part, err := self.FindOldestPartition()
  if err != nil { return err }

  if self.SimpleDirMetadata == nil {
    meta, err := NewSimpleDirMetadataAdmin(ctx, self.Conf, part.FsUuid)
    if err != nil { return err }
    err = meta.SetupMetadata(ctx)
    if err == nil { self.SimpleDirMetadata = meta.(*SimpleDirMetadata) }
    return err
  }
  return nil
}

// Storage should always be tied to the same metadata partition.
// Caller should call `SetupBackupContent` afterwards.
func (self *RoundRobinMetadata) GetPairStorage(codec types.Codec) (types.AdminBackupContent, error) {
  if self.SimpleDirMetadata == nil { util.Fatalf("Call SetupMetadata first") }
  if self.PairStorage != nil { return self.PairStorage, nil }
  fs_uuid := self.DirInfo.FsUuid
  var err error
  self.PairStorage, err = NewSimpleDirStorageAdmin(self.Conf, codec, fs_uuid)
  return self.PairStorage, err
}

