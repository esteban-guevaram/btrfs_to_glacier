package local_fs_metadata

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
  Sink        *pb.LocalFs_RoundRobin
  Conf        *pb.Config
  Linuxutil   types.Linuxutil
  PairStorage types.AdminStorage
}

// Does not initialize inner SimpleDirMetadata because filesystem may not be mounted yet.
func NewRoundRobinMetadataAdmin(
    ctx context.Context, conf *pb.Config, lu types.Linuxutil, sink_name string) (types.AdminMetadata, error) {
  var sink *pb.LocalFs_RoundRobin
  for _,s := range conf.LocalFs.Sinks {
    if s.Name != sink_name { continue }
    sink = s
  }
  if sink == nil {
    return nil, fmt.Errorf("Sink '%s' not found", sink_name)
  }
  if len(sink.Partitions) < 1 {
    return nil, fmt.Errorf("Sink '%s' does not contain any partition", sink_name)
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

func NewRoundRobinMetadata(
    ctx context.Context, conf *pb.Config, lu types.Linuxutil, sink_name string) (types.Metadata, error) {
  return NewRoundRobinMetadataAdmin(ctx, conf, lu, sink_name)
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

func (self *RoundRobinMetadata) FindOldestPartition() (*pb.LocalFs_Partition, error) {
  var oldest_part *pb.LocalFs_Partition
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

func (self *RoundRobinMetadata) SetupMetadata(ctx context.Context) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    err := self.MountAllSinkPartitions(ctx)
    if err != nil { done <- err; return }
    part, err := self.FindOldestPartition()
    if err != nil { done <- err; return }

    if self.SimpleDirMetadata == nil {
      meta, err := NewSimpleDirMetadataAdmin(ctx, self.Conf, part.FsUuid)
      if err != nil { done <- err; return }
      select {
        case err = <-meta.SetupMetadata(ctx):
          self.SimpleDirMetadata = meta.(*SimpleDirMetadata)
          done <- err
        case <-ctx.Done(): done <- ctx.Err()
      }
      return
    }
    done <- nil
  }()
  return done
}

// Storage should always be tied to the same metadata partition.
// Caller should call `SetupStorage` afterwards.
func (self *RoundRobinMetadata) GetPairStorage(codec types.Codec) (types.AdminStorage, error) {
  if self.SimpleDirMetadata == nil { util.Fatalf("Call SetupMetadata first") }
  if self.PairStorage != nil { return self.PairStorage, nil }
  fs_uuid := self.DirInfo.FsUuid
  var err error
  self.PairStorage, err = NewSimpleDirStorageAdmin(self.Conf, codec, fs_uuid)
  return self.PairStorage, err
}

