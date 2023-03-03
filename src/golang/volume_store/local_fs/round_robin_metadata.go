package local_fs

import (
  "context"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
)

type RoundRobinMetadata struct {
  *SimpleDirMetadata
  *RoundRobinSetup   // shadows the Conf field on the base object
}

// Does not initialize inner SimpleDirMetadata because filesystem may not be mounted yet.
func NewRoundRobinMetadataAdmin(
    conf *pb.Config, lu types.Linuxutil, backup_name string) (types.AdminMetadata, error) {
  setup, err := NewRoundRobinSetup(conf, lu, backup_name)
  metadata := &RoundRobinMetadata{
    SimpleDirMetadata: nil,
    RoundRobinSetup: setup,
  }
  return metadata, err
}

func NewRoundRobinMetadata(
    conf *pb.Config, lu types.Linuxutil, backup_name string) (types.Metadata, error) {
  return NewRoundRobinMetadataAdmin(conf, lu, backup_name)
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

