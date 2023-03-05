package local_fs

import (
  "context"
  "fmt"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
)

type RoundRobinContent struct {
  *SimpleDirStorage
  *RoundRobinSetup   // shadows the Conf field on the base object
  Codec types.Codec
}

// Does not initialize inner SimpleDirStorage because filesystem may not be mounted yet.
func NewRoundRobinContentAdmin(
    conf *pb.Config, lu types.Linuxutil, codec types.Codec, backup_name string) (types.AdminBackupContent, error) {
  setup, err := NewRoundRobinSetup(conf, lu, backup_name)
  content := &RoundRobinContent{
    SimpleDirStorage: nil,
    RoundRobinSetup: setup,
    Codec: codec,
  }
  return content, err
}

func NewRoundRobinContent(
    conf *pb.Config, lu types.Linuxutil, codec types.Codec, backup_name string) (types.BackupContent, error) {
  return NewRoundRobinContentAdmin(conf, lu, codec, backup_name)
}

func (self *RoundRobinContent) SetupBackupContent(ctx context.Context) error {
  err := self.MountAllSinkPartitions(ctx)
  if err != nil { return err }
  part, err := self.FindOldestPartition()
  if err != nil { return err }

  if self.SimpleDirStorage == nil {
    content, err := NewSimpleDirStorageAdmin(self.Conf, self.Codec, part.FsUuid)
    if err != nil { return err }
    err = content.SetupBackupContent(ctx)
    if err == nil { self.SimpleDirStorage = content.(*SimpleDirStorage) }
    return err
  }
  return nil
}

func (self *RoundRobinContent) TearDownBackupContent(ctx context.Context) error {
  if self.SimpleDirStorage == nil {
    return fmt.Errorf("TearDownBackupContent: did not call setup before")
  }
  return self.UMountAllSinkPartitions(ctx)
}

