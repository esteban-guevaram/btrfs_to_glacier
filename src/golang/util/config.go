package util

import (
  "errors"
  "fmt"

  pb "btrfs_to_glacier/messages"
)

var ErrNotFoundByName = errors.New("not_found_by_name")

func Load() (*pb.Config, error) {
  conf := &pb.Config{}
  // TODO Merge default values
  err := Validate(conf)
  return conf, err
}

func BackupByName(conf *pb.Config, name string) (*pb.Backup, error) {
  for _,b := range conf.Backups {
    if b.Name == name { return b, nil }
  }
  return nil, fmt.Errorf("%w %s", ErrNotFoundByName, name)
}

func BackupAwsByName(conf *pb.Config, name string) (*pb.Backup_Aws, error) {
  for _,b := range conf.Backups {
    if b.Name == name && b.Aws != nil { return b.Aws, nil }
  }
  return nil, fmt.Errorf("%w %s", ErrNotFoundByName, name)
}

func BackupPartitionByUuid(conf *pb.Config, name string) (*pb.Backup_Partition, error) {
  for _,b := range conf.Backups {
    if b.Fs == nil { continue }
    for _,rr := range b.Fs.Sinks {
      for _,p := range rr.Partitions {
        if p.FsUuid == name { return p, nil }
      }
    }
  }
  return nil, fmt.Errorf("%w %s", ErrNotFoundByName, name)
}

func Validate(conf *pb.Config) error {
  // Each subvolume should be mentioned in only 1 source
  // Sources, Stores, Restores, Workflows, Tools have a unique name
  // Reference by name not dangling
  return nil
}

