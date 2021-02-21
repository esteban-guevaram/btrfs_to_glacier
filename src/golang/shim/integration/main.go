package main

import (
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/shim"
  "flag"
)

var path_flag string

func init() {
  flag.StringVar(&path_flag, "subvol", "", "the fullpath to the btrfs subvolume")
}

// Cannot use a test since Testing does not support cgo
func main() {
  util.Infof("btrfs_prog_integration_run")
  flag.Parse()

  btrfsutil, err := shim.NewBtrfsutilShim()
  subvol, err := btrfsutil.SubvolumeInfo(path_flag, 0);
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  util.Infof("uuid = %s\n", subvol.Uuid())
}

