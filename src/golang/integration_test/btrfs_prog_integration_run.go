package main

import (
  "btrfs_to_glacier/log"
  "btrfs_to_glacier/shim"
  "flag"
)

var path_flag string

func init() {
  flag.StringVar(&path_flag, "subvol", "", "the fullpath to the btrfs subvolume")
}

// Cannot use a test since Testing does not support cgo
func main() {
  log.Infof("btrfs_prog_integration_run")
  flag.Parse()

  btrfsutil := shim.New()
  subvol, err := btrfsutil.SubvolumeInfo(path_flag, 0);
  if err != nil {
    log.Fatalf("integration failed = %v", err)
  }
  log.Infof("uuid = %s\n", subvol.Uuid())
}
