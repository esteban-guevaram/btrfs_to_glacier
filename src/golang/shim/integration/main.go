package main

import (
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "flag"
)

var path_flag string

func init() {
  flag.StringVar(&path_flag, "subvol", "", "the fullpath to the btrfs subvolume")
}

func TestBtrfsUtil_SubvolumeInfo() {
  var config types.Config
  var err error
  var btrfsutil types.Btrfsutil
  var subvol *pb.SubVolume
  config, err = util.Load()
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  btrfsutil, err = shim.NewBtrfsutil(config)
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  subvol, err = btrfsutil.SubvolumeInfo(path_flag, 0);
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  util.Infof("subvol = %s\n", subvol)
}

func TestLinuxUtils_AllFuncs() {
  var conf types.Config
  linuxutil, _ := shim.NewLinuxutil(conf)
  util.Infof("IsCapSysAdmin = %v", linuxutil.IsCapSysAdmin())
  kmaj, kmin := linuxutil.LinuxKernelVersion()
  util.Infof("LinuxKernelVersion = %d.%d", kmaj, kmin)
  bmaj, bmin := linuxutil.BtrfsProgsVersion()
  util.Infof("LinuxKernelVersion = %d.%d", bmaj, bmin)
}

// Cannot use a test since Testing does not support cgo
func main() {
  util.Infof("btrfs_prog_integration_run")
  flag.Parse()
  TestBtrfsUtil_SubvolumeInfo()
  TestLinuxUtils_AllFuncs()
}

