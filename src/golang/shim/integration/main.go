package main

import (
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "flag"
)

var path_flag string
var root_flag string

func init() {
  flag.StringVar(&path_flag, "subvol", "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&root_flag, "rootvol", "", "the fullpath to the btrfs subvolume")
}

func GetBtrfsUtil() types.Btrfsutil {
  var config *pb.Config
  var err error
  var btrfsutil types.Btrfsutil
  config, err = util.Load()
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  btrfsutil, err = shim.NewBtrfsutil(config)
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  return btrfsutil
}

func TestBtrfsUtil_SubvolumeInfo(btrfsutil types.Btrfsutil) {
  subvol, err := btrfsutil.SubvolumeInfo(path_flag);
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  util.Infof("subvol = %s\n", subvol)
}

func TestBtrfsUtil_ListSubVolumesUnder(btrfsutil types.Btrfsutil) {
  var err error
  var vols []*pb.Snapshot
  vols, err = btrfsutil.ListSubVolumesUnder(root_flag);
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  for _,subvol := range(vols) { util.Infof("subvol = %s\n", subvol) }
  util.Infof("len(vols) = %d\n", len(vols))
}

func TestLinuxUtils_AllFuncs() {
  var conf *pb.Config
  linuxutil, _ := shim.NewLinuxutil(conf)
  util.Infof("IsCapSysAdmin = %v", linuxutil.IsCapSysAdmin())
  kmaj, kmin := linuxutil.LinuxKernelVersion()
  util.Infof("LinuxKernelVersion = %d.%d", kmaj, kmin)
  bmaj, bmin := linuxutil.BtrfsProgsVersion()
  util.Infof("BtrfsProgsVersion = %d.%d", bmaj, bmin)
  util.Infof("ProjectVersion = %s", linuxutil.ProjectVersion())
}

// Cannot use a test since Testing does not support cgo
func main() {
  util.Infof("btrfs_prog_integration_run")
  flag.Parse()
  btrfsutil := GetBtrfsUtil()
  TestBtrfsUtil_SubvolumeInfo(btrfsutil)
  TestBtrfsUtil_ListSubVolumesUnder(btrfsutil)
  TestLinuxUtils_AllFuncs()
}

