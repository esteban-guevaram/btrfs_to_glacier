package main

import (
  "flag"
  "fmt"
  fpmod "path/filepath"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

var path_flag string
var root_flag string
var snap1_flag string
var snap2_flag string

func init() {
  flag.StringVar(&path_flag, "subvol",  "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&root_flag, "rootvol", "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&snap1_flag, "snap1",  "", "the fullpath to the oldest snapshot")
  flag.StringVar(&snap2_flag, "snap2",  "", "the fullpath to the latest snapshot")
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

func GetLinuxUtil() types.Linuxutil {
  var conf *pb.Config
  linuxutil, err := shim.NewLinuxutil(conf)
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  return linuxutil
}

func GetNewSnapName(leaf_name string) string {
  return fpmod.Join(fpmod.Dir(snap1_flag),
                    fmt.Sprintf("%s.%d", leaf_name, time.Now().Unix()))
}

func TestBtrfsUtil_SubvolumeInfo(btrfsutil types.Btrfsutil) {
  subvol, err := btrfsutil.SubvolumeInfo(path_flag);
  if err != nil { util.Fatalf("integration failed = %v", err) }
  util.Infof("subvol = %s\n", subvol)
}

func TestBtrfsUtil_ListSubVolumesUnder(btrfsutil types.Btrfsutil) {
  var err error
  var vols []*pb.Snapshot
  vols, err = btrfsutil.ListSubVolumesUnder(root_flag)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  for _,subvol := range(vols) { util.Infof("subvol = %s\n", subvol) }
  util.Infof("len(vols) = %d\n", len(vols))
}

func TestBtrfsUtil_CreateSnapshot(btrfsutil types.Btrfsutil) {
  snap_path := GetNewSnapName("TestBtrfsUtil_CreateSnapshotAndWait")
  err := btrfsutil.CreateSnapshot(path_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", path_flag, snap_path, err) }

  subvol, err := btrfsutil.SubvolumeInfo(snap_path);
  if err != nil { util.Fatalf("btrfsutil.SubvolumeInfo failed = %v", err) }
  util.Infof("subvol = %s\n", subvol)
}

// Requires CAP_SYS_ADMIN
func TestBtrfsUtil_DeleteSubvolume(linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("TestBtrfsUtil_DeleteSubvolume needs CAP_SYS_ADMIN")
    return
  }
  snap_path := GetNewSnapName("TestBtrfsUtil_DeleteSubvolume")
  err := btrfsutil.CreateSnapshot(path_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", path_flag, snap_path, err) }

  err = btrfsutil.DeleteSubvolume(snap_path);
  if err != nil { util.Fatalf("btrfsutil.DeleteSubvolume(%s) failed = %v", snap_path, err) }

  var subvol *pb.Snapshot
  subvol, err = btrfsutil.SubvolumeInfo(snap_path);
  if err == nil { util.Fatalf("btrfsutil.DeleteSubvolume was not deleted: %v", subvol) }
}

func TestBtrfsUtil_AllFuncs(linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  TestBtrfsUtil_SubvolumeInfo(btrfsutil)
  TestBtrfsUtil_ListSubVolumesUnder(btrfsutil)
  TestBtrfsUtil_CreateSnapshot(btrfsutil)
  TestBtrfsUtil_DeleteSubvolume(linuxutil, btrfsutil)
}

func TestLinuxUtils_AllFuncs(linuxutil types.Linuxutil) {
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
  linuxutil := GetLinuxUtil()
  TestBtrfsUtil_AllFuncs(linuxutil, btrfsutil)
  TestLinuxUtils_AllFuncs(linuxutil)
  TestSendDumpAll(btrfsutil)
  TestBtrfsSendStreamAll(linuxutil, btrfsutil)
  util.Infof("ALL DONE")
}

