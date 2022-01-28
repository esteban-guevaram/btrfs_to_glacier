package main

import (
  fpmod "path/filepath"

  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

type TestFilesystemUtil struct {
  linuxutil *shim.Linuxutil
  mnt_path_1 string
  mnt_path_2 string
}

func (self *TestFilesystemUtil) CheckBtrfsFs(fs *types.Filesystem, mnt_path string) bool {
   var found_path bool
   if fs.Label != fpmod.Base(mnt_path) { return false }
   for _,mnt := range fs.Mounts { found_path = found_path || mnt.MountedPath == mnt_path }
   if !found_path { util.Fatalf("Did not find expected mount: '%s'", mnt_path) }
   return true
}

func (self *TestFilesystemUtil) TestListBtrfsFilesystems() {
  var found_fs_1, found_fs_2 bool
  fs_list, err := self.linuxutil.ListBtrfsFilesystems()
  if err != nil { util.Fatalf("cannot get filesystems: %v", err) }
  if len(fs_list) < 1 { util.Fatalf("failed to find any btrfs fs") }

  for _,fs := range fs_list {
    if len(fs.Uuid) < 1 { util.Fatalf("bad uuid") }
    if len(fs.Label) < 1 { util.Fatalf("bad label") }
    if len(fs.Devices) < 1 { util.Fatalf("no devices found") }
    if len(fs.Mounts) < 1 { util.Fatalf("no mounts found") }
    for _,mnt := range fs.Mounts {
      if mnt.BtrfsVolId < 1 { util.Fatalf("bad btrfs subvol id") }
      if len(mnt.MountedPath) < 1 { util.Fatalf("bad btrfs mounted path") }
    }

    found_fs_1 = found_fs_1 || self.CheckBtrfsFs(fs, self.mnt_path_1)
    found_fs_2 = found_fs_2 || self.CheckBtrfsFs(fs, self.mnt_path_2)
  }

  util.Debugf("fs_list: %s", util.AsJson(fs_list))
  if !found_fs_1 || !found_fs_2 { util.Fatalf("Did not find all expected filesystems") }
}

func (self *TestFilesystemUtil) TestListBlockDevMounts() {
  var found_mnt_1, found_mnt_2 bool
  mnt_list, err := self.linuxutil.ListBlockDevMounts()
  if err != nil { util.Fatalf("ListBlockDevMounts: %v", err) }

  for _,mnt := range mnt_list {
    dev := mnt.Device
    if dev == nil || len(mnt.MountedPath) < 1 {
      util.Fatalf("malformed mount: %v", mnt)
    }
    if len(dev.Name) < 1 || len(dev.FsUuid) < 1 {
      util.Fatalf("malformed block device: %v", dev)
    }

    found_mnt_1 = found_mnt_1 || mnt.MountedPath == self.mnt_path_1
    found_mnt_2 = found_mnt_2 || mnt.MountedPath == self.mnt_path_2
  }
  util.Debugf("mnt_list: %s", util.AsJson(mnt_list))
  if !found_mnt_1 || !found_mnt_2 { util.Fatalf("Did not find all expected mounts") }
}

func TestFilesystemUtil_AllFuncs(
    linuxutil types.Linuxutil, src_fs string, dest_fs string) {
  suite := &TestFilesystemUtil{
    linuxutil: linuxutil.(*shim.Linuxutil),
    mnt_path_1: src_fs,
    mnt_path_2: dest_fs,
  }
  suite.TestListBtrfsFilesystems()
  suite.TestListBlockDevMounts()
}

