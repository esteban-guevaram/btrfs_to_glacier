package main

import (
  "context"
  fpmod "path/filepath"

  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

type TestFilesystemUtil struct {
  linuxutil *shim.Linuxutil
  mnt_path_1 string
  mnt_path_2 string
  mnt_path_3 string
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

func (self *TestFilesystemUtil) TestListMount_Noop() {
  var noop_mnt *types.MountEntry
  mnt_list, err := self.linuxutil.ListBlockDevMounts()
  if err != nil { util.Fatalf("ListBlockDevMounts: %v", err) }

  for _,mnt := range mnt_list {
    if mnt.MountedPath != self.mnt_path_1 { continue }
    noop_mnt = mnt
  }
  if noop_mnt == nil { util.Fatalf("Did not find %s", self.mnt_path_1) }

  got_mnt, err := self.linuxutil.Mount(context.TODO(), noop_mnt.Device.FsUuid, self.mnt_path_1)
  if err != nil { util.Fatalf("linuxutil.Mount: %v", err) }
  util.Debugf("entry: %s", util.AsJson(got_mnt))
  util.EqualsOrDie("Bad mount entry", got_mnt, noop_mnt)
}

func (self *TestFilesystemUtil) TestListMount_NoSuchDev() {
  _, err := self.linuxutil.Mount(context.TODO(), "bad_uuid", "/some/path")
  util.Debugf("no such dev err: %v", err)
  if err == nil { util.Fatalf("linuxutil.Mount: expected error for unexisting device") }
}

func (self *TestFilesystemUtil) TestListUMount_NoSuchDev() {
  err := self.linuxutil.UMount(context.TODO(), "bad_uuid")
  if err != nil { util.Fatalf("linuxutil.UMount: %v", err) }
}

func (self *TestFilesystemUtil) TestListUMount_Mount_Cycle() {
  var expect_mnt *types.MountEntry
  mnt_list, err := self.linuxutil.ListBlockDevMounts()
  if err != nil { util.Fatalf("ListBlockDevMounts: %v", err) }

  for _,mnt := range mnt_list {
    if mnt.MountedPath != self.mnt_path_3 { continue }
    expect_mnt = mnt
  }
  if expect_mnt == nil { util.Fatalf("Did not find %s", self.mnt_path_3) }

  err = self.linuxutil.UMount(context.TODO(), expect_mnt.Device.FsUuid)
  if err != nil { util.Fatalf("linuxutil.UMount: %v", err) }

  got_mnt, err := self.linuxutil.Mount(context.TODO(), expect_mnt.Device.FsUuid, self.mnt_path_3)
  if err != nil { util.Fatalf("linuxutil.Mount: %v", err) }
  util.Debugf("entry: %s", util.AsJson(got_mnt))
  util.EqualsOrDie("Bad mount entry", got_mnt, expect_mnt)
}

func TestFilesystemUtil_AllFuncs(
    linuxutil types.Linuxutil, src_fs string, dest_fs string) {
  suite := &TestFilesystemUtil{
    linuxutil: linuxutil.(*shim.Linuxutil),
    mnt_path_1: src_fs,
    mnt_path_2: dest_fs,
    mnt_path_3: "/tmp/ext4_test_partition_1",
  }
  suite.TestListBtrfsFilesystems()
  suite.TestListBlockDevMounts()
  suite.TestListMount_Noop()
  suite.TestListMount_NoSuchDev()
  suite.TestListUMount_NoSuchDev()
  // Special snowflake test, do not run automatically
  //suite.TestListUMount_Mount_Cycle()
}

