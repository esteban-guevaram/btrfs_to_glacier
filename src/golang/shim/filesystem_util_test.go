package shim

import (
  "context"
  "io/fs"
  "os"
  "testing"

  "btrfs_to_glacier/util"
)

func buildFilesystemUtil(t *testing.T) (*FilesystemUtil, *SysUtilMock) {
  sys_util := &SysUtilMock{
    FileContent: make(map[string]string),
    DirContent: make(map[string][]os.DirEntry),
    LinkTarget: make(map[string]string),
  }
  lu := &FilesystemUtil{ SysUtil:sys_util, }
  return lu, sys_util
}

func TestListMounts(t *testing.T) {
  lu,fs_reader := buildFilesystemUtil(t)
  fs_reader.FileContent["/proc/self/mountinfo"] = `
22 61 0:21   /           /proc            rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw
23 61 0:22   /           /sys             rw,nosuid,nodev,noexec,relatime shared:2 - sysfs sysfs rw
24 61 0:5    /           /dev             rw,nosuid shared:8 - devtmpfs devtmpfs rw

61 1 259:3   /           /                rw,noatime,nodiratime shared:1 - ext4 /dev/nvme0n1p1 rw
93 61 254:0  /           /media/some_fs_a           rw shared:47 - ext4 /dev/mapper/mapper-group rw
99 86 254:0  /Bind_dm-0  /home/host_user/Bind_dm-0  rw shared:47 - ext4 /dev/mapper/mapper-group rw
105 61 8:3   /           /media/some_fs_b           rw shared:52 - ext4 /dev/sda3 rw
108 61 8:4   /           /media/some_fs_c           rw shared:54 - ext4 /dev/sda4 rw
111 86 8:3   /Bind_sda3  /home/host_user/Bind_sda3  rw shared:52 - ext4 /dev/sda3 rw
114 86 8:4   /Bind_sda4  /home/host_user/Bind_sda4  rw shared:54 - ext4 /dev/sda4 rw
468 37 0:44  /           /tmp/btrfs_mnt_1           rw shared:245 - btrfs /dev/loop111p1 rw
483 37 0:47  /           /tmp/btrfs_mnt_2           rw shared:253 - btrfs /dev/loop111p2 rw
498 37 0:44  /asubvol    /tmp/btrfs_mnt_3/asubvol   rw shared:261 - btrfs /dev/loop111p1 rw
`
  fs_reader.DirContent["/dev/disk/by-partuuid"] = []os.DirEntry{
    &DirEntry{ Leaf:"gpt-uuid-sda1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-sda2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-sda3", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-sda4", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-nvme0n1p1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-nvme0n1p2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-nvme0n1p3", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-nvme0n1p4", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-nvme1n1p1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-loop111p1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"gpt-uuid-loop111p2", Mode:fs.ModeSymlink, },
  }
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-sda1"] = "/dev/sda1"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-sda2"] = "/dev/sda2"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-sda3"] = "/dev/sda3"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-sda4"] = "/dev/sda4"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-nvme0n1p1"] = "/dev/nvme0n1p1"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-nvme0n1p2"] = "/dev/nvme0n1p2"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-nvme0n1p3"] = "/dev/nvme0n1p3"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-nvme0n1p4"] = "/dev/nvme0n1p4"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-nvme1n1p1"] = "/dev/nvme1n1p1"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-loop111p1"] = "/dev/loop111p1"
  fs_reader.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-loop111p2"] = "/dev/loop111p2"

  fs_reader.DirContent["/dev/disk/by-uuid"] = []os.DirEntry{
    &DirEntry{ Leaf:"fs-uuid-sda1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-sda2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-sda3", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-sda4", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-nvme0n1p1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-nvme0n1p2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-nvme0n1p4", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-loop111p1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-loop111p2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"fs-uuid-dm-0", Mode:fs.ModeSymlink, },
  }
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-sda1"] = "/dev/sda1"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-sda2"] = "/dev/sda2"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-sda3"] = "/dev/sda3"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-sda4"] = "/dev/sda4"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-nvme0n1p1"] = "/dev/nvme0n1p1"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-nvme0n1p2"] = "/dev/nvme0n1p2"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-nvme0n1p4"] = "/dev/nvme0n1p4"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-loop111p1"] = "/dev/loop111p1"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-loop111p2"] = "/dev/loop111p2"
  fs_reader.LinkTarget["/dev/disk/by-uuid/fs-uuid-dm-0"] = "/dev/dm-0"

  fs_reader.DirContent["/dev/block"] = []os.DirEntry{
    &DirEntry{ Leaf:"254:0", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:0", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:1", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:2", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:3", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:5", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:6", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:7", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"259:8", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:0",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:1",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:111", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:2",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:3",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:4",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:5",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:6",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"7:7",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"8:0",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"8:1",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"8:2",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"8:3",   Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"8:4",   Mode:fs.ModeSymlink, },
  }
  fs_reader.LinkTarget["/dev/block/254:0"] = "/dev/dm-0"
  fs_reader.LinkTarget["/dev/block/259:0"] = "/dev/nvme1n1"
  fs_reader.LinkTarget["/dev/block/259:1"] = "/dev/nvme0n1"
  fs_reader.LinkTarget["/dev/block/259:2"] = "/dev/nvme1n1p1"
  fs_reader.LinkTarget["/dev/block/259:3"] = "/dev/nvme0n1p1"
  fs_reader.LinkTarget["/dev/block/259:4"] = "/dev/nvme0n1p2"
  fs_reader.LinkTarget["/dev/block/259:5"] = "/dev/nvme0n1p3"
  fs_reader.LinkTarget["/dev/block/259:6"] = "/dev/nvme0n1p4"
  fs_reader.LinkTarget["/dev/block/259:7"] = "/dev/loop111p1"
  fs_reader.LinkTarget["/dev/block/259:8"] = "/dev/loop111p2"
  fs_reader.LinkTarget["/dev/block/7:0"]   = "/dev/loop0"
  fs_reader.LinkTarget["/dev/block/7:1"]   = "/dev/loop1"
  fs_reader.LinkTarget["/dev/block/7:111"] = "/dev/loop111"
  fs_reader.LinkTarget["/dev/block/7:2"]   = "/dev/loop2"
  fs_reader.LinkTarget["/dev/block/7:3"]   = "/dev/loop3"
  fs_reader.LinkTarget["/dev/block/7:4"]   = "/dev/loop4"
  fs_reader.LinkTarget["/dev/block/7:5"]   = "/dev/loop5"
  fs_reader.LinkTarget["/dev/block/7:6"]   = "/dev/loop6"
  fs_reader.LinkTarget["/dev/block/7:7"]   = "/dev/loop7"
  fs_reader.LinkTarget["/dev/block/8:0"]   = "/dev/sda"
  fs_reader.LinkTarget["/dev/block/8:1"]   = "/dev/sda1"
  fs_reader.LinkTarget["/dev/block/8:2"]   = "/dev/sda2"
  fs_reader.LinkTarget["/dev/block/8:3"]   = "/dev/sda3"
  fs_reader.LinkTarget["/dev/block/8:4"]   = "/dev/sda4"

  fs_reader.DirContent["/dev/mapper"] = []os.DirEntry{
    &DirEntry{ Leaf:"control", Mode:fs.ModeDir, },
    &DirEntry{ Leaf:"mapper-group", Mode:fs.ModeSymlink, },
  }
  fs_reader.LinkTarget["/dev/mapper/mapper-group"] = "/dev/dm-0"

  fs_reader.DirContent["/sys/block"] = []os.DirEntry{
    &DirEntry{ Leaf:"sda", Mode:fs.ModeSymlink, },
    &DirEntry{ Leaf:"loop111", Mode:fs.ModeSymlink, },
  }
  fs_reader.DirContent["/sys/block/loop111"] = []os.DirEntry{
    &DirEntry{ Leaf:"loop111p1", Mode:fs.ModeDir, },
    &DirEntry{ Leaf:"loop111p2", Mode:fs.ModeDir, },
  }
  fs_reader.DirContent["/sys/block/loop111/loop"] = []os.DirEntry{
    &DirEntry{ Leaf:"backing_file", Mode:0, },
  }
  fs_reader.FileContent["/sys/block/loop111/loop/backing_file"] = "/tmp/loopfile"

  expected := `[
  {
    "Id": 61,
    "Device": {
      "Name": "nvme0n1p1",
      "MapperGroup": "",
      "Minor": 3,
      "Major": 259,
      "FsUuid": "fs-uuid-nvme0n1p1",
      "GptUuid": "gpt-uuid-nvme0n1p1",
      "LoopFile": ""
    },
    "TreePath": "",
    "MountedPath": "/",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 93,
    "Device": {
      "Name": "dm-0",
      "MapperGroup": "mapper-group",
      "Minor": 0,
      "Major": 254,
      "FsUuid": "fs-uuid-dm-0",
      "GptUuid": "",
      "LoopFile": ""
    },
    "TreePath": "",
    "MountedPath": "/media/some_fs_a",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 99,
    "Device": {
      "Name": "dm-0",
      "MapperGroup": "mapper-group",
      "Minor": 0,
      "Major": 254,
      "FsUuid": "fs-uuid-dm-0",
      "GptUuid": "",
      "LoopFile": ""
    },
    "TreePath": "Bind_dm-0",
    "MountedPath": "/home/host_user/Bind_dm-0",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 105,
    "Device": {
      "Name": "sda3",
      "MapperGroup": "",
      "Minor": 3,
      "Major": 8,
      "FsUuid": "fs-uuid-sda3",
      "GptUuid": "gpt-uuid-sda3",
      "LoopFile": ""
    },
    "TreePath": "",
    "MountedPath": "/media/some_fs_b",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 108,
    "Device": {
      "Name": "sda4",
      "MapperGroup": "",
      "Minor": 4,
      "Major": 8,
      "FsUuid": "fs-uuid-sda4",
      "GptUuid": "gpt-uuid-sda4",
      "LoopFile": ""
    },
    "TreePath": "",
    "MountedPath": "/media/some_fs_c",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 111,
    "Device": {
      "Name": "sda3",
      "MapperGroup": "",
      "Minor": 3,
      "Major": 8,
      "FsUuid": "fs-uuid-sda3",
      "GptUuid": "gpt-uuid-sda3",
      "LoopFile": ""
    },
    "TreePath": "Bind_sda3",
    "MountedPath": "/home/host_user/Bind_sda3",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 114,
    "Device": {
      "Name": "sda4",
      "MapperGroup": "",
      "Minor": 4,
      "Major": 8,
      "FsUuid": "fs-uuid-sda4",
      "GptUuid": "gpt-uuid-sda4",
      "LoopFile": ""
    },
    "TreePath": "Bind_sda4",
    "MountedPath": "/home/host_user/Bind_sda4",
    "FsType": "ext4",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 468,
    "Device": {
      "Name": "loop111p1",
      "MapperGroup": "",
      "Minor": 7,
      "Major": 259,
      "FsUuid": "fs-uuid-loop111p1",
      "GptUuid": "gpt-uuid-loop111p1",
      "LoopFile": "/tmp/loopfile"
    },
    "TreePath": "",
    "MountedPath": "/tmp/btrfs_mnt_1",
    "FsType": "btrfs",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 483,
    "Device": {
      "Name": "loop111p2",
      "MapperGroup": "",
      "Minor": 8,
      "Major": 259,
      "FsUuid": "fs-uuid-loop111p2",
      "GptUuid": "gpt-uuid-loop111p2",
      "LoopFile": "/tmp/loopfile"
    },
    "TreePath": "",
    "MountedPath": "/tmp/btrfs_mnt_2",
    "FsType": "btrfs",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  },
  {
    "Id": 498,
    "Device": {
      "Name": "loop111p1",
      "MapperGroup": "",
      "Minor": 7,
      "Major": 259,
      "FsUuid": "fs-uuid-loop111p1",
      "GptUuid": "gpt-uuid-loop111p1",
      "LoopFile": "/tmp/loopfile"
    },
    "TreePath": "asubvol",
    "MountedPath": "/tmp/btrfs_mnt_3/asubvol",
    "FsType": "btrfs",
    "Options": {
      "rw": ""
    },
    "BtrfsVolId": 0,
    "Binds": null
  }
]`
  mnt_list, err := lu.ListBlockDevMounts()
  if err != nil { t.Fatalf("ListMounts: %v", err) }
  util.Debugf("mnt_list: %v", util.AsJson(mnt_list))
  diff_line := util.DiffLines(mnt_list, expected)
  if len(diff_line) > 0 { t.Errorf(diff_line) }
}

func TestMount_Simple(t *testing.T) {
  lu,sys_util := buildFilesystemUtil(t)
  fs_uuid := "fs_uuid"
  target := "/some/path"
  sys_util.AddMount(fs_uuid, "sda666", target)
  mnt, err := lu.Mount(context.TODO(), fs_uuid, target)

  if err != nil { t.Errorf("Mount err: %v", err) }
  if mnt.Device.FsUuid != fs_uuid || mnt.MountedPath != target {
    t.Errorf("Bad mount: %v", mnt)
  }
}

func TestMount_NotMounted(t *testing.T) {
  lu,_ := buildFilesystemUtil(t)
  fs_uuid := "fs_uuid"
  target := "/some/path"
  _, err := lu.Mount(context.TODO(), fs_uuid, target)

  if err == nil { t.Errorf("Expected error if device was not mounted") }
}

func TestMount_MountBadPath(t *testing.T) {
  lu,sys_util := buildFilesystemUtil(t)
  fs_uuid := "fs_uuid"
  target := "/some/path"
  sys_util.AddMount(fs_uuid, "sda666", target)
  _, err := lu.Mount(context.TODO(), fs_uuid, target+"bad")

  if err == nil { t.Errorf("Expected error if device was mounted at wrong path") }
}

func TestUMount_Simple(t *testing.T) {
  lu,sys_util := buildFilesystemUtil(t)
  sys_util.AddMount("fs_uuid", "sda666", "/some/path")
  err := lu.UMount(context.TODO(), "another_uuid")

  if err != nil { t.Errorf("UMount err: %v", err) }
}

func TestUMount_StillMounted(t *testing.T) {
  lu,sys_util := buildFilesystemUtil(t)
  fs_uuid := "fs_uuid"
  target := "/some/path"
  sys_util.AddMount(fs_uuid, "sda666", target)
  err := lu.UMount(context.TODO(), fs_uuid)

  if err == nil { t.Errorf("Expected error if device is still mounted") }
}

