package shim

import (
  "io/fs"
  "os"
  "testing"

  "btrfs_to_glacier/util"
)

func buildFilesystemUtil(t *testing.T) (*FilesystemUtil, *FsReaderMock) {
  fs_reader := &FsReaderMock{
    FileContent: make(map[string]string),
    DirContent: make(map[string][]os.DirEntry),
    LinkTarget: make(map[string]string),
  }
  lu := &FilesystemUtil{ FsReader:fs_reader, }
  return lu, fs_reader
}

// 25 23 0:6 / /sys/kernel/security rw,nosuid,nodev,noexec,relatime shared:3 - securityfs securityfs rw
// 26 24 0:23 / /dev/shm rw,nosuid,nodev shared:9 - tmpfs tmpfs rw,size=16415840k,nr_inodes=4103960,inode64
// 27 24 0:24 / /dev/pts rw,nosuid,noexec,relatime shared:10 - devpts devpts rw,gid=5,mode=620,ptmxmode=000
// 28 61 0:25 / /run rw,nosuid,nodev shared:11 - tmpfs tmpfs rw,size=6566336k,nr_inodes=819200,mode=755,inode64
// 29 23 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot
// 30 23 0:27 / /sys/fs/pstore rw,nosuid,nodev,noexec,relatime shared:5 - pstore pstore rw
// 31 23 0:28 / /sys/firmware/efi/efivars rw,nosuid,nodev,noexec,relatime shared:6 - efivarfs efivarfs rw
// 32 23 0:29 / /sys/fs/bpf rw,nosuid,nodev,noexec,relatime shared:7 - bpf none rw,mode=700
// 34 24 0:20 / /dev/mqueue rw,nosuid,nodev,noexec,relatime shared:14 - mqueue mqueue rw
// 35 23 0:7 / /sys/kernel/debug rw,nosuid,nodev,noexec,relatime shared:15 - debugfs debugfs rw
// 36 23 0:12 / /sys/kernel/tracing rw,nosuid,nodev,noexec,relatime shared:16 - tracefs tracefs rw
// 37 61 0:31 / /tmp rw,nosuid,nodev shared:17 - tmpfs tmpfs rw,size=16415840k,nr_inodes=1048576,inode64
// 38 23 0:32 / /sys/kernel/config rw,nosuid,nodev,noexec,relatime shared:18 - configfs configfs rw
// 59 28 0:33 / /run/credentials/systemd-sysusers.service ro,nosuid,nodev,noexec,relatime shared:19 - ramfs none rw,mode=700
// 39 23 0:34 / /sys/fs/fuse/connections rw,nosuid,nodev,noexec,relatime shared:20 - fusectl fusectl rw
// 120 33 0:36 / /proc/sys/fs/binfmt_misc rw,nosuid,nodev,noexec,relatime shared:59 - binfmt_misc binfmt_misc rw
// 413 61 0:40 / /var/lib/hugetlbfs/global/pagesize-2MB rw,relatime shared:154 - hugetlbfs none rw,pagesize=2M
// 424 61 0:41 / /var/lib/hugetlbfs/global/pagesize-1GB rw,relatime shared:179 - hugetlbfs none rw,pagesize=1024M
// 435 28 0:42 / /run/user/1000 rw,nosuid,nodev,relatime shared:202 - tmpfs tmpfs rw,size=3283320k,nr_inodes=820830,mode=700,uid=1000,gid=1001,inode64
// 451 435 0:43 / /run/user/1000/doc rw,nosuid,nodev,relatime shared:235 - fuse.portal portal rw,user_id=1000,group_id=1001
// 86 61 259:4  /                    /home rw,noatime,nodiratime shared:38 - ext4 /dev/nvme0n1p2 rw
// 89 61 259:6  /                    /boot/efi rw,noatime shared:45 - vfat /dev/nvme0n1p4 rw
func TestListMounts(t *testing.T) {
  lu,fs_reader := buildFilesystemUtil(t)
  fs_reader.FileContent["/proc/self/mountinfo"] = `
22 61 0:21   /                    /proc rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw
23 61 0:22   /                    /sys rw,nosuid,nodev,noexec,relatime shared:2 - sysfs sysfs rw
24 61 0:5    /                    /dev rw,nosuid shared:8 - devtmpfs devtmpfs rw
61 1 259:3   /                    / rw,noatime,nodiratime shared:1 - ext4 /dev/nvme0n1p1 rw
93 61 254:0  /                    /media/some_fs_a rw shared:47 - ext4 /dev/mapper/mapper-group rw
99 86 254:0  /Bind_dm-0           /home/host_user/Bind_dm-0 rw shared:47 - ext4 /dev/mapper/mapper-group rw
105 61 8:3   /                    /media/some_fs_b rw shared:52 - ext4 /dev/sda3 rw
108 61 8:4   /                    /media/some_fs_c rw shared:54 - ext4 /dev/sda4 rw
111 86 8:3   /Bind_sda3           /home/host_user/Bind_sda3 rw shared:52 - ext4 /dev/sda3 rw
114 86 8:4   /Bind_sda4           /home/host_user/Bind_sda4 rw shared:54 - ext4 /dev/sda4 rw
468 37 0:44  /                    /tmp/btrfs_test_partition_src rw shared:245 - btrfs /dev/loop111p1 rw
483 37 0:47  /                    /tmp/btrfs_test_partition_dst rw shared:253 - btrfs /dev/loop111p2 rw
498 37 0:44  /asubvol             /tmp/btrfs_test_partition_vol/asubvol rw shared:261 - btrfs /dev/loop111p1 rw
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

  expected := `[
  {
    "Id": 61,
    "Device": {
      "Name": "nvme0n1p1",
      "MapperGroup": "",
      "Minor": 3,
      "Major": 259,
      "FsUuid": "fs-uuid-nvme0n1p1",
      "GptUuid": "gpt-uuid-nvme0n1p1"
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
      "GptUuid": ""
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
      "GptUuid": ""
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
      "GptUuid": "gpt-uuid-sda3"
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
      "GptUuid": "gpt-uuid-sda4"
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
      "GptUuid": "gpt-uuid-sda3"
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
      "GptUuid": "gpt-uuid-sda4"
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
      "GptUuid": "gpt-uuid-loop111p1"
    },
    "TreePath": "",
    "MountedPath": "/tmp/btrfs_test_partition_src",
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
      "GptUuid": "gpt-uuid-loop111p2"
    },
    "TreePath": "",
    "MountedPath": "/tmp/btrfs_test_partition_dst",
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
      "GptUuid": "gpt-uuid-loop111p1"
    },
    "TreePath": "asubvol",
    "MountedPath": "/tmp/btrfs_test_partition_vol/asubvol",
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

