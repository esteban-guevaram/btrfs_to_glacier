package shim

/*
#include <btrfs/version.h>
#include <stdlib.h>
#include <stdio.h>
#include <linux_utils.h>

// This macro is normally defined by the preprocessor flags introduced by `go env`
#ifndef BTRFS_TO_GLACIER_VERSION
#define BTRFS_TO_GLACIER_VERSION "NO_VERSION"
#endif
*/
import "C"
import (
  "fmt"
  fpmod "path/filepath"
  "os"
  "regexp"
  "strconv"
  "strings"
  "sync"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  pb "btrfs_to_glacier/messages"
)

var (
  cap_sys_admin_mutex sync.Mutex
  cap_sys_admin_nesting uint32
  is_cap_sys_admin bool
)

func init() {
  is_cap_sys_admin = C.is_cap_sys_admin() != 0
}

const ROOT_UID = 0
const SYS_FS = "/sys/fs/btrfs"
const SYS_FS_FEATURE_DIR = "features"
const SYS_FS_UUID = "metadata_uuid"
const SYS_FS_LABEL = "label"
const SYS_FS_DEVICE_DIR = "devices"
const SYS_FS_DEVICE_FILE = "dev"
const MOUNT_INFO = "/proc/self/mountinfo"

// Dependency injection for unittests.
type FsReaderIf interface {
  ReadAsciiFile(string, string, bool) (string, error)
  ReadDir(string) ([]os.DirEntry, error)
  EvalSymlinks(string) (string, error)
}
type FsReaderImpl struct {}
type Linuxutil struct {
  FsReader FsReaderIf
  HasLinuxVersion bool
  LinuxMaj, LinuxMin uint32
  HasSudoUid bool
  SudoUid int
}

func NewLinuxutil(conf *pb.Config) (types.Linuxutil, error) {
  return &Linuxutil{ FsReader:new(FsReaderImpl), }, nil
}

func (*Linuxutil) IsCapSysAdmin() bool {
  cap_sys_admin_mutex.Lock()
  defer cap_sys_admin_mutex.Unlock()
  //return ROOT_UID == os.Geteuid()
  return is_cap_sys_admin
}

func (self *Linuxutil) LinuxKernelVersion() (uint32, uint32) {
  if !self.HasLinuxVersion {
    var result C.struct_MajorMinor
    C.linux_kernel_version(&result)
    self.LinuxMaj = uint32(result.major)
    self.LinuxMin = uint32(result.minor)
    self.HasLinuxVersion = true
  }
  return self.LinuxMaj, self.LinuxMin
}

func (*Linuxutil) BtrfsProgsVersion() (uint32, uint32) {
  var maj, min int
  tok_cnt, err := fmt.Sscanf(C.BTRFS_BUILD_VERSION, "Btrfs v%d.%d", &maj, &min)
  if tok_cnt != 2 || err != nil {
    panic("Failed to get btrfs progrs version from header.")
  }
  return uint32(maj), uint32(min)
}

func (*Linuxutil) ProjectVersion() string {
  return C.BTRFS_TO_GLACIER_VERSION
}

func (self *Linuxutil) getSudouidFromEnv() (int, error) {
  if !self.HasSudoUid {
    var err error
    self.SudoUid, err = strconv.Atoi(os.Getenv("SUDO_UID"))
    if err != nil { return 0, err }
    if self.SudoUid < 1 { return 0, fmt.Errorf("invalid sudo uid") }
    //util.Debugf("sudo_ruid=%v", sudo_uid)
    self.HasSudoUid = true
  }
  return self.SudoUid, nil
}

func (self *Linuxutil) DropRoot() (func(), error) {
  sudo_uid, err := self.getSudouidFromEnv()
  if err != nil { return nil, err }

  cap_sys_admin_mutex.Lock()
  defer cap_sys_admin_mutex.Unlock()
  if !is_cap_sys_admin { return func() {}, nil }

  C.set_euid_or_die((C.int)(sudo_uid))
  cap_sys_admin_nesting += 1
  expect_nest := cap_sys_admin_nesting
  is_cap_sys_admin = false

  restore_f := func() {
    cap_sys_admin_mutex.Lock()
    defer cap_sys_admin_mutex.Unlock()
    if cap_sys_admin_nesting != expect_nest { util.Fatalf("DropRoot bad nesting") }
    C.set_euid_or_die(ROOT_UID)
    cap_sys_admin_nesting -= 1
    is_cap_sys_admin = true
  }
  return restore_f, nil
}

func (self *Linuxutil) GetRoot() (func(), error) {
  sudo_uid, err := self.getSudouidFromEnv()
  if err != nil { return nil, err }

  cap_sys_admin_mutex.Lock()
  defer cap_sys_admin_mutex.Unlock()
  if is_cap_sys_admin { return func() {}, nil }

  C.set_euid_or_die(ROOT_UID)
  cap_sys_admin_nesting += 1
  expect_nest := cap_sys_admin_nesting
  is_cap_sys_admin = true

  restore_f := func() {
    cap_sys_admin_mutex.Lock()
    defer cap_sys_admin_mutex.Unlock()
    if cap_sys_admin_nesting != expect_nest { util.Fatalf("GetRoot bad nesting") }
    C.set_euid_or_die((C.int)(sudo_uid))
    cap_sys_admin_nesting -= 1
    is_cap_sys_admin = false
  }
  return restore_f, nil
}

func (self *FsReaderImpl) ReadDir(dir string) ([]os.DirEntry, error) {
  return os.ReadDir(dir)
}

func (self *FsReaderImpl) EvalSymlinks(path string) (string, error) {
  return fpmod.EvalSymlinks(path)
}

func (self *FsReaderImpl) ReadAsciiFile(
    dir string, name string, allow_ctrl bool) (string, error) {
  fpath := fpmod.Join(dir, name)
  //util.Debugf("Reading: '%s'", fpath)
  bytes, err := os.ReadFile(fpath)
  if err != nil { return "", err }
  str := strings.TrimRight(string(bytes), "\n")
  err = util.IsOnlyAsciiString(str, allow_ctrl)
  if err != nil { err = fmt.Errorf("file:'%s', err:%v", fpath, err) }
  return str, err
}

func majminFromString(in string) (int,int,error) {
  sep_idx := strings.Index(in, ":")
  if sep_idx < 1 { return 0,0, fmt.Errorf("bad format for device/dev file, expectin maj:min") }
  maj, err := strconv.Atoi(in[:sep_idx])
  if err != nil { return 0,0, err }
  min, err := strconv.Atoi(in[sep_idx+1:])
  if err != nil { return 0,0, err }
  return maj, min, nil
}

func (self *Linuxutil) devicesFromSysFs(path string) ([]*types.Device, error) {
  var dev_list []*types.Device
  items, err := self.FsReader.ReadDir(path)
  if err != nil { return nil, err }

  for _,item := range items {
    dev := &types.Device{ Name:item.Name(), }
    if item.IsDir() { return nil, fmt.Errorf("expecting only links under: '%s'", path) }
    real_path, err := self.FsReader.EvalSymlinks(fpmod.Join(path, item.Name()))
    if err != nil { return nil, err }
    majmin, err := self.FsReader.ReadAsciiFile(real_path, SYS_FS_DEVICE_FILE, false)
    if err != nil { return nil, err }
    dev.Major, dev.Minor, err = majminFromString(majmin)
    if err != nil { return nil, err }
    dev_list = append(dev_list, dev)
  }
  return dev_list, nil
}

func (self *Linuxutil) btrfsFilesystemsFromSysFs() ([]*types.Filesystem, error) {
  var fs_list []*types.Filesystem
  items, err := self.FsReader.ReadDir(SYS_FS)
  if err != nil { return nil, err }

  for _,item := range items {
    if !item.IsDir() { continue }
    if item.Name() == SYS_FS_FEATURE_DIR { continue }
    fs_item := &types.Filesystem{ Uuid: item.Name(), }
    dir_path := fpmod.Join(SYS_FS, fs_item.Uuid)
    items, err := self.FsReader.ReadDir(dir_path)
    if err != nil { return nil, err }

    for _,item := range items {
      switch name := item.Name(); name {
        case SYS_FS_UUID:
          uuid, err := self.FsReader.ReadAsciiFile(dir_path, name, false)
          if err != nil { return nil, err }
          if uuid != fs_item.Uuid { return nil, fmt.Errorf("fs uuid mismatch: %s != %s", uuid, fs_item.Uuid) }
        case SYS_FS_LABEL:
          label, err := self.FsReader.ReadAsciiFile(dir_path, name, false)
          if err != nil { return nil, err }
          if len(label) < 1 { return nil, fmt.Errorf("expect fs to have a non empty label") }
          fs_item.Label = label
        case SYS_FS_DEVICE_DIR:
          devs, err := self.devicesFromSysFs(fpmod.Join(dir_path, name))
          if err != nil { return nil, err }
          fs_item.Devices = devs
      }
    }

    if len(fs_item.Label) > 0 && len(fs_item.Devices) > 0 {
      fs_list = append(fs_list, fs_item)
    }
  }
  return fs_list, nil
}

func (self *Linuxutil) parseMountOptions(line string) (map[string]string, error) {
  opts := make(map[string]string)
  for _,kv := range strings.Split(line, ",") {
    var err error
    var key, val string
    eq_idx := strings.Index(kv, "=")
    if eq_idx > -1 {
      key, err = strconv.Unquote(fmt.Sprintf(`"%s"`, kv[:eq_idx]))
      if err != nil { return nil, err }
      val, err = strconv.Unquote(fmt.Sprintf(`"%s"`, kv[eq_idx+1:]))
      if err != nil { return nil, err }
    } else {
      key, err = strconv.Unquote(fmt.Sprintf(`"%s"`, kv))
      if err != nil { return nil, err }
      val = ""
    }
    opts[key] = val
  }
  return opts, nil
}

// Note : there is a c library to do this, but re-implementing is easier.
// https://git.kernel.org/pub/scm/utils/util-linux/util-linux.git/tree/libmount
//
// be careful it is a trap ! the major/minor numbers in this file do not correspond to the device major/minor
//
//29  1   8:18 /                    /                     rw,... shared:1   - ext4  /dev/sdb2 rw
//142 29  8:19 /                    /home                 rw,... shared:63  - ext4  /dev/sdb3 rw
//169 29  0:38 /Lucian_PrioA        /media/Lucian_PrioA   rw,... shared:92  - btrfs /dev/sdc1 subvolid=260,subvol=/Lucian_PrioA
//172 29  0:38 /Lucian_PrioB        /media/Lucian_PrioB   rw,... shared:89  - btrfs /dev/sdc1 subvolid=258,subvol=/Lucian_PrioB
//170 29  0:38 /Lucian_PrioC        /media/Lucian_PrioC   rw,... shared:95  - btrfs /dev/sdc1 subvolid=259,subvol=/Lucian_PrioC
//189 142 0:38 /Lucian_PrioA/Images /home/cguevara/Images rw,... shared:92  - btrfs /dev/sdc1 subvolid=260,subvol=/Lucian_PrioA
//194 142 0:38 /Lucian_PrioA/MyProj /home/cguevara/Progr  rw,... shared:92  - btrfs /dev/sdc1 subvolid=260,subvol=/Lucian_PrioA
//199 142 0:38 /Lucian_PrioC/Music  /home/cguevara/Music  rw,... shared:95  - btrfs /dev/sdc1 subvolid=259,subvol=/Lucian_PrioC
//204 142 0:38 /Lucian_PrioC/Video  /home/cguevara/Videos rw,... shared:95  - btrfs /dev/sdc1 subvolid=259,subvol=/Lucian_PrioC
//436 29  0:38 /BifrostSnap         /media/BifrostSnap    rw,... shared:219 - btrfs /dev/sdc1 subvolid=629,subvol=/BifrostSnap
//451 29  0:38 /                    /media/Bifrost        rw,... shared:252 - btrfs /dev/sdc1 subvolid=5,subvol=/
//
//578 37  0:43 /snaps/asubvol.snap  /tmp/with\040spaces   rw,... shared:341 - btrfs /dev/loop111p1 subvolid=258,subvol=/snaps/asubvol.snap
func (self *Linuxutil) parseMountinfo() ([]*types.MountEntry, error) {
  const ID_IDX = 0
  const MAJMIN_IDX = 2
  const TREE_IDX = 3
  const MOUNT_IDX = 4
  const FS_IDX = 1
  const DEV_IDX = 2
  const OPT_IDX = 3
  const OPT_FLD_END = "-"

  var mnt_list []*types.MountEntry
  rx := regexp.MustCompile(" +")
  file_content, err := self.FsReader.ReadAsciiFile(fpmod.Dir(MOUNT_INFO),
                                                   fpmod.Base(MOUNT_INFO), true)
  if err != nil { return nil, err }

  for _,line := range strings.Split(file_content, "\n") {
    mnt := &types.MountEntry{}
    toks := rx.Split(line, /*all_matches=*/-1)
    if len(line) < 1 || len(toks) < 1 { continue }

    mnt.Major, mnt.Minor, err = majminFromString(toks[MAJMIN_IDX])
    if err != nil { return nil, err }

    mnt.Id, err = strconv.Atoi(toks[ID_IDX])
    if err != nil { return nil, err }

    mnt.TreePath = strings.TrimLeft(toks[TREE_IDX], "/")
    if len(mnt.TreePath) == len(toks[TREE_IDX]) {
      return nil, fmt.Errorf("mountinfo line malformed, expected path: '%s'", line)
    }
    mnt.TreePath, err = strconv.Unquote(fmt.Sprintf(`"%s"`, mnt.TreePath))
    if err != nil { return nil, err }

    mnt.MountedPath, err = strconv.Unquote(fmt.Sprintf(`"%s"`, toks[MOUNT_IDX]))
    if err != nil { return nil, err }

    var tok string
    sep_idx := 0
    for sep_idx,tok = range toks { if tok == OPT_FLD_END { break } }
    if sep_idx >= len(toks)-1 {
      return nil, fmt.Errorf("mountinfo line malformed, no separator: '%s'", line)
    }

    mnt.FsType, err = strconv.Unquote(fmt.Sprintf(`"%s"`, toks[sep_idx+FS_IDX]))
    if err != nil { return nil, err }
    mnt.DevPath, err = strconv.Unquote(fmt.Sprintf(`"%s"`, toks[sep_idx+DEV_IDX]))
    if err != nil { return nil, err }
    mnt.Options, err = self.parseMountOptions(toks[sep_idx+OPT_IDX])
    if err != nil { return nil, err }

    mnt_list = append(mnt_list, mnt)
  }
  util.Infof("Found %d mount entries", len(mnt_list))
  return mnt_list, nil
}

func treePathFromOpts(mnt *types.MountEntry) string {
  return strings.TrimLeft(mnt.Options["subvol"], "/")
}

// Make sure we can rely on the device name as a unique id for the filesystem.
func (self *Linuxutil) validateAndKeepOnlyBtrfsEntries(
    mnt_list []*types.MountEntry) ([]*types.MountEntry, error) {
  var new_list []*types.MountEntry
  devname_to_majmin := make(map[string]string)
  for _,mnt := range mnt_list {
    var err error
    if mnt.FsType != "btrfs" { continue }

    mnt.BtrfsVolId, err = strconv.ParseUint(mnt.Options["subvolid"], 10, 64)
    if err != nil { return nil, err }

    opt_tree_path := treePathFromOpts(mnt)
    if mnt.BtrfsVolId != BTRFS_FS_TREE_OBJECTID {
      if len(opt_tree_path) < 1 { return nil, fmt.Errorf("expected subvol path to be in options") }
      if !strings.HasPrefix(mnt.TreePath, opt_tree_path) {
        return nil, fmt.Errorf("mismatch between tree paths")
      }
    }

    majmin := fmt.Sprintf("%d%d", mnt.Major, mnt.Minor)
    devname := fpmod.Base(mnt.DevPath)
    expect,found := devname_to_majmin[devname]
    if found && expect != majmin { return nil, fmt.Errorf("devname is not unique") }
    if !found { devname_to_majmin[devname] = majmin }

    new_list = append(new_list, mnt)
  }
  util.Infof("Found %d/%d relevant mount entries", len(new_list), len(mnt_list))
  return new_list, nil
}

func (self *Linuxutil) collapseBindMounts(
    mnt_list []*types.MountEntry) ([]*types.MountEntry, error) {
  masters := make(map[string]*types.MountEntry)
  // To avoid name collisions between different btrfs filesystems
  key_f := func(mnt *types.MountEntry) string {
    return fmt.Sprintf("%s%d", mnt.DevPath, mnt.BtrfsVolId)
  }
  var new_list []*types.MountEntry

  for _,mnt := range mnt_list {
    if mnt.TreePath != treePathFromOpts(mnt) { continue }
    key := key_f(mnt)
    if _,found := masters[key]; found { return nil, fmt.Errorf("duplicate master mount") }
    masters[key] = mnt
  }
  for _,mnt := range mnt_list {
    key := key_f(mnt)
    master,found := masters[key]
    if !found { return nil, fmt.Errorf("no master mount found") }
    if master.Id == mnt.Id { new_list = append(new_list, mnt); continue }

    if !strings.HasPrefix(mnt.TreePath, master.TreePath) &&
      mnt.BtrfsVolId != master.BtrfsVolId {
      return nil, fmt.Errorf("mismatch between tree paths")
    }
    master.Binds = append(master.Binds, mnt)
  }
  util.Infof("Found %d/%d after collapsing bind mounts", len(new_list), len(mnt_list))
  return new_list, nil
}

func (self *Linuxutil) matchMountEntriesToFilesystem(
    mnt_list []*types.MountEntry, fs_list []*types.Filesystem) error {
  grp_by_devname := make(map[string][]*types.MountEntry)
  for _,mnt := range mnt_list {
    devname := fpmod.Base(mnt.DevPath)
    grp_by_devname[devname] = append(grp_by_devname[devname], mnt)
  }
  for _,fs := range fs_list {
    found_mnts := false
    for _,dev := range fs.Devices {
      mnts,found := grp_by_devname[dev.Name]
      if !found { continue }
      if found_mnts { return fmt.Errorf("fs has mount entries on several devices") }
      fs.Mounts = mnts
      found_mnts = true
    }
    if !found_mnts { return fmt.Errorf("fs '%s' has no mount entries", fs.Uuid) }
  }
  return nil
}

func (self *Linuxutil) ListBtrfsFilesystems() ([]*types.Filesystem, error) {
  mnt_list, err := self.parseMountinfo()
  if err != nil { return nil, err }
  mnt_list, err = self.validateAndKeepOnlyBtrfsEntries(mnt_list)
  if err != nil { return nil, err }
  mnt_list, err = self.collapseBindMounts(mnt_list)
  if err != nil { return nil, err }

  fs_list, err := self.btrfsFilesystemsFromSysFs()
  if err != nil { return nil, err }
  err = self.matchMountEntriesToFilesystem(mnt_list, fs_list)
  if err != nil { return nil, err }
  util.Infof("Found %d btrfs filesystems", len(fs_list))
  return fs_list, nil
}

