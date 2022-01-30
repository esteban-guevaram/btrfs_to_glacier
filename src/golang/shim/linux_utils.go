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
  "os"
  "strconv"
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

type Linuxutil struct {
  *FilesystemUtil
  HasLinuxVersion bool
  LinuxMaj, LinuxMin uint32
  HasSudoUid bool
  SudoUid int
}

func NewLinuxutil(conf *pb.Config) (types.Linuxutil, error) {
  return &Linuxutil{ 
    FilesystemUtil: &FilesystemUtil{ SysUtil:new(SysUtilImpl), },
  }, nil
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

