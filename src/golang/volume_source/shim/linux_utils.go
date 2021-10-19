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

  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
)

const ROOT_UID = 0

type Linuxutil struct {}

func NewLinuxutil(conf *pb.Config) (types.Linuxutil, error) {
  return new(Linuxutil), nil
}

func (*Linuxutil) IsCapSysAdmin() bool {
  //return ROOT_UID == os.Geteuid()
  return C.is_cap_sys_admin() != 0
}

func (*Linuxutil) LinuxKernelVersion() (uint32, uint32) {
  var result C.struct_MajorMinor
  C.linux_kernel_version(&result)
  return uint32(result.major), uint32(result.minor)
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

func get_sudo_uid_from_env() (int, error) {
  sudo_uid, err := strconv.Atoi(os.Getenv("SUDO_UID"))
  if err != nil { return 0, err }
  if sudo_uid < 1 { return 0, fmt.Errorf("invalid sudo uid") }
  //util.Debugf("sudo_ruid=%v", sudo_uid)
  return sudo_uid, nil
}

func (self *Linuxutil) DropRoot() (func(), error) {
  if ROOT_UID != os.Geteuid() { return func() {}, nil }
  sudo_uid, err := get_sudo_uid_from_env()
  if err != nil { return nil, err }

  C.set_euid_or_die((C.int)(sudo_uid))
  restore_f := func() { C.set_euid_or_die(ROOT_UID) }
  return restore_f, nil
}

func (self *Linuxutil) GetRoot() (func(), error) {
  if ROOT_UID == os.Geteuid() { return func() {}, nil }
  sudo_uid, err := get_sudo_uid_from_env()
  if err != nil { return nil, err }

  C.set_euid_or_die(ROOT_UID)
  restore_f := func() { C.set_euid_or_die((C.int)(sudo_uid)) }
  return restore_f, nil
}

