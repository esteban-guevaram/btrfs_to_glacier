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
import "fmt"
import "btrfs_to_glacier/types"

type linuxutilImpl struct {}

func NewLinuxutil(conf types.Config) (types.Linuxutil, error) {
  return new(linuxutilImpl), nil
}

func (*linuxutilImpl) IsCapSysAdmin() bool {
  // can only cast C.int to int
  res := int(C.is_cap_sys_admin())
  return (res != 0)
}

func (*linuxutilImpl) LinuxKernelVersion() (uint32, uint32) {
  var result C.struct_MajorMinor
  C.linux_kernel_version(&result)
  return uint32(result.major), uint32(result.minor)
}

func (*linuxutilImpl) BtrfsProgsVersion() (uint32, uint32) {
  var maj, min int
  tok_cnt, err := fmt.Sscanf(C.BTRFS_BUILD_VERSION, "Btrfs v%d.%d", &maj, &min)
  if tok_cnt != 2 || err != nil {
    panic("Failed to get btrfs progrs version from header.")
  }
  return uint32(maj), uint32(min)
}

func (*linuxutilImpl) ProjectVersion() string {
  return C.BTRFS_TO_GLACIER_VERSION
}

