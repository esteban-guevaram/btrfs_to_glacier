package shim

/*
#include <btrfs/version.h>
#include <stdlib.h>
#include <stdio.h>
#include <linux_utils.h>
*/
import "C"
import "fmt"

func IsCapSysAdmin() bool {
  // can only cast C.int to int
  res := int(C.is_cap_sys_admin())
  return (res != 0)
}

func LinuxKernelVersion() (uint32, uint32) {
  var result C.struct_MajorMinor
  C.linux_kernel_version(&result)
  return uint32(result.major), uint32(result.minor)
}

func BtrfsProgsVersion() (uint32, uint32) {
  var maj, min int
  tok_cnt, err := fmt.Sscanf(C.BTRFS_BUILD_VERSION, "Btrfs v%d.%d", &maj, &min)
  if tok_cnt != 2 || err != nil {
    panic("Failed to get btrfs progrs version from header.")
  }
  return uint32(maj), uint32(min)
}

