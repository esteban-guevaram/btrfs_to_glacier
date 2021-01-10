package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <btrfsutil.h>
*/
import "C"
import (
  "fmt"
  "strings"
)

type subvolume_info_impl struct {
  subvol_ C.struct_btrfs_util_subvolume_info
}

func (self *subvolume_info_impl) Uuid() string {
  var b strings.Builder
  for _, chr := range self.subvol_.uuid {
    fmt.Fprintf(&b, "%.2x", chr)
  }
  return b.String()
}

