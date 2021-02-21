package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <btrfsutil.h>
*/
import "C"
import (
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/types"
  "fmt"
  "unsafe"
)

type btrfsUtilImpl struct {}

func NewBtrfsutil(conf types.Config) (types.Btrfsutil, error) {
  impl := new(btrfsUtilImpl)
  return impl, nil
}

func (*btrfsUtilImpl) CheckCompatibleWithHost() error {
  return nil
}

func (*btrfsUtilImpl) SubvolumeInfo(path string, subvol_id int) (types.SubvolumeInfoIf, error) {
  var subvol subvolume_info_impl
  c_path := C.CString(path)
  defer C.free(unsafe.Pointer(c_path))

  util.Infof("btrfs_util_subvolume_info('%s')", path)
  stx := C.btrfs_util_subvolume_info(c_path, 0, &subvol.subvol_)
  if stx != 0 {
    return nil, fmt.Errorf("btrfs_util_subvolume_info = %d", stx)
  }
  return &subvol, nil
}

