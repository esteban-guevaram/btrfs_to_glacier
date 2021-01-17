package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <btrfsutil.h>
*/
import "C"
import (
  "btrfs_to_glacier/log"
  "btrfs_to_glacier/types"
  "fmt"
  "unsafe"
)

type btrfs_util_impl struct {}

func New() types.Btrfsutil { return new(btrfs_util_impl) }

func (*btrfs_util_impl) SubvolumeInfo(path string, subvol_id int) (types.SubvolumeInfoIf, error) {
  var subvol subvolume_info_impl
  c_path := C.CString(path)
  defer C.free(unsafe.Pointer(c_path))

  log.Infof("btrfs_util_subvolume_info('%s')", path)
  stx := C.btrfs_util_subvolume_info(c_path, 0, &subvol.subvol_);
  if stx != 0 {
    return nil, fmt.Errorf("btrfs_util_subvolume_info = %d", stx)
  }
  return &subvol, nil
}

