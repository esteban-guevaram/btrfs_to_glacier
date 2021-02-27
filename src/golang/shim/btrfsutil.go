package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <btrfsutil.h>
*/
import "C"
import (
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/types"
  "fmt"
  fpmod "path/filepath"
  "strings"
  "unsafe"
)

type btrfsUtilImpl struct {}

func NewBtrfsutil(conf types.Config) (types.Btrfsutil, error) {
  impl := new(btrfsUtilImpl)
  return impl, nil
}

func (self *btrfsUtilImpl) SubvolumeInfo(path string, subvol_id int) (*pb.SubVolume, error) {
  var subvol C.struct_btrfs_util_subvolume_info
  c_path := C.CString(path)
  defer C.free(unsafe.Pointer(c_path))

  if !fpmod.IsAbs(path) {
    return nil, fmt.Errorf("needs an absolute path, got: %s", path)
  }

  util.Infof("btrfs_util_subvolume_info('%s')", path)
  stx := C.btrfs_util_subvolume_info(c_path, 0, &subvol)
  if stx != C.BTRFS_UTIL_OK {
    return nil, fmt.Errorf("btrfs_util_subvolume_info: %s = %d",
                           C.GoString(C.btrfs_util_strerror(stx)), stx)
  }
  if subvol.parent_id == 0 {
    return nil, fmt.Errorf("returning root subvolume is not supported")
  }
  return self.toProtoSubVolume(&subvol, path), nil
}

func (*btrfsUtilImpl) toProtoSubVolume(subvol *C.struct_btrfs_util_subvolume_info, path string) *pb.SubVolume {
  return &pb.SubVolume {
    Uuid: bytesToUuid(subvol),
    MountedPath: strings.TrimSuffix(path, "/"),
    CreatedTs: uint64(subvol.otime.tv_sec),
  }
}

func bytesToUuid(subvol *C.struct_btrfs_util_subvolume_info) string {
  var b strings.Builder
  for _, chr := range subvol.uuid {
    fmt.Fprintf(&b, "%.2x", chr)
  }
  return b.String()
}

