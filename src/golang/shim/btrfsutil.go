package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <btrfsutil.h>
#include <btrfs/ctree.h>
*/
import "C"
import (
  "context"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/types"
  "fmt"
  fpmod "path/filepath"
  "strings"
  "unsafe"
)

type btrfsUtilImpl struct {}

func NewBtrfsutil(conf *pb.Config) (types.Btrfsutil, error) {
  impl := new(btrfsUtilImpl)
  return impl, nil
}

func (self *btrfsUtilImpl) SubvolumeInfo(path string) (*pb.SubVolume, error) {
  var subvol C.struct_btrfs_util_subvolume_info
  c_path := C.CString(path)
  defer C.free(unsafe.Pointer(c_path))

  if !fpmod.IsAbs(path) {
    return nil, fmt.Errorf("needs an absolute path, got: %s", path)
  }

  util.Infof("btrfs_util_subvolume_info('%s')", path)
  // We pass 0 so that the subvolume ID of @path is used.
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
    Uuid: bytesToUuid(subvol.uuid),
    MountedPath: strings.TrimSuffix(path, "/"),
    GenAtCreation: uint64(subvol.otransid),
    CreatedTs: uint64(subvol.otime.tv_sec),
  }
}

// Unlike btrfs we consider ONLY read-only snaps
func (self *btrfsUtilImpl) toProtoSnapOrSubvol(subvol *C.struct_btrfs_util_subvolume_info, path string) *pb.Snapshot {
  const NULL_UUID = "00000000000000000000000000000000"
  snap := &pb.Snapshot {
    Subvol: self.toProtoSubVolume(subvol, path),
  }
  parent_uuid := bytesToUuid(subvol.parent_uuid)
  // Becareful it is a trap subvol.flags does not do sh*t to determine read_only snaps
  // C.BTRFS_SUBVOL_RDONLY is not the right mask ?!
  //util.Infof("path=%s parent_uuid=%s, flags=%x", path, parent_uuid, subvol.flags)
  if parent_uuid != NULL_UUID {
    var is_read_only C.bool = false
    c_path := C.CString(path)
    defer C.free(unsafe.Pointer(c_path))
    stx := C.btrfs_util_get_subvolume_read_only(c_path, &is_read_only)
    if stx != C.BTRFS_UTIL_OK || !is_read_only { return snap }
    snap.ParentUuid = parent_uuid
    received_uuid := bytesToUuid(subvol.received_uuid)
    if received_uuid != NULL_UUID { snap.ReceivedUuid = received_uuid }
  }
  return snap
}

func bytesToUuid(uuid [16]C.uchar) string {
  var b strings.Builder
  for _, chr := range uuid {
    fmt.Fprintf(&b, "%.2x", chr)
  }
  return b.String()
}

func (self *btrfsUtilImpl) CreateSubVolumeIterator(path string) (*C.struct_btrfs_util_subvolume_iterator, error) {
  var subvol_it *C.struct_btrfs_util_subvolume_iterator
  c_path := C.CString(path)
  defer C.free(unsafe.Pointer(c_path))

  if !fpmod.IsAbs(path) {
    return nil, fmt.Errorf("needs an absolute path, got: %s", path)
  }

  util.Infof("btrfs_util_create_subvolume_iterator('%s')", path)
  // top=0 lists all vols if the path is the root of the btrfs filesystem
  stx := C.btrfs_util_create_subvolume_iterator(c_path, 0, 0, &subvol_it)
  if stx != C.BTRFS_UTIL_OK {
    return nil, fmt.Errorf("btrfs_util_create_subvolume_iterator: %s = %d",
                           C.GoString(C.btrfs_util_strerror(stx)), stx)
  }
  return subvol_it, nil
}

func (self *btrfsUtilImpl) SubVolumeIteratorNextInfo(subvol_it *C.struct_btrfs_util_subvolume_iterator, root_path string) (*pb.Snapshot, error) {
  var c_rel_path *C.char = nil
  var subvol C.struct_btrfs_util_subvolume_info
  defer C.free(unsafe.Pointer(c_rel_path))

  util.Infof("btrfs_util_subvolume_iterator_next_info('%p')", subvol_it)
  stx := C.btrfs_util_subvolume_iterator_next_info(subvol_it, &c_rel_path, &subvol)
  if stx == C.BTRFS_UTIL_ERROR_STOP_ITERATION {
    return nil, nil
  }
  if stx != C.BTRFS_UTIL_OK {
    return nil, fmt.Errorf("btrfs_util_subvolume_iterator_next_info: %s = %d",
                           C.GoString(C.btrfs_util_strerror(stx)), stx)
  }
  subvol_path := root_path
  if c_rel_path != nil {
    subvol_path = fpmod.Join(root_path, C.GoString(c_rel_path))
  }
  return self.toProtoSnapOrSubvol(&subvol, subvol_path), nil
}

func (self *btrfsUtilImpl) ListSubVolumesUnder(path string) ([]*pb.Snapshot, error) {
  vols := make([]*pb.Snapshot, 0, 32)
  var err error
  var subvol *pb.Snapshot
  var subvol_it *C.struct_btrfs_util_subvolume_iterator

  subvol_it, err = self.CreateSubVolumeIterator(path)
  if err != nil { return nil, err }
  defer C.btrfs_util_destroy_subvolume_iterator(subvol_it)
  for subvol, err = self.SubVolumeIteratorNextInfo(subvol_it, path);
      err == nil && subvol != nil;
      subvol, err = self.SubVolumeIteratorNextInfo(subvol_it, path) {
      vols = append(vols, subvol)
  }
  return vols, nil
}

func (self *btrfsUtilImpl) ReadAndProcessSendStream(dump types.PipeReadEnd) (*types.SendDumpOperations, error) {
  return readAndProcessSendStreamHelper(dump.Fd())
}

func (self *btrfsUtilImpl) StartSendStream(ctx context.Context, from string, to string, no_data bool) (types.PipeReadEnd, error) {
  return nil, nil
}


