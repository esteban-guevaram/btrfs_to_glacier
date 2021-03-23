package shim

/*
#include <common.h>
#include <btrfs/send-stream.h>
#include <string.h>

extern int go_subvol_cb        (char *path, u8 *uuid, u64 ctransid, void *user);
extern int go_snapshot_cb      (char *path, u8 *uuid, u64 ctransid,
                                u8 *parent_uuid, u64 parent_ctransid, void *user);
extern int go_mkfile_cb        (char *path, void *user);
extern int go_mkdir_cb         (char *path, void *user);
extern int go_mknod_cb         (char *path, u64 mode, u64 dev, void *user);
extern int go_mkfifo_cb        (char *path, void *user);
extern int go_mksock_cb        (char *path, void *user);
extern int go_symlink_cb       (char *path, char *lnk, void *user);
extern int go_rename_cb        (char *from, char *to, void *user);
extern int go_link_cb          (char *path, char *lnk, void *user);
extern int go_unlink_cb        (char *path, void *user);
extern int go_rmdir_cb         (char *path, void *user);
extern int go_write_cb         (char *path, void *data, u64 offset, u64 len,
                                void *user);
extern int go_clone_cb         (char *path, u64 offset, u64 len, u8 *clone_uuid,
                                u64 clone_ctransid, char *clone_path, u64 clone_offset, void *user);
extern int go_set_xattr_cb     (char *path, char *name, void *data,
                                int len, void *user);
extern int go_remove_xattr_cb  (char *path, char *name, void *user);
extern int go_truncate_cb      (char *path, u64 size, void *user);
extern int go_chmod_cb         (char *path, u64 mode, void *user);
extern int go_chown_cb         (char *path, u64 uid, u64 gid, void *user);
extern int go_utimes_cb        (char *path, struct timespec *at,
                                struct timespec *mt, struct timespec *ct, void *user);
extern int go_update_extent_cb (char *path, u64 offset, u64 len, void *user);

IGNORE_WARNING_PUSH("-Wincompatible-pointer-types")
IGNORE_WARNING_PUSH("-Wunused-function")
static struct btrfs_send_ops get_go_ops() {
  struct btrfs_send_ops ops = {
    .subvol = go_subvol_cb,
    .snapshot = go_snapshot_cb,
    .mkfile = go_mkfile_cb,
    .mkdir = go_mkdir_cb,
    .mknod = go_mknod_cb,
    .mkfifo = go_mkfifo_cb,
    .mksock = go_mksock_cb,
    .symlink = go_symlink_cb,
    .rename = go_rename_cb,
    .link = go_link_cb,
    .unlink = go_unlink_cb,
    .rmdir = go_rmdir_cb,
    .write = go_write_cb,
    .clone = go_clone_cb,
    .set_xattr = go_set_xattr_cb,
    .remove_xattr = go_remove_xattr_cb,
    .truncate = go_truncate_cb,
    .chmod = go_chmod_cb,
    .chown = go_chown_cb,
    .utimes = go_utimes_cb,
    .update_extent = go_update_extent_cb,
  };
  return ops;
}
IGNORE_WARNING_POP
IGNORE_WARNING_POP
*/
import "C"
import "fmt"
import "sync/atomic"
import "syscall"
import "unsafe"
import "btrfs_to_glacier/types"

func callback_harness(user unsafe.Pointer, body func(state *types.SendDumpOperations)) C.int {
  state := getSendDumpOpsFromHandle(user)
  if state.Err != nil { return 1 }
  body(state)
  if state.Err != nil { return 1 }
  return 0
}

//export go_subvol_cb
func go_subvol_cb        (path *C.char, uuid *C.u8, ctransid C.u64, user unsafe.Pointer) C.int { return 0; }
//export go_snapshot_cb
func go_snapshot_cb      (path *C.char, uuid *C.u8, ctransid C.u64,
                          parent_uuid *C.u8, parent_ctransid C.u64, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    if len(state.ToUuid) > 0 || len(state.FromUuid) > 0 {
      state.Err = fmt.Errorf("Overwriting to_uuid=%s, from_uuid=%s", state.ToUuid, state.FromUuid)
    }
    state.ToUuid = bytePtrToUuid(uuid)
    state.FromUuid = bytePtrToUuid(parent_uuid)
  })
}

//export go_mkfile_cb
func go_mkfile_cb        (path *C.char, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_path := C.GoString(path)
    state.New[go_path]= true
  })
}

//export go_mkdir_cb
func go_mkdir_cb         (path *C.char, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_path := C.GoString(path)
    state.NewDir[go_path]= true
  })
}

//export go_mknod_cb
func go_mknod_cb         (path *C.char, mode C.u64, dev C.u64, user unsafe.Pointer) C.int { return 0 }
//export go_mkfifo_cb
func go_mkfifo_cb        (path *C.char, user unsafe.Pointer) C.int { return 0 }
//export go_mksock_cb
func go_mksock_cb        (path *C.char, user unsafe.Pointer) C.int { return 0 }
//export go_symlink_cb
func go_symlink_cb       (path *C.char, lnk *C.char, user unsafe.Pointer) C.int { return 0 }
//export go_rename_cb
func go_rename_cb        (from *C.char, to *C.char, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_from := C.GoString(from)
    go_to := C.GoString(to)
    if _, found := state.FromTo[go_from]; found {
      state.Err = fmt.Errorf("Overwriting FromTo key=%s", go_from)
    }
    state.FromTo[go_from] = go_to
  })
}

//export go_link_cb
func go_link_cb          (path *C.char, lnk *C.char, user unsafe.Pointer) C.int {
  return go_rename_cb(lnk, path, user)
}

//export go_unlink_cb
func go_unlink_cb        (path *C.char, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_path := C.GoString(path)
    state.Deleted[go_path]= true
  })
}

//export go_rmdir_cb
func go_rmdir_cb         (path *C.char, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_path := C.GoString(path)
    state.DelDir[go_path] = true
  })
}

//export go_write_cb
func go_write_cb         (path *C.char, data unsafe.Pointer, offset C.u64, len_ C.u64,
                          user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    state.Err = fmt.Errorf("Did not expect go_write_cb for %s (len=%d)", C.GoString(path), len_)
  })
}

//export go_clone_cb
func go_clone_cb         (path *C.char, offset C.u64, len_ C.u64, clone_uuid *C.u8,
                          clone_ctransid C.u64, clone_path *C.char, clone_offset C.u64, user unsafe.Pointer) C.int { return 0 }
//export go_set_xattr_cb
func go_set_xattr_cb     (path *C.char, name *C.char, data unsafe.Pointer,
                          len_ C.int, user unsafe.Pointer) C.int { return 0 }
//export go_remove_xattr_cb
func go_remove_xattr_cb  (path *C.char, name *C.char, user unsafe.Pointer) C.int { return 0 }
//export go_truncate_cb
func go_truncate_cb      (path *C.char, size C.u64, user unsafe.Pointer) C.int {
  return go_update_extent_cb(path, 0, size, user)
}

//export go_chmod_cb
func go_chmod_cb         (path *C.char, mode C.u64, user unsafe.Pointer) C.int { return 0 }
//export go_chown_cb
func go_chown_cb         (path *C.char, uid C.u64, gid C.u64, user unsafe.Pointer) C.int { return 0 }
//export go_utimes_cb
func go_utimes_cb        (path *C.char, at *C.struct_timespec,
                          mt *C.struct_timespec, ct *C.struct_timespec, user unsafe.Pointer) C.int { return 0 }
//export go_update_extent_cb
func go_update_extent_cb (path *C.char, offset C.u64, len_ C.u64, user unsafe.Pointer) C.int {
  return callback_harness(user, func(state *types.SendDumpOperations) {
    go_path := C.GoString(path)
    state.Written[go_path]= true
  })
}

type stateTable struct {
  slots map[int32]*types.SendDumpOperations
  free int32
}

var static_table stateTable
func init() {
  static_table = stateTable {
    slots: make(map[int32]*types.SendDumpOperations),
  }
}

func allocateSendDumpOps() int32 {
  slot_idx := atomic.AddInt32(&static_table.free, 1)
  static_table.slots[slot_idx] = &types.SendDumpOperations{
    Written: make(map[string]bool),
    New: make(map[string]bool),
    NewDir: make(map[string]bool),
    Deleted: make(map[string]bool),
    DelDir: make(map[string]bool),
    FromTo: make(map[string]string),
  }
  return slot_idx
}

func deallocateSendDumpOps(slot_idx int32) {
  delete(static_table.slots, slot_idx)
}

func getSendDumpOpsFromHandle(handle unsafe.Pointer) *types.SendDumpOperations {
  slot_idx := *(*int32)(handle)
  return getSendDumpOpsFromKey(slot_idx)
}
func getSendDumpOpsFromKey(idx int32) *types.SendDumpOperations {
  state, ok := static_table.slots[idx]
  if !ok { panic("could not find state in static table") }
  return state
}

func bytePtrToUuid(uuid *C.u8) string {
  var a [16]C.u8
  C.memcpy(unsafe.Pointer(&a[0]), unsafe.Pointer(uuid), 16)
  return bytesToUuid(a)
}

func readAndProcessSendStreamHelper(dump_fd uintptr) (*types.SendDumpOperations, error) {
  const IGNORE_ERR = 0
  const PROPAGATE_LAST_CMD_ERR = 1
  slot_idx := allocateSendDumpOps()
  defer deallocateSendDumpOps(slot_idx)
  ops := C.get_go_ops()
  ret := C.btrfs_read_and_process_send_stream(C.int(dump_fd), &ops, unsafe.Pointer(&slot_idx),
                                              PROPAGATE_LAST_CMD_ERR, IGNORE_ERR)
  state := getSendDumpOpsFromKey(slot_idx)
  if state.Err != nil {
    return nil, state.Err
  }
  // btrfs_read_and_process_send_stream is f*cked BTRFS_SEND_C_END will always produce a 1 return code ?
  if ret != 0 && ret != 1 {
    return nil, fmt.Errorf("btrfs_read_and_process_send_stream: %d=%s", ret, syscall.Errno(ret))
  }
  return state, nil
}

