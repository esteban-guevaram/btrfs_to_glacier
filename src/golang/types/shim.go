package types

import (
  "context"
  pb "btrfs_to_glacier/messages"
)

// Represents a line in /proc/self/mountinfo
// Many be enriched from info from other places ...
type MountEntry struct {
  Id int // NOT stable across reads of mountinfo
  Device *Device
  TreePath string
  MountedPath string
  FsType string
  Options map[string]string // correspond to the per-superblock options
  BtrfsVolId uint64
  // Bind mounts to the same filesystem/subvolume
  Binds []*MountEntry
}
type Device struct {
  Name string
  MapperGroup string // if device belongs to a virtual block device
  Minor, Major int // Unreliable when taken from /proc/self/mountinfo
  FsUuid string  // Optional
  GptUuid string // Optional
}
type Filesystem struct {
  Uuid string
  Label string
  Devices []*Device
  Mounts []*MountEntry
}

type Linuxutil interface {
  // Returns true if this process is running with CAP_SYS_ADMIN privileges.
  // Many btrfs operations require this.
  IsCapSysAdmin() bool
  // The same as what you would get with `uname -a`.
  LinuxKernelVersion() (uint32, uint32)
  // The build version in the btrfs-progs header we are linking against.
  BtrfsProgsVersion() (uint32, uint32)
  // The git commit hash from which the current binary was built.
  ProjectVersion() string
  // Drops root privileges or dies if `seteuid` clib call fails.
  // Only works if go binary invoked via `sudo`.
  // Returns a function that can be called to restore back privileges.
  DropRoot() (func(), error)
  // Obtains root privileges back or dies if `seteuid` clib call fails.
  // Only works if go binary invoked via `sudo`.
  // Returns a function that can be called to restore user permissions.
  GetRoot() (func(), error)

  // Mounts the device and checks it got mounted at desired path.
  // If device is already mounted at target, this is a noop.
  // The device needs to be mountable by the user in /etc/fstab.
  // CAP_SYS_ADMIN will not be acquired.
  Mount(context.Context, string, string) (*MountEntry, error)
  UMount(context.Context, string) error
  // Returns all mounts found on the host that are backed by a block device.
  // Caveats:
  // * Bind mounts are NOT deduplicated.
  // * Mounts assotiated to multiple devices (ex: btrfs raid) will only have device assotiated
  // * Retrieves each mount `FsUUID` and `GptUUID` (if available: /dev/mapper does not have a GptUuid)
  ListBlockDevMounts() ([]*MountEntry, error)
  // Returns all btrfs filesystems found on the host.
  // Bind mounts to the same subvolume are deduplicated.
  // For each filesystem list all the mounts it owns.
  ListBtrfsFilesystems() ([]*Filesystem, error)
}

type Btrfsutil interface {
  // Get the `struct btrfs_util_subvolume_info` for a btrfs subvolume.
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  // @path must be the root of the subvolume.
  SubVolumeInfo(path string) (*pb.SubVolume, error)
  // Returns the btrfs filesystem ID for the subvolume that owns `path`.
  // Works for any path under the volume.
  SubVolumeIdForPath(path string) (uint64, error)
  // Returns an error unless `path` is the root of a btrfs subvolume.
  // It works even on the root subvolume.
  IsSubVolumeMountPath(path string) error
  // Returns the TreePath of a volume in its btrfs filesystem.
  // Requires the argument to have a valid MountedPath (it can work with a path inside the volume).
  // Requires CAP_SYS_ADMIN.
  GetSubVolumeTreePath(*pb.SubVolume) (string, error)
  // Returns a list with all subvolumes in the filesystem that owns `path`.
  // If `is_root_fs` then `path` must be the filesystem root and this method can be called without CAP_SYS_ADMIN.
  // Otherwise listing on non-root paths can only be done by root.
  // If the subvolume is not a snapshot then the corresponding fields will be empty.
  ListSubVolumesInFs(path string, is_root_fs bool) ([]*pb.SubVolume, error)
  // Reads a stream generated from `btrfs send --no-data` and returns a record of the operations.
  // Takes ownership of `read_pipe` and will close it once done.
  ReadAndProcessSendStream(dump ReadEndIf) (*SendDumpOperations, error)
  // Starts a separate `btrfs send` and returns the read end of the pipe.
  // `no_data` is the same option as for `btrfs send`.
  // `from` can be null to get the full contents of the subvolume.
  // When `ctx` is done/cancelled the write end of the pipe should be closed and the forked process killed.
  StartSendStream(ctx context.Context, from string, to string, no_data bool) (ReadEndIf, error)
  // Wrapper around `btrfs receive`. `to_dir` must exist and be a directory.
  // The mounted path of the received subvol will be `to_dir/<basename_src_subvol>`.
  // Takes ownership of `read_pipe` and will close it once done.
  ReceiveSendStream(ctx context.Context, to_dir string, read_pipe ReadEndIf) error
  // Calls `btrfs_util_create_snapshot()` to create a snapshot of `subvol` in `snap` path.
  // Sets the read-only flag.
  // Note async subvolume is no longer possible.
  CreateSnapshot(subvol string, snap string) error
  // Calls `btrfs_util_delete_subvolume` with empty `flags` argument.
  DeleteSubVolume(subvol string) error
  // Calls `btrfs_util_start_sync()` to wait for a transaction to sync.
  WaitForTransactionId(root_fs string, tid uint64) error
}

