package types

import (
  "context"
  "io"
  pb "btrfs_to_glacier/messages"
)

type SnapshotChangesOrError struct {
  Val *pb.SnapshotChanges
  Err error
}

type SubVolumeOrError struct {
  Val *pb.SubVolume
  Err error
}

// The raw operations from a btrfs-send dump
type SendDumpOperations struct {
  Written map[string]bool
  New map[string]bool
  NewDir map[string]bool
  Deleted map[string]bool
  DelDir map[string]bool
  FromTo map[string]string
  ToUuid string
  FromUuid string
}

// Represents a line in /proc/self/mountinfo
type MountEntry struct {
  Id int // NOT stable across reads of mountinfo
  // Device major and minor may not match any real device.
  Minor, Major int
  TreePath string
  MountedPath string
  FsType string
  DevPath string
  Options map[string]string // correspond to the per-superblock options
  BtrfsVolId int
  // Bind mounts to the same filesystem/subvolume
  Binds []*MountEntry
}
type Device struct {
  Name string
  Minor, Major int
}
type Filesystem struct {
  Uuid string
  Label string
  MountedPath string
  Devices []*Device
  Mounts []*MountEntry
}

func ByUuid(uuid string) func(*pb.SubVolume) bool {
  return func(sv *pb.SubVolume) bool { return sv.Uuid == uuid }
}
func ByReceivedUuid(uuid string) func(*pb.SubVolume) bool {
  return func(sv *pb.SubVolume) bool { return sv.ReceivedUuid == uuid }
}

type VolumeManager interface {
  // `path` must be the root of the volume.
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  GetVolume(path string) (*pb.SubVolume, error)
  // Returns the first subvolume in filesystem owning `fs_path` that matches.
  // May return nil if nothing was found.
  FindVolume(fs_path string, matcher func(*pb.SubVolume) bool) (*pb.SubVolume, error)
  // Returns all snapshots whose parent is `subvol`.
  // Returned snaps are sorted by creation generation (oldest first).
  // `received_uuid` will only be set if the snapshot was effectibely received.
  GetSnapshotSeqForVolume(subvol *pb.SubVolume) ([]*pb.SubVolume, error)
  // Returns the changes between 2 snapshots of the same subvolume.
  // Both snaps must come from the same parent and `from` must be from a previous gen than `to`.
  GetChangesBetweenSnaps(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (<-chan SnapshotChangesOrError, error)
}

type VolumeSource interface {
  VolumeManager
  // Creates a read-only snapshot of `subvol`.
  // The path for the new snapshot will be determined by configuration.
  CreateSnapshot(subvol *pb.SubVolume) (*pb.SubVolume, error)
  // Create a pipe with the data from the delta between `from` and `to` snapshots.
  // `from` can be nil to get the full snapshot content.
  GetSnapshotStream(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (io.ReadCloser, error)
}

type VolumeDestination interface {
  VolumeManager
  // Reads subvolume data from the pipe and creates a subvolume using `btrfs receive`.
  // Received subvolume will be mounted at `root_path/<basename_src_subvol>`.
  // As a safety check this method asserts that received uuid equals `rec_uuid`.
  // Takes ownership of `read_pipe` and will close it once done.
  ReceiveSendStream(ctx context.Context, root_path string, rec_uuid string, read_pipe io.ReadCloser) (<-chan SubVolumeOrError, error)
}

type VolumeAdmin interface {
  VolumeManager
  // Deletes a snapshot. Returns an error if attempting to delete a write snapshot or subvolume.
  DeleteSnapshot(snap *pb.SubVolume) error
  // Goes through all snapshots fathered by `src_subvol` and deletes the oldest ones according to the parameters in the config.
  // Returns the list of snapshots deleted.
  // If there are no old snapshots this is a noop.
  TrimOldSnapshots(src_subvol *pb.SubVolume, dry_run bool) ([]*pb.SubVolume, error)
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
  // Returns all btrfs filesystems found on the host.
  // For each filesystem list all the mounts it owns.
  ListBtrfsFilesystems() ([]*Filesystem, error)
}

type Btrfsutil interface {
  // Get the `struct btrfs_util_subvolume_info` for a btrfs subvolume.
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  // @path must be the root of the subvolume.
  SubVolumeInfo(path string) (*pb.SubVolume, error)
  // Returns an error unless `path` is the root of a btrfs subvolume.
  // It works even on the root subvolume.
  IsSubVolumeMountPath(path string) error
  // Returns the TreePath of a volume in its btrfs filesystem.
  // Requires the argument to have a valid MountedPath (it can work we a path inside the volume).
  // Requires CAP_SYS_ADMIN.
  GetSubVolumeTreePath(*pb.SubVolume) (string, error)
  // Returns a list with all subvolumes in the filesystem that owns `path`.
  // If `is_root_fs` then `path` must be the filesystem root and this method can be called without CAP_SYS_ADMIN.
  // Otherwise listing on non-root paths can only be done by root.
  // If the subvolume is not a snapshot then the corresponding fields will be empty.
  ListSubVolumesInFs(path string, is_root_fs bool) ([]*pb.SubVolume, error)
  // Reads a stream generated from `btrfs send --no-data` and returns a record of the operations.
  // Takes ownership of `read_pipe` and will close it once done.
  ReadAndProcessSendStream(dump io.ReadCloser) (*SendDumpOperations, error)
  // Starts a separate `btrfs send` and returns the read end of the pipe.
  // `no_data` is the same option as for `btrfs send`.
  // `from` can be null to get the full contents of the subvolume.
  // When `ctx` is done/cancelled the write end of the pipe should be closed and the forked process killed.
  StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error)
  // Wrapper around `btrfs receive`. `to_dir` must exist and be a directory.
  // The mounted path of the received subvol will be `to_dir/<basename_src_subvol>`.
  // Takes ownership of `read_pipe` and will close it once done.
  ReceiveSendStream(ctx context.Context, to_dir string, read_pipe io.ReadCloser) error
  // Calls `btrfs_util_create_snapshot()` to create a snapshot of `subvol` in `snap` path.
  // Sets the read-only flag.
  // Note async subvolume is no longer possible.
  CreateSnapshot(subvol string, snap string) error
  // Calls `btrfs_util_delete_subvolume` with empty `flags` argument.
  DeleteSubVolume(subvol string) error
  // Calls `btrfs_util_start_sync()` to wait for a transaction to sync.
  WaitForTransactionId(root_fs string, tid uint64) error
}

