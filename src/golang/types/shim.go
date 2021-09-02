package types

import (
  "context"
  "io"
  pb "btrfs_to_glacier/messages"
)

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
  Err error
}

type Btrfsutil interface {
  // Get the `struct btrfs_util_subvolume_info` for a btrfs subvolume.
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  // @path must be the root of the subvolume.
  SubvolumeInfo(path string) (*pb.SubVolume, error)
  // Returns a list with all subvolumes under `path`.
  // If the subvolume is not a snapshot then the corresponding fields will be empty.
  // @path must be the root of the subvolume or root_volume.
  ListSubVolumesUnder(path string) ([]*pb.SubVolume, error)
  // Reads a stream generated from `btrfs send --no-data` and returns a record of the operations.
  // Takes ownership of `read_pipe` and will close it once done.
  ReadAndProcessSendStream(dump io.ReadCloser) *SendDumpOperations
  // Starts a separate `btrfs send` and returns the read end of the pipe.
  // `no_data` is the same option as for `btrfs send`.
  // `from` can be null to get the full contents of the subvolume.
  // When `ctx` is done/cancelled the write end of the pipe should be closed and the forked process killed.
  StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error)
  // Wrapper around `btrfs receive`
  // Takes ownership of `read_pipe` and will close it once done.
  ReceiveSendStream(ctx context.Context, to_dir string, read_pipe io.ReadCloser) error
  // Calls `btrfs_util_create_snapshot()` to create a snapshot of `subvol` in `snap` path.
  // Sets the read-only flag.
  // Note async subvolume is no longer possible.
  CreateSnapshot(subvol string, snap string) error
  // Calls `btrfs_util_delete_subvolume` with empty `flags` argument.
  DeleteSubvolume(subvol string) error
  // Calls `btrfs_util_start_sync()` to wait for a transaction to sync.
  WaitForTransactionId(root_fs string, tid uint64) error
}


type MockLinuxutil struct {
  IsAdmin bool
  SysInfo *pb.SystemInfo
}
func (self *MockLinuxutil) IsCapSysAdmin() bool { return self.IsAdmin }
func (self *MockLinuxutil) LinuxKernelVersion() (uint32, uint32) {
  return self.SysInfo.KernMajor, self.SysInfo.KernMinor
}
func (self *MockLinuxutil) BtrfsProgsVersion() (uint32, uint32) {
  return self.SysInfo.BtrfsUsrMajor, self.SysInfo.BtrfsUsrMinor
}
func (self *MockLinuxutil) ProjectVersion() string { return self.SysInfo.ToolGitCommit }


type MockBtrfsutil struct {
  Err error
  Subvol     *pb.SubVolume
  Snaps      []*pb.SubVolume
  DumpOps    *SendDumpOperations
  SendStream Pipe
}
func (self *MockBtrfsutil) SubvolumeInfo(path string) (*pb.SubVolume, error) {
  return self.Subvol, self.Err
}
func (self *MockBtrfsutil) ListSubVolumesUnder(path string) ([]*pb.SubVolume, error) {
  return self.Snaps, self.Err
}
func (self *MockBtrfsutil) ReadAndProcessSendStream(dump io.ReadCloser) *SendDumpOperations {
  return self.DumpOps
}
func (self *MockBtrfsutil) StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error) {
  return self.SendStream.ReadEnd(), self.Err
}
func (self *MockBtrfsutil) CreateSnapshot(subvol string, snap string) error {
  return self.Err
}
func (self *MockBtrfsutil) DeleteSubvolume(subvol string) error {
  return self.Err
}
func (self *MockBtrfsutil) WaitForTransactionId(root_fs string, tid uint64) error {
  return self.Err
}
func (self *MockBtrfsutil) ReceiveSendStream(ctx context.Context, to_dir string, read_pipe io.ReadCloser) error {
  return self.Err
}

