package mocks

import (
  "context"
  "io"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
)

type Linuxutil struct {
  IsAdmin bool
  SysInfo *pb.SystemInfo
}
func (self *Linuxutil) IsCapSysAdmin() bool { return self.IsAdmin }
func (self *Linuxutil) LinuxKernelVersion() (uint32, uint32) {
  return self.SysInfo.KernMajor, self.SysInfo.KernMinor
}
func (self *Linuxutil) BtrfsProgsVersion() (uint32, uint32) {
  return self.SysInfo.BtrfsUsrMajor, self.SysInfo.BtrfsUsrMinor
}
func (self *Linuxutil) ProjectVersion() string { return self.SysInfo.ToolGitCommit }


type Btrfsutil struct {
  Err        error
  DumpErr    error
  Subvol     *pb.SubVolume
  Snaps      []*pb.SubVolume
  DumpOps    *types.SendDumpOperations
  SendStream types.Pipe
}
func (self *Btrfsutil) SubvolumeInfo(path string) (*pb.SubVolume, error) {
  return self.Subvol, self.Err
}
func (self *Btrfsutil) ListSubVolumesUnder(path string) ([]*pb.SubVolume, error) {
  return self.Snaps, self.Err
}
func (self *Btrfsutil) ReadAndProcessSendStream(dump io.ReadCloser) (*types.SendDumpOperations, error) {
  return self.DumpOps, self.DumpErr
}
func (self *Btrfsutil) StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error) {
  return self.SendStream.ReadEnd(), self.Err
}
func (self *Btrfsutil) CreateSnapshot(subvol string, snap string) error {
  return self.Err
}
func (self *Btrfsutil) DeleteSubvolume(subvol string) error {
  return self.Err
}
func (self *Btrfsutil) WaitForTransactionId(root_fs string, tid uint64) error {
  return self.Err
}
func (self *Btrfsutil) ReceiveSendStream(ctx context.Context, to_dir string, read_pipe io.ReadCloser) error {
  return self.Err
}


