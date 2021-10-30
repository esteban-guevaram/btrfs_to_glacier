package mocks

import (
  "context"
  "fmt"
  "io"
  fpmod "path/filepath"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
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
func (self *Btrfsutil) GetSubVolumeTreePath(subvol *pb.SubVolume) (string, error) {
  if subvol.MountedPath == "" { return "", fmt.Errorf("GetSubvolumeTreePath bad args") }
  return fpmod.Base(subvol.MountedPath), self.Err
}
func (self *Btrfsutil) SubVolumeIdForPath(path string) (uint64, error) {
  if path == "" { return 0, fmt.Errorf("SubVolumeIdForPath bad args") }
  if path == self.Subvol.MountedPath { return self.Subvol.VolId, self.Err }
  return 0, fmt.Errorf("SubVolumeIdForPath nothing matched")
}
func (self *Btrfsutil) IsSubVolumeMountPath(path string) error {
  if path == "" { return fmt.Errorf("SubvolumeInfo bad args") }
  if path == self.Subvol.MountedPath { return self.Err }
  return fmt.Errorf("SubvolumeInfo nothing found for '%s'", path)
}
func (self *Btrfsutil) SubVolumeInfo(path string) (*pb.SubVolume, error) {
  if path == "" { return nil, fmt.Errorf("SubvolumeInfo bad args") }
  return self.Subvol, self.Err
}
func (self *Btrfsutil) ListSubVolumesInFs(path string, is_root_fs bool) ([]*pb.SubVolume, error) {
  if path == "" { return nil, fmt.Errorf("ListSubVolumesInFs bad args") }
  return self.Snaps, self.Err
}
func (self *Btrfsutil) ReadAndProcessSendStream(dump io.ReadCloser) (*types.SendDumpOperations, error) {
  return self.DumpOps, self.DumpErr
}
func (self *Btrfsutil) StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error) {
  if from == "" || to == "" { return nil, fmt.Errorf("StartSendStream bad args") }
  return self.SendStream.ReadEnd(), self.Err
}
func (self *Btrfsutil) CreateSnapshot(subvol string, snap string) error {
  if subvol == "" || snap == "" { return fmt.Errorf("CreateSnapshot bad args") }
  sv := util.DummySnapshot(uuid.NewString(), subvol)
  self.Snaps = append(self.Snaps, sv)
  return self.Err
}
func (self *Btrfsutil) DeleteSubVolume(subvol string) error {
  if subvol == "" { return fmt.Errorf("DeleteSubvolume bad args") }
  for idx,sv := range self.Snaps {
    util.Debugf("tree(%s) / del_path(%s)", sv.TreePath, subvol)
    // Cannot do a perfect job at matching since at this level we lost the uuid.
    if fpmod.Base(sv.TreePath) == fpmod.Base(subvol) {
      self.Snaps = append(self.Snaps[:idx], self.Snaps[idx+1:]...)
      return self.Err
    }
  }
  return fmt.Errorf("delete unexisting vol '%s'", subvol)
}
func (self *Btrfsutil) WaitForTransactionId(root_fs string, tid uint64) error {
  if root_fs == "" { return fmt.Errorf("WaitForTransactionId bad args") }
  return self.Err
}
func (self *Btrfsutil) ReceiveSendStream(ctx context.Context, to_dir string, read_pipe io.ReadCloser) error {
  defer read_pipe.Close()
  if to_dir == "" { return fmt.Errorf("ReceiveSendStream bad args") }
  _, err := io.Copy(io.Discard, read_pipe)
  if err != nil { return err }
  return self.Err
}

