package mocks

import (
  "context"
  "fmt"
  "io"
  fpmod "path/filepath"
  "strings"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
)

type Linuxutil struct {
  Err error
  IsAdmin bool
  SysInfo *pb.SystemInfo
  Filesystems []*types.Filesystem
  Mounts      []*types.MountEntry
}
func (self *Linuxutil) IsCapSysAdmin() bool { return self.IsAdmin }
func (self *Linuxutil) LinuxKernelVersion() (uint32, uint32) {
  return self.SysInfo.KernMajor, self.SysInfo.KernMinor
}
func (self *Linuxutil) BtrfsProgsVersion() (uint32, uint32) {
  return self.SysInfo.BtrfsUsrMajor, self.SysInfo.BtrfsUsrMinor
}
func (self *Linuxutil) ProjectVersion() string { return self.SysInfo.ToolGitCommit }
func (self *Linuxutil) DropRoot() (func(), error) { return func() {}, self.Err }
func (self *Linuxutil) GetRoot() (func(), error) { return func() {}, self.Err }
func (self *Linuxutil) ListBtrfsFilesystems() ([]*types.Filesystem, error) {
  return self.Filesystems, self.Err
}
func (self *Linuxutil) Mount(ctx context.Context, fs_uuid string, target string) (*types.MountEntry, error) {
  mnt := &types.MountEntry{
    Device: &types.Device{ FsUuid: fs_uuid, },
    MountedPath: target,
  }
  return mnt, self.Err
}
func (self *Linuxutil) UMount(ctx context.Context, fs_uuid string) error {
  return self.Err
}
func (self *Linuxutil) ListBlockDevMounts() ([]*types.MountEntry, error) {
  return self.Mounts, self.Err
}

type Btrfsutil struct {
  Err        error
  DumpErr    error
  Subvols    []*pb.SubVolume
  Snaps      []*pb.SubVolume
  DumpOps    *types.SendDumpOperations
  SendStream types.Pipe
}
func (self *Btrfsutil) GetSubVolumeTreePath(subvol *pb.SubVolume) (string, error) {
  if !fpmod.IsAbs(subvol.MountedPath) { return "", fmt.Errorf("GetSubvolumeTreePath bad args") }
  return fpmod.Base(subvol.MountedPath), self.Err
}
func (self *Btrfsutil) SubVolumeIdForPath(path string) (uint64, error) {
  if !fpmod.IsAbs(path) { return 0, fmt.Errorf("SubVolumeIdForPath bad args") }
  var candidate_sv *pb.SubVolume
  for _,sv := range self.Subvols {
    if strings.HasPrefix(path, sv.MountedPath) {
      if candidate_sv == nil { candidate_sv = sv }
      if len(candidate_sv.MountedPath) < len(sv.MountedPath) { candidate_sv = sv }
    }
  }
  if candidate_sv != nil { return candidate_sv.VolId, self.Err }

  candidate_sv = nil
  for _,sv := range self.Snaps {
    if strings.HasPrefix(path, sv.MountedPath) {
      if candidate_sv == nil { candidate_sv = sv }
      if len(candidate_sv.MountedPath) < len(sv.MountedPath) { candidate_sv = sv }
    }
  }
  if candidate_sv != nil { return candidate_sv.VolId, self.Err }
  return 0, fmt.Errorf("SubVolumeIdForPath nothing matched")
}
func (self *Btrfsutil) IsSubVolumeMountPath(path string) error {
  if !fpmod.IsAbs(path) { return fmt.Errorf("IsSubVolumeMountPath bad args") }
  for _,sv := range self.Subvols {
    if path == sv.MountedPath { return self.Err }
  }
  for _,snap := range self.Snaps {
    if path == snap.MountedPath { return self.Err }
  }
  return fmt.Errorf("IsSubVolumeMountPath nothing found for '%s'", path)
}
func (self *Btrfsutil) SubVolumeInfo(path string) (*pb.SubVolume, error) {
  if !fpmod.IsAbs(path) { return nil, fmt.Errorf("SubvolumeInfo bad args") }
  for _,sv := range self.Subvols {
    if path == sv.MountedPath { return sv, self.Err }
  }
  for _,snap := range self.Snaps {
    if path == snap.MountedPath { return snap, self.Err }
  }
  return nil, fmt.Errorf("SubvolumeInfo nothing found for '%s'", path)
}
func (self *Btrfsutil) ListSubVolumesInFs(path string, is_root_fs bool) ([]*pb.SubVolume, error) {
  if !fpmod.IsAbs(path) { return nil, fmt.Errorf("ListSubVolumesInFs bad args") }
  return append(self.Subvols, self.Snaps...), self.Err
}
func (self *Btrfsutil) ReadAndProcessSendStream(dump io.ReadCloser) (*types.SendDumpOperations, error) {
  return self.DumpOps, self.DumpErr
}
func (self *Btrfsutil) StartSendStream(ctx context.Context, from string, to string, no_data bool) (io.ReadCloser, error) {
  if from == "" || to == "" { return nil, fmt.Errorf("StartSendStream bad args") }
  return self.SendStream.ReadEnd(), self.Err
}
func (self *Btrfsutil) CreateSnapshot(subvol string, snap string) error {
  if !fpmod.IsAbs(subvol) || !fpmod.IsAbs(snap) { return fmt.Errorf("CreateSnapshot bad args") }
  sv := util.DummySnapshot(uuid.NewString(), subvol)
  sv.MountedPath = snap
  self.Snaps = append(self.Snaps, sv)
  return self.Err
}
func (self *Btrfsutil) DeleteSubVolume(subvol string) error {
  if subvol == "" { return fmt.Errorf("DeleteSubvolume bad args") }
  for idx,sv := range self.Snaps {
    //util.Debugf("tree(%s) / del_path(%s)", sv.TreePath, subvol)
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

