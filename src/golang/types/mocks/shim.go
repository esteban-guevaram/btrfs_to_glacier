package mocks

import (
  "context"
  "fmt"
  "io"
  "io/fs"
  fpmod "path/filepath"
  "os"
  "os/exec"
  "math/rand"
  "strings"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type Linuxutil struct {
  ErrBase
  IsAdmin bool
  SysInfo *pb.SystemInfo
  Filesystems []*types.Filesystem
  Mounts      []*types.MountEntry
  UMounts     []*types.MountEntry
  Devs        []*types.Device
}

type LinuxCounts struct {
   Filesystems, Mounts, UMounts, Devs int
}

func NewLinuxutil() *Linuxutil {
  lu := &Linuxutil{}
  _  = (types.Linuxutil)(lu)
  lu.ForAllErr(nil)
  return lu
}

func (self *Linuxutil) IsCapSysAdmin() bool { return self.IsAdmin }
func (self *Linuxutil) LinuxKernelVersion() (uint32, uint32) {
  return self.SysInfo.KernMajor, self.SysInfo.KernMinor
}
func (self *Linuxutil) BtrfsProgsVersion() (uint32, uint32) {
  return self.SysInfo.BtrfsUsrMajor, self.SysInfo.BtrfsUsrMinor
}
func (self *Linuxutil) ProjectVersion() string { return self.SysInfo.ToolGitCommit }
func (self *Linuxutil) DropRoot() (func(), error) { return func() {}, self.ErrInject(self.DropRoot) }
func (self *Linuxutil) GetRoot() (func(), error) { return func() {}, self.ErrInject(self.GetRoot) }
func (self *Linuxutil) GetRootOrDie() func() {
  if self.ErrInject(self.GetRootOrDie) != nil { util.Fatalf("mocks.Linuxutil.GetRootOrDie") }
  return func() {}
}
func (self *Linuxutil) ListBtrfsFilesystems() ([]*types.Filesystem, error) {
  return self.Filesystems, self.ErrInject(self.ListBtrfsFilesystems)
}
func (self *Linuxutil) Mount(ctx context.Context, fs_uuid string, target string) (*types.MountEntry, error) {
  if !fpmod.HasPrefix(target, os.TempDir()) {
    return nil, fmt.Errorf("HasPrefix('%s', '%s')", target, os.TempDir())
  }
  if err := os.MkdirAll(target, fs.ModePerm); err != nil {
    return nil, fmt.Errorf("failed to create mount dir: %v", err)
  }
  mnt := &types.MountEntry{
    Device: &types.Device{ FsUuid: fs_uuid, },
    MountedPath: target,
  }
  self.Mounts = append(self.Mounts, mnt)
  return mnt, self.ErrInject(self.Mount)
}
func (self *Linuxutil) UMount(ctx context.Context, fs_uuid string) error {
  for i,m := range self.Mounts {
    if m.Device.FsUuid == fs_uuid {
      self.UMounts = append(self.UMounts, self.Mounts[i])
      self.Mounts = append(self.Mounts[:i], self.Mounts[i+1:]...);
      break
    }
  }
  return self.ErrInject(self.UMount)
}
func (self *Linuxutil) ListBlockDevMounts() ([]*types.MountEntry, error) {
  return self.Mounts, self.ErrInject(self.ListBlockDevMounts)
}
func (self *Linuxutil) CreateLoopDevice(
    ctx context.Context, size_mb uint64) (*types.Device, error) {
  dev := &types.Device{
    Name: "loop1",
    Minor: 0, Major: 213,
    GptUuid: uuid.NewString(),
    LoopFile: uuid.NewString(),
  }
  self.Devs = append(self.Devs, dev)
  return dev, self.ErrInject(self.CreateLoopDevice)
}
func (self *Linuxutil) DeleteLoopDevice(ctx context.Context, dev *types.Device) error {
  for i,d := range self.Devs {
    if d.LoopFile == dev.LoopFile { self.Devs = append(self.Devs[:i], self.Devs[i+1:]...); break }
  }
  return self.ErrInject(self.DeleteLoopDevice)
}
func (self *Linuxutil) CreateBtrfsFilesystem(
    ctx context.Context, dev *types.Device, label string, opts ...string) (*types.Filesystem, error) {
  fs := &types.Filesystem{
    Uuid: uuid.NewString(),
    Label: uuid.NewString(),
    Devices: []*types.Device{ dev },
  }
  self.Filesystems = append(self.Filesystems, fs)
  return fs, self.ErrInject(self.CreateBtrfsFilesystem)
}
func (self *Linuxutil) CleanMountDirs() {
  for _,m := range self.Mounts { util.RemoveAll(m.MountedPath) }
  for _,m := range self.UMounts { util.RemoveAll(m.MountedPath) }
}

func (self *Linuxutil) ObjCounts() LinuxCounts {
  return LinuxCounts{ Filesystems:len(self.Filesystems),
                      Mounts:len(self.Mounts),
                      UMounts:len(self.UMounts),
                      Devs:len(self.Devs), }
}
func (self LinuxCounts) Increment(
    filesystems int, mounts int, umounts int, devs int) LinuxCounts {
  self.Filesystems += filesystems
  self.Mounts += mounts
  self.UMounts += umounts
  self.Devs += devs
  return self
}


type Btrfsutil struct {
  Err        error
  DumpErr    error
  CreateDirs bool
  Subvols    []*pb.SubVolume
  Snaps      []*pb.SubVolume
  DumpOps    *types.SendDumpOperations
  SendStream types.Pipe
  SubvolCreateCallback func(*pb.SubVolume) error
}
type BtrfsCounts struct {
  Subvols, Snaps int
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
    if path == sv.MountedPath { return proto.Clone(sv).(*pb.SubVolume), self.Err }
  }
  for _,snap := range self.Snaps {
    if path == snap.MountedPath { return proto.Clone(snap).(*pb.SubVolume), self.Err }
  }
  return nil, fmt.Errorf("SubvolumeInfo nothing found for '%s'", path)
}
func (self *Btrfsutil) ListSubVolumesInFs(path string, is_root_fs bool) ([]*pb.SubVolume, error) {
  if !fpmod.IsAbs(path) { return nil, fmt.Errorf("ListSubVolumesInFs bad args") }
  list := append(self.Subvols, self.Snaps...)
  for i,sv := range list {
    list[i] = proto.Clone(sv).(*pb.SubVolume)
  }
  return list, self.Err
}
func (self *Btrfsutil) ReadAndProcessSendStream(dump types.ReadEndIf) (*types.SendDumpOperations, error) {
  return self.DumpOps, util.Coalesce(self.DumpErr, dump.GetErr())
}
func (self *Btrfsutil) StartSendStream(
    ctx context.Context, from string, to string, no_data bool) (types.ReadEndIf, error) {
  if from == "" || to == "" { return nil, fmt.Errorf("StartSendStream bad args") }
  return self.SendStream.ReadEnd(), self.Err
}
func (self *Btrfsutil) CreateSubvolume(sv_path string) error {
  sv := util.DummySubVolume(uuid.NewString())
  sv.MountedPath = sv_path
  if self.CreateDirs {
    if err := os.Mkdir(sv_path, fs.ModePerm); err != nil { return err }
    if self.SubvolCreateCallback != nil {
      if err := self.SubvolCreateCallback(sv); err != nil { return err }
    }
  }
  self.Subvols = append(self.Subvols, sv)
  //util.Debugf("mock.Btrfsutil.CreateSubvolume: %s", sv_path)
  return self.Err
}
func (self *Btrfsutil) CreateClone(sv_path string, clone_path string) error {
  return self.CreateSubvolume(clone_path)
}
func (self *Btrfsutil) CreateSnapshot(subvol string, snap string) error {
  if !fpmod.IsAbs(subvol) || !fpmod.IsAbs(snap) { return fmt.Errorf("CreateSnapshot bad args") }
  sv := util.DummySnapshot(uuid.NewString(), subvol)
  sv.MountedPath = snap
  if self.CreateDirs {
    //util.Debugf("mock.Btrfsutil.CreateSnapshot: %s", snap)
    if err := os.Mkdir(snap, fs.ModePerm); err != nil { return err }
    if self.SubvolCreateCallback != nil { return self.SubvolCreateCallback(sv) }
  }
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
func (self *Btrfsutil) ReceiveSendStream(
    ctx context.Context, to_dir string, read_pipe types.ReadEndIf) error {
  defer read_pipe.Close()
  if to_dir == "" { return fmt.Errorf("ReceiveSendStream bad args") }
  _, err := io.Copy(io.Discard, read_pipe)
  if err != nil { return err }
  return util.Coalesce(self.Err, read_pipe.GetErr())
}
// Includes subvolumes and snapshots
func (self *Btrfsutil) VolCount() int {
  return len(self.Subvols) + len(self.Snaps)
}

func (self *Btrfsutil) ObjCounts() BtrfsCounts {
  return BtrfsCounts{ Subvols:len(self.Subvols),
                      Snaps:len(self.Snaps), }
}
func (self BtrfsCounts) Increment(subvols int, snaps int) BtrfsCounts {
  self.Subvols += subvols
  self.Snaps += snaps
  return self
}

type SysUtil struct {
  FileContent map[string]string
  DirContent  map[string][]os.DirEntry
  LinkTarget  map[string]string
  CmdOutput   map[string]string
  Removed     map[string]bool
  RunOnCmd    func(*exec.Cmd)
  Err error
}

type DirEntry struct {
  Leaf string
  Mode fs.FileMode
}

func (self *DirEntry) Name() string { return self.Leaf }
func (self *DirEntry) IsDir() bool { return fs.ModeDir & self.Mode != 0 }
func (self *DirEntry) Type() fs.FileMode { return self.Mode }
func (self *DirEntry) Info() (fs.FileInfo, error) { return nil, nil }

func (self *SysUtil) ReadAsciiFile(
    dir string, name string, allow_ctrl bool) (string, error) {
  path := fpmod.Join(dir, name)
  if content,found := self.FileContent[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w ReadAsciiFile '%s'", fs.ErrNotExist, path)
}

func (self *SysUtil) ReadDir(dir string) ([]os.DirEntry, error) {
  if content,found := self.DirContent[dir]; found {
    return content, self.Err
  }
  return nil, fmt.Errorf("%w ReadDir '%s'", fs.ErrNotExist, dir)
}

func (self *SysUtil) IsDir(dir string) bool {
  _,found := self.DirContent[dir]
  return found
}

func (self *SysUtil) EvalSymlinks(path string) (string, error) {
  if content,found := self.LinkTarget[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w EvalSymlinks '%s'", fs.ErrNotExist, path)
}

func (self *SysUtil) AddBtrfsFilesystem(
    fs_uuid string, devname string, target string) *types.Filesystem {
  label := uuid.NewString()
  mount := self.AddMount(fs_uuid, devname, target)
  self.DirContent[types.SYS_FS_BTRFS] = append(
    self.DirContent[types.SYS_FS_BTRFS],
    &DirEntry{ Leaf:fs_uuid, Mode:fs.ModeDir, })
  sys_path := fpmod.Join(types.SYS_FS_BTRFS, fs_uuid)
  self.DirContent[sys_path] = append(
    self.DirContent[sys_path],
    &DirEntry{ Leaf:types.SYS_FS_LABEL, Mode:0, },
    &DirEntry{ Leaf:types.SYS_FS_UUID, Mode:0, },
    &DirEntry{ Leaf:types.SYS_FS_DEVICE_DIR, Mode:fs.ModeDir, })
  sys_devs := fpmod.Join(sys_path, types.SYS_FS_DEVICE_DIR)
  self.DirContent[sys_devs] = append(
    self.DirContent[sys_devs],
    &DirEntry{ Leaf:mount.Device.Name, Mode:fs.ModeSymlink, })
  self.FileContent[sys_path + "/" + types.SYS_FS_LABEL] = label
  self.FileContent[sys_path + "/" + types.SYS_FS_UUID] = fs_uuid
  return &types.Filesystem{
    Uuid: fs_uuid,
    Label: label,
    Devices: []*types.Device{ mount.Device, },
    Mounts: []*types.MountEntry{ mount, },
  }
}

func (self *SysUtil) AddMount(
    fs_uuid string, devname string, target string) *types.MountEntry {
  dev := self.AddDevice(devname)
  dev.FsUuid = fs_uuid
  id := rand.Intn(256)
  maj_min := fmt.Sprintf("%d:%d", dev.Major, dev.Minor)
  self.FileContent["/proc/self/mountinfo"] = fmt.Sprintf(
    "%s\n%d %d %s / %s rw bla - ext4 /dev/%s rw\n",
    self.FileContent["/proc/self/mountinfo"],
    id, rand.Intn(256), maj_min, target, devname)

  self.DirContent["/dev/disk/by-uuid"] = append(
    self.DirContent["/dev/disk/by-uuid"],
    &DirEntry{ Leaf:fs_uuid, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/disk/by-uuid/"+fs_uuid] = "/dev/"+devname
  return &types.MountEntry{
    Id: id,
    Device: dev,
    TreePath: "/",
    MountedPath: target, 
    FsType: "ext4",
    Options: map[string]string{},
  }
}

func (self *SysUtil) AddDevice(dev string) *types.Device {
  min, maj := rand.Intn(256), rand.Intn(256)
  maj_min := fmt.Sprintf("%d:%d", maj, min)

  self.DirContent["/dev/disk/by-partuuid"] = append(
    self.DirContent["/dev/disk/by-partuuid"],
    &DirEntry{ Leaf:"gpt-uuid-"+dev, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-"+dev] = "/dev/"+dev

  self.DirContent["/dev/block"] = append(
    self.DirContent["/dev/block"],
    &DirEntry{ Leaf:maj_min, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/block/"+maj_min] = "/dev/"+dev

  create_empty := []string{"/dev/disk/by-uuid", "/dev/mapper", "/sys/block", }
  for _,d := range create_empty {
    if _,found := self.DirContent[d]; !found { self.DirContent[d] = []os.DirEntry{} }
  }
  return &types.Device{
    Name: dev,
    Minor: min, Major: maj,
    GptUuid: "gpt-uuid-" + dev,
  }
}

func (self *SysUtil) AddLoopDev(dev string, backing_file string) {
  self.DirContent["/sys/block"] = append(self.DirContent["/sys/block"],
                                         &DirEntry{ Leaf:dev, Mode:fs.ModeSymlink, })
  self.DirContent["/sys/block/" + dev] = []os.DirEntry{
    &DirEntry{ Leaf:"loop", Mode:fs.ModeDir, },
  }
  self.DirContent["/sys/block/" + dev + "/loop"] = []os.DirEntry{
    &DirEntry{ Leaf:"backing_file", Mode:0, },
  }
  self.FileContent["/sys/block/" + dev + "/loop/backing_file"] = backing_file
}

func (self *SysUtil) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
  if cmd == nil { util.Fatalf("cmd == nil") }
  if self.RunOnCmd != nil { self.RunOnCmd(cmd) }
  output, found := self.CmdOutput[fpmod.Base(cmd.Path)]
  if !found && len(self.CmdOutput) > 0 {
    return nil, fmt.Errorf("cmd '%s' not expected", cmd.Path)
  }
  return []byte(output), self.Err
}

func (self *SysUtil) Remove(p string) error {
  self.Removed[p] = true
  return self.Err
}

