package shim

import (
  "fmt"
  "io/fs"
  "math/rand"
  fpmod "path/filepath"
  "os"
  "os/exec"
  "strings"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
)

// Dependency injection for unittests.
type SysUtilIf interface {
  ReadAsciiFile(string, string, bool) (string, error)
  ReadDir(string) ([]os.DirEntry, error)
  IsDir(string) bool
  Remove(string) error
  EvalSymlinks(string) (string, error)
  CombinedOutput(*exec.Cmd) ([]byte, error)
}
type SysUtilImpl struct {}

func (self *SysUtilImpl) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
  util.Debugf("%s %s", cmd.Path, strings.Join(cmd.Args, " "))
  return cmd.CombinedOutput()
}

func (self *SysUtilImpl) ReadDir(dir string) ([]os.DirEntry, error) {
  return os.ReadDir(dir)
}

func (self *SysUtilImpl) EvalSymlinks(path string) (string, error) {
  return fpmod.EvalSymlinks(path)
}

func (self *SysUtilImpl) ReadAsciiFile(
    dir string, name string, allow_ctrl bool) (string, error) {
  fpath := fpmod.Join(dir, name)
  //util.Debugf("Reading: '%s'", fpath)
  bytes, err := os.ReadFile(fpath)
  if err != nil { return "", err }
  str := strings.TrimRight(string(bytes), "\n")
  err = util.IsOnlyAsciiString(str, allow_ctrl)
  if err != nil { err = fmt.Errorf("file:'%s', err:%v", fpath, err) }
  return str, err
}

func (self *SysUtilImpl) IsDir(p string) bool {
  return util.IsDir(p)
}

func (self *SysUtilImpl) Remove(p string) error {
  return os.Remove(p)
}

type DirEntry struct {
  Leaf string
  Mode fs.FileMode
}

func (self *DirEntry) Name() string { return self.Leaf }
func (self *DirEntry) IsDir() bool { return fs.ModeDir & self.Mode != 0 }
func (self *DirEntry) Type() fs.FileMode { return self.Mode }
func (self *DirEntry) Info() (fs.FileInfo, error) { return nil, nil }

type SysUtilMock struct {
  FileContent map[string]string
  DirContent  map[string][]os.DirEntry
  LinkTarget  map[string]string
  CmdOutput   map[string]string
  Removed     map[string]bool
  RunOnCmd    func(*exec.Cmd)
  Err error
}

func (self *SysUtilMock) ReadAsciiFile(
    dir string, name string, allow_ctrl bool) (string, error) {
  path := fpmod.Join(dir, name)
  if content,found := self.FileContent[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w ReadAsciiFile '%s'", fs.ErrNotExist, path)
}

func (self *SysUtilMock) ReadDir(dir string) ([]os.DirEntry, error) {
  if content,found := self.DirContent[dir]; found {
    return content, self.Err
  }
  return nil, fmt.Errorf("%w ReadDir '%s'", fs.ErrNotExist, dir)
}

func (self *SysUtilMock) IsDir(dir string) bool {
  _,found := self.DirContent[dir]
  return found
}

func (self *SysUtilMock) EvalSymlinks(path string) (string, error) {
  if content,found := self.LinkTarget[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w EvalSymlinks '%s'", fs.ErrNotExist, path)
}

func (self *SysUtilMock) AddBtrfsFilesystem(
    fs_uuid string, devname string, target string) *types.Filesystem {
  label := uuid.NewString()
  mount := self.AddMount(fs_uuid, devname, target)
  self.DirContent[SYS_FS_BTRFS] = append(
    self.DirContent[SYS_FS_BTRFS],
    &DirEntry{ Leaf:fs_uuid, Mode:fs.ModeDir, })
  sys_path := fpmod.Join(SYS_FS_BTRFS, fs_uuid)
  self.DirContent[sys_path] = append(
    self.DirContent[sys_path],
    &DirEntry{ Leaf:SYS_FS_LABEL, Mode:0, },
    &DirEntry{ Leaf:SYS_FS_UUID, Mode:0, },
    &DirEntry{ Leaf:SYS_FS_DEVICE_DIR, Mode:fs.ModeDir, })
  sys_devs := fpmod.Join(sys_path, SYS_FS_DEVICE_DIR)
  self.DirContent[sys_devs] = append(
    self.DirContent[sys_devs],
    &DirEntry{ Leaf:mount.Device.Name, Mode:fs.ModeSymlink, })
  self.FileContent[sys_path + "/" + SYS_FS_LABEL] = label
  self.FileContent[sys_path + "/" + SYS_FS_UUID] = fs_uuid
  return &types.Filesystem{
    Uuid: fs_uuid,
    Label: label,
    Devices: []*types.Device{ mount.Device, },
    Mounts: []*types.MountEntry{ mount, },
  }
}

func (self *SysUtilMock) AddMount(
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

func (self *SysUtilMock) AddDevice(dev string) *types.Device {
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

func (self *SysUtilMock) AddLoopDev(dev string, backing_file string) {
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

func (self *SysUtilMock) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
  if cmd == nil { util.Fatalf("cmd == nil") }
  if self.RunOnCmd != nil { self.RunOnCmd(cmd) }
  output, found := self.CmdOutput[fpmod.Base(cmd.Path)]
  if !found && len(self.CmdOutput) > 0 {
    return nil, fmt.Errorf("cmd '%s' not expected", cmd.Path)
  }
  return []byte(output), self.Err
}

func (self *SysUtilMock) Remove(p string) error {
  self.Removed[p] = true
  return self.Err
}

