package shim

import (
  "fmt"
  "io/fs"
  "math/rand"
  fpmod "path/filepath"
  "os"
  "os/exec"
  "strings"

  "btrfs_to_glacier/util"
)

// Dependency injection for unittests.
type SysUtilIf interface {
  ReadAsciiFile(string, string, bool) (string, error)
  ReadDir(string) ([]os.DirEntry, error)
  IsDir(string) bool
  EvalSymlinks(string) (string, error)
  CombinedOutput(*exec.Cmd) ([]byte, error)
}
type SysUtilImpl struct {}

func (self *SysUtilImpl) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
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
  DirContent map[string][]os.DirEntry
  LinkTarget map[string]string
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

func (self *SysUtilMock) AddMount(fs_uuid string, dev string, target string) {
  min, maj := rand.Intn(256), rand.Intn(256)
  min_maj := fmt.Sprintf("%d:%d", min, maj)
  self.FileContent["/proc/self/mountinfo"] = fmt.Sprintf(
    "%s\n%d %d %s / %s rw bla - ext4 /dev/%s rw\n",
    self.FileContent["/proc/self/mountinfo"],
    rand.Intn(256), rand.Intn(256), min_maj, target, dev)

  self.DirContent["/dev/disk/by-partuuid"] = append(
    self.DirContent["/dev/disk/by-partuuid"],
    &DirEntry{ Leaf:"gpt-uuid-"+dev, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/disk/by-partuuid/gpt-uuid-"+dev] = "/dev/"+dev

  self.DirContent["/dev/disk/by-uuid"] = append(
    self.DirContent["/dev/disk/by-uuid"],
    &DirEntry{ Leaf:fs_uuid, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/disk/by-uuid/"+fs_uuid] = "/dev/"+dev

  self.DirContent["/dev/block"] = append(
    self.DirContent["/dev/block"],
    &DirEntry{ Leaf:min_maj, Mode:fs.ModeSymlink, })
  self.LinkTarget["/dev/block/"+min_maj] = "/dev/"+dev

  if _,found := self.DirContent["/sys/block"]; !found {
    self.DirContent["/sys/block"] = []os.DirEntry{}
  }
  if _,found := self.DirContent["/dev/mapper"]; !found {
    self.DirContent["/dev/mapper"] = []os.DirEntry{}
  }
}

func (self *SysUtilMock) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
  if cmd == nil { util.Fatalf("cmd == nil") }
  return nil, self.Err
}

