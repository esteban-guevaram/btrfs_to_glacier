package shim

import (
  "fmt"
  "io/fs"
  fpmod "path/filepath"
  "os"
  "strings"

  "btrfs_to_glacier/util"
)

// Dependency injection for unittests.
type FsReaderIf interface {
  ReadAsciiFile(string, string, bool) (string, error)
  ReadDir(string) ([]os.DirEntry, error)
  EvalSymlinks(string) (string, error)
}
type FsReaderImpl struct {}

func (self *FsReaderImpl) ReadDir(dir string) ([]os.DirEntry, error) {
  return os.ReadDir(dir)
}

func (self *FsReaderImpl) EvalSymlinks(path string) (string, error) {
  return fpmod.EvalSymlinks(path)
}

func (self *FsReaderImpl) ReadAsciiFile(
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

type DirEntry struct {
  Leaf string
  Mode fs.FileMode
}

func (self *DirEntry) Name() string { return self.Leaf }
func (self *DirEntry) IsDir() bool { return fs.ModeDir & self.Mode != 0 }
func (self *DirEntry) Type() fs.FileMode { return self.Mode }
func (self *DirEntry) Info() (fs.FileInfo, error) { return nil, nil }

type FsReaderMock struct {
  FileContent map[string]string
  DirContent map[string][]os.DirEntry
  LinkTarget map[string]string
  Err error
}

func (self *FsReaderMock) ReadAsciiFile(
    dir string, name string, allow_ctrl bool) (string, error) {
  path := fpmod.Join(dir, name)
  if content,found := self.FileContent[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w ReadAsciiFile '%s'", fs.ErrNotExist, path)
}

func (self *FsReaderMock) ReadDir(dir string) ([]os.DirEntry, error) {
  if content,found := self.DirContent[dir]; found {
    return content, self.Err
  }
  return nil, fmt.Errorf("%w ReadDir '%s'", fs.ErrNotExist, dir)
}

func (self *FsReaderMock) EvalSymlinks(path string) (string, error) {
  if content,found := self.LinkTarget[path]; found {
    return content, self.Err
  }
  return "", fmt.Errorf("%w EvalSymlinks '%s'", fs.ErrNotExist, path)
}

