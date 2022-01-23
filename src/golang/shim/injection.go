package shim

import (
  "fmt"
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

