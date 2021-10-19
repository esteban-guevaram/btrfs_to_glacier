package main

import (
  "context"
  "fmt"
  "flag"
  fpmod "path/filepath"
  "os"
  "strings"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/volume_source/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

var root_flag string
var snap1_flag string
var snap2_flag string
var subvol_flag string
var subvol_alt_flag string

func init() {
  flag.StringVar(&root_flag, "rootvol", "", "the fullpath to the btrfs filesystem")
  flag.StringVar(&snap1_flag, "snap1",  "", "the fullpath to the oldest snapshot")
  flag.StringVar(&snap2_flag, "snap2",  "", "the fullpath to the latest snapshot")
  flag.StringVar(&subvol_flag, "subvol",  "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&subvol_alt_flag, "subvol-alt",  "", "another mount point only for the subvolume")
  flag.Parse()
}

type TestBtrfsUtil struct {
  conf *pb.Config
  btrfsutil types.Btrfsutil
  linuxutil types.Linuxutil
}

func GetConf() *pb.Config {
  conf := &pb.Config{}
  flags := []string{subvol_flag, subvol_alt_flag, root_flag, snap1_flag, snap2_flag,}
  for _,f := range flags {
    if f == "" { util.Fatalf("Bad flag value: '%s'", f) }
  }
  return conf
}

func GetBtrfsUtil(conf *pb.Config, linuxutil types.Linuxutil) types.Btrfsutil {
  var err error
  var btrfsutil types.Btrfsutil
  btrfsutil, err = shim.NewBtrfsutil(conf, linuxutil)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  return btrfsutil
}

func GetLinuxUtil() types.Linuxutil {
  var conf *pb.Config
  linuxutil, err := shim.NewLinuxutil(conf)
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  return linuxutil
}

func (self *TestBtrfsUtil) GetNewSnapName(leaf_name string) string {
  return fpmod.Join(fpmod.Dir(snap1_flag),
                    fmt.Sprintf("%s.%d", leaf_name, time.Now().Unix()))
}

func (self *TestBtrfsUtil) GetRestoreDir() string {
  restore_dir := fpmod.Join(root_flag,
                            fmt.Sprintf("restore.%d", time.Now().Unix()))
  err := os.Mkdir(restore_dir, os.ModePerm)
  if err != nil {
    if !os.IsExist(err) { util.Fatalf("failed to create '%s': %v", restore_dir, err) }
  }
  return restore_dir
}

func (self *TestBtrfsUtil) TestSubvolumeInfo(conf *pb.Config, btrfsutil types.Btrfsutil, path string) {
  subvol, err := btrfsutil.SubvolumeInfo(path)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  bad_vol := len(subvol.MountedPath) < 1 ||
             len(subvol.Uuid) < 1 || subvol.CreatedTs < 1
  if bad_vol { util.Fatalf("bad subvol = %s\n", util.AsJson(subvol)) }
  util.Infof("subvol = %s\n", util.AsJson(subvol))
}
func (self *TestBtrfsUtil) TestSubvolumeInfoFail(conf *pb.Config, btrfsutil types.Btrfsutil, path string) {
  _, err := btrfsutil.SubvolumeInfo(path)
  if err == nil { util.Fatalf("btrfsutil.SubvolumeInfo should have failed for '%s'", path) }
}

func (self *TestBtrfsUtil) TestListSubVolumesAt(
    conf *pb.Config, btrfsutil types.Btrfsutil, path string, is_root_fs bool) {
  if !self.linuxutil.IsCapSysAdmin() && !is_root_fs {
    util.Warnf("TestBtrfsUtil_ListSubVolumesAt needs CAP_SYS_ADMIN for non root paths")
    return
  }
  var err error
  var vols []*pb.SubVolume
  vols, err = btrfsutil.ListSubVolumesInFs(path, is_root_fs)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  if len(vols) < 1 { util.Fatalf("returned 0 vols: '%s'", path) }
  for _,subvol := range(vols) { util.Infof("%s\n", util.AsJson(subvol)) }
  util.Infof("len(vols) = %d\n", len(vols))
}

func (self *TestBtrfsUtil) TestCreateSnapshot(conf *pb.Config, btrfsutil types.Btrfsutil) {
  snap_path := self.GetNewSnapName("TestBtrfsUtil_CreateSnapshotAndWait")
  err := btrfsutil.CreateSnapshot(subvol_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", subvol_flag, snap_path, err) }

  subvol, err := btrfsutil.SubvolumeInfo(snap_path);
  if err != nil { util.Fatalf("btrfsutil.SubvolumeInfo failed = %v", err) }
  util.Infof("subvol = %s\n", subvol)
}

// Requires CAP_SYS_ADMIN
func (self *TestBtrfsUtil) TestDeleteSubvolume(conf *pb.Config, linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("TestBtrfsUtil_DeleteSubvolume needs CAP_SYS_ADMIN")
    return
  }
  snap_path := self.GetNewSnapName("TestBtrfsUtil_DeleteSubvolume")
  err := btrfsutil.CreateSnapshot(subvol_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", subvol_flag, snap_path, err) }

  err = btrfsutil.DeleteSubvolume(snap_path);
  if err != nil { util.Fatalf("btrfsutil.DeleteSubvolume(%s) failed = %v", snap_path, err) }

  var subvol *pb.SubVolume
  subvol, err = btrfsutil.SubvolumeInfo(snap_path);
  if err == nil { util.Fatalf("btrfsutil.DeleteSubvolume was not deleted: %v", subvol) }
}

const send_stream = `
YnRyZnMtc3RyZWFtAAEAAAAyAAAAAQBpJfHHDwAOAGFzdWJ2b2wuc25hcC4xAQAQABZlGMCFNHhA
ibMz9Eo0TMsCAAgABwAAAAAAAAAcAAAAEwDZwsOqDwAAAAYACADoAwAAAAAAAAcACADpAwAAAAAA
ABAAAAASAFlVdTUPAAAABQAIAO0BAAAAAAAANAAAABQAN8fP7Q8AAAALAAwAUV5nYAAAAAB5dbYD
CgAMAFFeZ2AAAAAAOqpLBgkADABRXmdgAAAAADqqSwYYAAAAAwBsbtsDDwAIAG8yNTctNy0wAwAI
AAEBAAAAAAAAGwAAAAkAGi4Hzg8ACABvMjU3LTctMBAACwBmaWxlLkwzQkFyajQAAAAUADfHz+0P
AAAACwAMAFFeZ2AAAAAAeXW2AwoADABRXmdgAAAAADqqSwYJAAwAUV5nYAAAAAA6qksGlAIAAA8A
FeqoZg8ACwBmaWxlLkwzQkFyahIACAAAAAAAAAAAABMAdQJnNywzCiInZmwrIgpRMWtYSwpqSVhl
ClFASWNERQo3cmI7JworOE5DPidceApvZGQ5Ci5cQkR8Ci4iWHYKV1lRewogIEkwCiR6N1MKd2Bz
eQpQPCV3CkxZYEkKUk8tWHQKUXpMXQogd2g+ZQo7YjpqCmBRY2sKLQlBQQppbHtWCitxID0KSyIk
Wwo7dEZ7Cm5dZjNEcwooRyZ3CmV4PzkKVlM0RApjfUtQCkJMOkYKPU91Swp0IG8pCj88cX0KaXUv
XQonVy1zCkA3NH1wRgotCSx6CnctNnYKaXVGSQp9ZzxqCktfd2wKall1SQpTXGh8Jwo0dWhOIQpo
bVM6JQptcEZACjtZV00KYCl5eVgKMXInSwo0P2RPCkFlejAKRSxeZFBPCioJYzkKV1hIQQp9bDRy
Cmx1J0AKfltWTAouUHR7CnouWDQKYEA9ICFSLQpMV25uCk5Bb0MKa2RUegpKfV8jCiojcjsiCkV2
CXJqSwpFT2ZfClJGeHQKTVBCOgprbXx5Cno/YU0KOmcwWQogUCRNVgpPSl09CiFeTTVeClA6Pj5V
Ck1nTmwKQGU/MApqclNeCm07OiUKKVp5LwolYn4yCiViUV8jQgo9O04lCnJxM00KUixjCQp9MnZx
CikjZDgKc3dqbwpqP3ksPGoKdUM4JGIKTikuJzIKIEZuSzMKOD4scgpQVjtfCnxfakkKW2h8SQop
OyVWCnlda0IKRkQpaAo/YSJ1CQphcl8wCj0mMiIKX35uPwpiLVhUCmRQWCZjcQouTiJyCnx3JFQK
b2g9UApoKDFpClktfScKOUVFYQpHTwl+ZT0KOGpxXgojOVVnCicAAAATALfmH3YPAAsAZmlsZS5M
M0JBcmoGAAgA6AMAAAAAAAAHAAgA6QMAAAAAAAAbAAAAEgB+Pj7mDwALAGZpbGUuTDNCQXJqBQAI
AKQBAAAAAAAAPwAAABQAg7nC8g8ACwBmaWxlLkwzQkFyagsADABRXmdgAAAAADqqSwYKAAwAUV5n
YAAAAAA6qksGCQAMAFFeZ2AAAAAAOqpLBgAAAAAVAFBsyZ0=
`
const received_name = "asubvol.snap.1"
const received_uuid = "166518c08534784089b333f44a344ccb"

// Requires CAP_SYS_ADMIN
func (self *TestBtrfsUtil) TestReceiveSendStream(conf *pb.Config, linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("TestBtrfsUtil_ReceiveSendStream needs CAP_SYS_ADMIN")
    return
  }
  restore_dir := self.GetRestoreDir()
  preload_pipe := LoadPipeFromBase64SendData(send_stream)
  defer preload_pipe.WriteEnd().Close()
  ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
  defer cancel()

  err := btrfsutil.ReceiveSendStream(ctx, restore_dir, preload_pipe.ReadEnd())
  if err != nil { util.Fatalf("btrfsutil.ReceiveSendStream failed = %v", err) }

  var vols []*pb.SubVolume
  vols, err = btrfsutil.ListSubVolumesInFs(root_flag, true)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  for _,sv := range(vols) {
    util.Debugf("subvol = %s", util.AsJson(sv))
    if strings.HasSuffix(sv.TreePath, received_name) && sv.ReceivedUuid == received_uuid {
      util.Infof("Received subvol: %v", sv)
      return
    }
  }
  util.Fatalf("Could not find any volume in '%s'", restore_dir)
}


func TestBtrfsUtil_AllFuncs(conf *pb.Config, linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  suite := &TestBtrfsUtil{
    conf: conf, btrfsutil: btrfsutil, linuxutil: linuxutil,
  }
  suite.TestSubvolumeInfo(conf, btrfsutil, subvol_flag)
  suite.TestSubvolumeInfo(conf, btrfsutil, subvol_alt_flag)
  suite.TestSubvolumeInfoFail(conf, btrfsutil, fpmod.Join(subvol_flag, "adir"))
  suite.TestListSubVolumesAt(conf, btrfsutil, root_flag, true)
  suite.TestListSubVolumesAt(conf, btrfsutil, subvol_flag, false)
  suite.TestListSubVolumesAt(conf, btrfsutil, subvol_alt_flag, false)
  suite.TestListSubVolumesAt(conf, btrfsutil, snap1_flag, false)
  suite.TestCreateSnapshot(conf, btrfsutil)
  suite.TestDeleteSubvolume(conf, linuxutil, btrfsutil)
  suite.TestReceiveSendStream(conf, linuxutil, btrfsutil)
}

// Cannot use a test since Testing does not support cgo
func main() {
  util.Infof("shim_integration run")
  conf := GetConf()
  linuxutil := GetLinuxUtil()
  btrfsutil := GetBtrfsUtil(conf, linuxutil)

  TestLinuxUtils_AllFuncs(linuxutil)
  TestBtrfsUtil_AllFuncs(conf, linuxutil, btrfsutil)
  TestSendDumpAll(btrfsutil)
  TestBtrfsSendStreamAll(linuxutil, btrfsutil)

  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("Some tests will only be run with CAP_SYS_ADMIN")
  }
  util.Infof("ALL DONE")
}

