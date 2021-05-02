package main

import (
  "context"
  "flag"
  "fmt"
  fpmod "path/filepath"
  "os"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

var path_flag string
var root_flag string
var snap1_flag string
var snap2_flag string

func init() {
  flag.StringVar(&path_flag, "subvol",  "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&root_flag, "rootvol", "", "the fullpath to the btrfs subvolume")
  flag.StringVar(&snap1_flag, "snap1",  "", "the fullpath to the oldest snapshot")
  flag.StringVar(&snap2_flag, "snap2",  "", "the fullpath to the latest snapshot")
}

func GetBtrfsUtil() types.Btrfsutil {
  var config *pb.Config
  var err error
  var btrfsutil types.Btrfsutil
  config, err = util.Load()
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
  btrfsutil, err = shim.NewBtrfsutil(config)
  if err != nil {
    util.Fatalf("integration failed = %v", err)
  }
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

func GetNewSnapName(leaf_name string) string {
  return fpmod.Join(fpmod.Dir(snap1_flag),
                    fmt.Sprintf("%s.%d", leaf_name, time.Now().Unix()))
}

func GetRestoreDir() string {
  restore_dir := fpmod.Join(root_flag,
                            fmt.Sprintf("restore.%d", time.Now().Unix()))
  err := os.Mkdir(restore_dir, os.ModePerm)
  if err != nil { panic(fmt.Sprintf("failed to create '%s': %v", restore_dir, err)) }
  return restore_dir
}

func TestBtrfsUtil_SubvolumeInfo(btrfsutil types.Btrfsutil) {
  subvol, err := btrfsutil.SubvolumeInfo(path_flag);
  if err != nil { util.Fatalf("integration failed = %v", err) }
  util.Infof("subvol = %s\n", subvol)
}

func TestBtrfsUtil_ListSubVolumesUnder(btrfsutil types.Btrfsutil) {
  var err error
  var vols []*pb.SubVolume
  vols, err = btrfsutil.ListSubVolumesUnder(root_flag)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  for _,subvol := range(vols) { util.Infof("subvol = %s\n", subvol) }
  util.Infof("len(vols) = %d\n", len(vols))
}

func TestBtrfsUtil_CreateSnapshot(btrfsutil types.Btrfsutil) {
  snap_path := GetNewSnapName("TestBtrfsUtil_CreateSnapshotAndWait")
  err := btrfsutil.CreateSnapshot(path_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", path_flag, snap_path, err) }

  subvol, err := btrfsutil.SubvolumeInfo(snap_path);
  if err != nil { util.Fatalf("btrfsutil.SubvolumeInfo failed = %v", err) }
  util.Infof("subvol = %s\n", subvol)
}

// Requires CAP_SYS_ADMIN
func TestBtrfsUtil_DeleteSubvolume(linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("TestBtrfsUtil_DeleteSubvolume needs CAP_SYS_ADMIN")
    return
  }
  snap_path := GetNewSnapName("TestBtrfsUtil_DeleteSubvolume")
  err := btrfsutil.CreateSnapshot(path_flag, snap_path);
  if err != nil { util.Fatalf("btrfsutil.CreateSnapshot(%s, %s) failed = %v", path_flag, snap_path, err) }

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
const received_uuid = "166518c08534784089b333f44a344ccb"

// Requires CAP_SYS_ADMIN
func TestBtrfsUtil_ReceiveSendStream(linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  if !linuxutil.IsCapSysAdmin() {
    util.Warnf("TestBtrfsUtil_ReceiveSendStream needs CAP_SYS_ADMIN")
    return
  }
  restore_dir := GetRestoreDir()
  preload_pipe := LoadPipeFromBase64SendData(send_stream)
  defer preload_pipe.Close()
  ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
  defer cancel()

  err := btrfsutil.ReceiveSendStream(ctx, restore_dir, preload_pipe.ReadEnd())
  if err != nil { util.Fatalf("btrfsutil.ReceiveSendStream failed = %v", err) }

  var vols []*pb.SubVolume
  vols, err = btrfsutil.ListSubVolumesUnder(root_flag)
  if err != nil { util.Fatalf("integration failed = %v", err) }
  for _,sv := range(vols) {
    if fpmod.Dir(sv.MountedPath) == restore_dir {
      if sv.ReceivedUuid == received_uuid {
        util.Infof("Received subvol: %v", sv)
        return
      }
      util.Fatalf("Created subvol has bad received uuid: %s/%v", received_uuid, sv)
    }
  }
  util.Fatalf("Could not find any volume in '%s'", restore_dir)
}


func TestBtrfsUtil_AllFuncs(linuxutil types.Linuxutil, btrfsutil types.Btrfsutil) {
  TestBtrfsUtil_SubvolumeInfo(btrfsutil)
  TestBtrfsUtil_ListSubVolumesUnder(btrfsutil)
  TestBtrfsUtil_CreateSnapshot(btrfsutil)
  TestBtrfsUtil_DeleteSubvolume(linuxutil, btrfsutil)
  TestBtrfsUtil_ReceiveSendStream(linuxutil, btrfsutil)
}

func TestLinuxUtils_AllFuncs(linuxutil types.Linuxutil) {
  util.Infof("IsCapSysAdmin = %v", linuxutil.IsCapSysAdmin())
  kmaj, kmin := linuxutil.LinuxKernelVersion()
  util.Infof("LinuxKernelVersion = %d.%d", kmaj, kmin)
  bmaj, bmin := linuxutil.BtrfsProgsVersion()
  util.Infof("BtrfsProgsVersion = %d.%d", bmaj, bmin)
  util.Infof("ProjectVersion = %s", linuxutil.ProjectVersion())
}

// Cannot use a test since Testing does not support cgo
func main() {
  util.Infof("shim_integration run")
  flag.Parse()
  btrfsutil := GetBtrfsUtil()
  linuxutil := GetLinuxUtil()
  TestBtrfsUtil_AllFuncs(linuxutil, btrfsutil)
  TestLinuxUtils_AllFuncs(linuxutil)
  TestSendDumpAll(btrfsutil)
  TestBtrfsSendStreamAll(linuxutil, btrfsutil)
  util.Infof("ALL DONE")
}

