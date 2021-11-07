package volume_source

import (
  "context"
  "fmt"
  "io"
  fpmod "path/filepath"
  "sort"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_source/shim"
)

type btrfsVolumeManager struct {
  btrfsutil types.Btrfsutil
  juggler   types.BtrfsPathJuggler
  sysinfo   *pb.SystemInfo
  conf      *pb.Config
}

func get_system_info(linuxutil types.Linuxutil) *pb.SystemInfo {
  kern_major, kern_minor := linuxutil.LinuxKernelVersion()
  btrfs_major, btrfs_minor := linuxutil.BtrfsProgsVersion()
  return &pb.SystemInfo{
    KernMajor: kern_major,
    KernMinor: kern_minor,
    BtrfsUsrMajor: btrfs_major,
    BtrfsUsrMinor: btrfs_minor,
    ToolGitCommit: linuxutil.ProjectVersion(),
  }
}

func NewVolumeManager(
    conf *pb.Config, btrfsutil types.Btrfsutil, linuxutil types.Linuxutil,
    juggler types.BtrfsPathJuggler) (types.VolumeManager, error) {
  _, err := juggler.CheckSourcesAndReturnCorrespondingFs(conf.Sources)
  mgr := btrfsVolumeManager{
    btrfsutil,
    juggler,
    get_system_info(linuxutil),
    conf,
  }
  return &mgr, err
}

func (self *btrfsVolumeManager) GetVolume(path string) (*pb.SubVolume, error) {
  var subvol *pb.SubVolume
  var err error
  subvol, err = self.btrfsutil.SubVolumeInfo(path)
  if err != nil { return nil, err }
  if len(subvol.ParentUuid) > 0 && !subvol.ReadOnly {
    return nil, fmt.Errorf("'%s' is a writable snapshot, those are not supported", path)
  }
  clone := *self.sysinfo
  subvol.OriginSys = &clone
  return subvol, nil
}

func (self *btrfsVolumeManager) FindSnapPathForSubVolume(sv *pb.SubVolume) (string, error) {
  for _,src := range self.conf.Sources {
    if src.Type != pb.Source_BTRFS { continue }
    for _,p := range src.Paths {
      if p.VolPath == sv.MountedPath { return p.SnapPath, nil }
    }
  }
  return "", fmt.Errorf("no snap path for '%s'", sv.MountedPath)
}

func (self *btrfsVolumeManager) FindMountedPath(sv *pb.SubVolume) (string, error) {
  if len(sv.MountedPath) > 0 { return sv.MountedPath, nil }
  //return "", fmt.Errorf("implement me")
  return "/implement/me", nil
}

func (self *btrfsVolumeManager) FindVolume(fs_path string, matcher func(*pb.SubVolume) bool) (*pb.SubVolume, error) {
  _,mnt,_,err := self.juggler.FindFsAndTighterMountOwningPath(fs_path)
  if err != nil { return nil, err }
  vols, err := self.btrfsutil.ListSubVolumesInFs(mnt.MountedPath,
                                                 mnt.BtrfsVolId == shim.BTRFS_FS_TREE_OBJECTID)
  if err != nil { return nil, err }
  for _,vol := range vols {
    if matcher(vol) { return vol, nil }
  }
  return nil, nil
}

// Implement interface for sorter
type ByCGen []*pb.SubVolume
func (a ByCGen) Len() int           { return len(a) }
func (a ByCGen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByCGen) Less(i, j int) bool { return a[i].GenAtCreation < a[j].GenAtCreation }

func (self *btrfsVolumeManager) GetSnapshotSeqForVolume(subvol *pb.SubVolume) ([]*pb.SubVolume, error) {
  var vols []*pb.SubVolume
  var err error
  var last_gen uint64

  if len(subvol.MountedPath) < 1 { return nil, fmt.Errorf("GetSnapshotSeqForVolume needs MountedPath") }
  vols, err = self.btrfsutil.ListSubVolumesInFs(subvol.MountedPath,
                                                subvol.VolId == shim.BTRFS_FS_TREE_OBJECTID)
  if err != nil { return nil, err }

  seq := make([]*pb.SubVolume, 0, 32)
  sort.Sort(ByCGen(vols))
  for _,vol := range vols {
    if vol.ParentUuid == subvol.Uuid {
      if last_gen == vol.GenAtCreation {
        util.Fatalf("Found 2 snapshots with the same creation gen belonging to same parent")
      }
      seq = append(seq, vol)
      last_gen = vol.GenAtCreation
    }
  }
  return seq, nil
}

func (self *btrfsVolumeManager) GetChangesBetweenSnaps(
    ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (<-chan types.SnapshotChangesOrError, error) {
  var read_end io.ReadCloser
  var from_path, to_path string
  var err error
  if from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from.GenAtCreation < to.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.GenAtCreation, to.GenAtCreation)
  }
  if from_path,err = self.FindMountedPath(from); err != nil { return nil, err }
  if to_path,err = self.FindMountedPath(to); err != nil { return nil, err }

  read_end, err = self.btrfsutil.StartSendStream(ctx, from_path, to_path, true)
  if err != nil { return nil, err }

  changes_chan := make(chan types.SnapshotChangesOrError, 1)
  go func() {
    defer close(changes_chan)
    defer read_end.Close()
    dump_ops, err := self.btrfsutil.ReadAndProcessSendStream(read_end)
    if err != nil {
      changes_chan <- types.SnapshotChangesOrError{nil, err}
      return
    }
    changes_chan <- types.SnapshotChangesOrError{
      Val: sendDumpOpsToSnapChanges(dump_ops),
      Err: nil,
    }
  }()
  return changes_chan, nil
}

func (self *btrfsVolumeManager) GetSnapshotStream(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (io.ReadCloser, error) {
  var read_end io.ReadCloser
  var from_path, to_path string
  var err error
  if from != nil && from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from != nil && from.GenAtCreation < to.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.GenAtCreation, to.GenAtCreation)
  }
  from_path = ""
  if from != nil {
    if from_path,err = self.FindMountedPath(from); err != nil { return nil, err }
  }
  if to_path,err = self.FindMountedPath(to); err != nil { return nil, err }

  read_end, err = self.btrfsutil.StartSendStream(ctx, from_path, to_path, false)
  return read_end, err
}

func (self *btrfsVolumeManager) CreateSnapshot(subvol *pb.SubVolume) (*pb.SubVolume, error) {
  // If tree path == "" then it is not known or this is the root subvol
  if len(subvol.MountedPath) < 1 || len(subvol.TreePath) < 1 {
    return nil, fmt.Errorf("CreateSnapshot subvol needs MountedPath and TreePath")
  }
  if len(subvol.ParentUuid) > 0 { return nil, fmt.Errorf("CreateSnapshot from a snapshot") }

  snap_root, err := self.FindSnapPathForSubVolume(subvol)
  if err != nil { return nil, err }

  ts_str    := time.Now().Format("20060201")
  snap_name := fmt.Sprintf("%s.%s.%d",
                           fpmod.Base(subvol.TreePath), ts_str, time.Now().Unix())
  snap_path := fpmod.Join(snap_root, snap_name)

  err = self.btrfsutil.CreateSnapshot(subvol.MountedPath, snap_path)
  if err != nil { return nil, err }
  snap, err := self.GetVolume(snap_path)
  if err != nil { return nil, err }
  snap.MountedPath = snap_path
  return snap, nil
}

func IsReadOnlySnap(subvol *pb.SubVolume) bool {
  return len(subvol.ParentUuid) > 0 && subvol.ReadOnly
}

func (self *btrfsVolumeManager) DeleteSnapshot(subvol *pb.SubVolume) error {
  var err error
  var del_path string
  var re_read_sv *pb.SubVolume
  del_path, err = self.FindMountedPath(subvol)
  if err != nil { return err }

  re_read_sv, err = self.GetVolume(del_path)
  if err != nil { return err }
  if !IsReadOnlySnap(re_read_sv) {
    return fmt.Errorf("%v is not readonly", subvol)
  }
  err = self.btrfsutil.DeleteSubVolume(del_path)
  if err != nil { return err }
  return nil
}

func (self *btrfsVolumeManager) ReceiveSendStream(
    ctx context.Context, root_path string, rec_uuid string, read_pipe io.ReadCloser)  (<-chan types.SubVolumeOrError, error) {
  ch := make(chan types.SubVolumeOrError, 1)
  go func() {
    var err error
    var sv *pb.SubVolume
    defer close(ch)
    defer read_pipe.Close()

    err = self.btrfsutil.ReceiveSendStream(ctx, root_path, read_pipe)
    if err != nil {
      ch <- types.SubVolumeOrError{Err: err}
      return
    }
    sv, err = self.FindVolume(root_path, types.ByReceivedUuid(rec_uuid))
    if err != nil {
      ch <- types.SubVolumeOrError{Err: err}
      return
    }
    if sv == nil {
      ch <- types.SubVolumeOrError{Err: fmt.Errorf("No subvolume with received uuid '%s' got created", rec_uuid)}
      return
    }
    ch <- types.SubVolumeOrError{Val: sv}
  }()
  return ch, nil
}

func (self *btrfsVolumeManager) TrimOldSnapshots(
    src_subvol *pb.SubVolume, dry_run bool) ([]*pb.SubVolume, error) {
  return nil, nil
}

