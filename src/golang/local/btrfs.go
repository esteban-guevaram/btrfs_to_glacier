package local

import (
  "context"
  "fmt"
  fpmod "path/filepath"
  "sort"
  "time"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
)

type btrfsVolumeManager struct {
  btrfsutil types.Btrfsutil
  linuxutil types.Linuxutil
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

func NewVolumeManager(conf *pb.Config) (types.VolumeManager, error) {
  var btrfsutil types.Btrfsutil
  var linuxutil types.Linuxutil
  var err error
  linuxutil, err = shim.NewLinuxutil(conf)
  if err != nil || !linuxutil.IsCapSysAdmin() {
    return nil, fmt.Errorf("To manage btrfs volumes you need CAP_SYS_ADMIN")
  }
  btrfsutil, err = shim.NewBtrfsutil(conf)
  mgr := btrfsVolumeManager{
    btrfsutil,
    linuxutil,
    get_system_info(linuxutil),
    conf,
  }
  return &mgr, err
}

func (self *btrfsVolumeManager) GetVolume(path string) (*pb.Snapshot, error) {
  var subvol *pb.Snapshot
  var err error
  subvol, err = self.btrfsutil.SubvolumeInfo(path)
  if err != nil { return subvol, err }
  clone := *self.sysinfo
  subvol.Subvol.OriginSys = &clone
  return subvol, nil
}

// Implement interface for sorter
type ByCGen []*pb.Snapshot
func (a ByCGen) Len() int           { return len(a) }
func (a ByCGen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByCGen) Less(i, j int) bool { return a[i].Subvol.GenAtCreation < a[j].Subvol.GenAtCreation }

func (self *btrfsVolumeManager) GetSnapshotSeqForVolume(subvol *pb.SubVolume) (*pb.SnapshotSeq, error) {
  var vols []*pb.Snapshot
  var err error
  var last_gen uint64
  vols, err = self.btrfsutil.ListSubVolumesUnder(self.conf.RootSnapPath)
  if err != nil { return nil, err }

  seq := pb.SnapshotSeq {
    Snaps: make([]*pb.Snapshot, 0, 32),
  }
  sort.Sort(ByCGen(vols))
  for _,vol := range vols {
    if vol.ParentUuid == subvol.Uuid {
      if last_gen == vol.Subvol.GenAtCreation {
        panic("Found 2 snapshots with the same creation gen belong to same parent")
      }
      seq.Snaps = append(seq.Snaps, vol)
      last_gen = vol.Subvol.GenAtCreation
    }
  }
  return &seq, nil
}

func (self *btrfsVolumeManager) GetChangesBetweenSnaps(ctx context.Context, from *pb.Snapshot, to *pb.Snapshot) (<-chan types.SnapshotChangesOrError, error) {
  if from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from.Subvol.GenAtCreation < to.Subvol.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.Subvol.GenAtCreation, to.Subvol.GenAtCreation)
  }
  read_end, err := self.btrfsutil.StartSendStream(ctx, from.Subvol.MountedPath, to.Subvol.MountedPath, true)
  if err != nil { return nil, err }

  changes_chan := make(chan types.SnapshotChangesOrError, 1)
  go func() {
    defer close(changes_chan)
    defer read_end.Close()
    dump_ops := self.btrfsutil.ReadAndProcessSendStream(read_end)
    if dump_ops.Err != nil || read_end.GetErr() != nil {
      err = fmt.Errorf("src_err:%v dst_err:%v", read_end.GetErr(), dump_ops.Err)
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

func (self *btrfsVolumeManager) GetSnapshotStream(ctx context.Context, from *pb.Snapshot, to *pb.Snapshot) (types.PipeReadEnd, error) {
  if from != nil && from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from != nil && from.Subvol.GenAtCreation < to.Subvol.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.Subvol.GenAtCreation, to.Subvol.GenAtCreation)
  }
  from_path := ""
  if from != nil { from_path = from.Subvol.MountedPath }
  read_end, err := self.btrfsutil.StartSendStream(ctx, from_path, to.Subvol.MountedPath, false)
  return read_end, err
}

func (self *btrfsVolumeManager) CreateSnapshot(subvol *pb.SubVolume) (*pb.Snapshot, error) {
  ts_str    := time.Now().Format("20060201")
  snap_name := fmt.Sprintf("%s.%s.%d", fpmod.Base(subvol.MountedPath), ts_str, time.Now().Unix())
  snap_path := fpmod.Join(self.conf.RootSnapPath, snap_name)
  err  := self.btrfsutil.CreateSnapshot(subvol.MountedPath, snap_path)
  if err != nil { return nil, err }
  return self.GetVolume(snap_path)
}

func (self *btrfsVolumeManager) DeleteSnapshot(subvol *pb.SubVolume) error {
  re_read_sv, err := self.GetVolume(subvol.MountedPath)
  if err != nil { return err }
  if len(re_read_sv.ParentUuid) < 1 {
    return fmt.Errorf("%v is not a snapshot", subvol)
  }
  err = self.btrfsutil.DeleteSubvolume(subvol.MountedPath)
  if err != nil { return err }
  return nil
}

