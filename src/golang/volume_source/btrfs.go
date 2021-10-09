package volume_source

import (
  "context"
  "fmt"
  "io"
  fpmod "path/filepath"
  "sort"
  "time"
  "btrfs_to_glacier/volume_source/shim"
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

func (self *btrfsVolumeManager) GetVolume(path string) (*pb.SubVolume, error) {
  var subvol *pb.SubVolume
  var err error
  subvol, err = self.btrfsutil.SubvolumeInfo(path)
  if err != nil { return nil, err }
  if len(subvol.ParentUuid) > 0 && !subvol.ReadOnly {
    return nil, fmt.Errorf("'%s' is a writable snapshot, those are not supported", path)
  }
  clone := *self.sysinfo
  subvol.OriginSys = &clone
  return subvol, nil
}

func (self *btrfsVolumeManager) FindSnapRootForVol(sv *pb.SubVolume) (string, error) {
  return "", nil
}

func (self *btrfsVolumeManager) FindVolume(fs_root string, matcher func(*pb.SubVolume) bool) (*pb.SubVolume, error) {
  var vols []*pb.SubVolume
  var err error
  vols, err = self.btrfsutil.ListSubVolumesUnder(fs_root)
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
  var snap_root string
  snap_root, err = self.FindSnapRootForVol(subvol)
  if err != nil { return nil, err }
  vols, err = self.btrfsutil.ListSubVolumesUnder(snap_root)
  if err != nil { return nil, err }

  seq := make([]*pb.SubVolume, 0, 32)
  sort.Sort(ByCGen(vols))
  for _,vol := range vols {
    if vol.ParentUuid == subvol.Uuid {
      if last_gen == vol.GenAtCreation {
        panic("Found 2 snapshots with the same creation gen belong to same parent")
      }
      seq = append(seq, vol)
      last_gen = vol.GenAtCreation
    }
  }
  return seq, nil
}

func (self *btrfsVolumeManager) GetChangesBetweenSnaps(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (<-chan types.SnapshotChangesOrError, error) {
  if from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from.GenAtCreation < to.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.GenAtCreation, to.GenAtCreation)
  }
  read_end, err := self.btrfsutil.StartSendStream(ctx, from.MountedPath, to.MountedPath, true)
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
  if from != nil && from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from != nil && from.GenAtCreation < to.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.GenAtCreation, to.GenAtCreation)
  }
  from_path := ""
  if from != nil { from_path = from.MountedPath }
  read_end, err := self.btrfsutil.StartSendStream(ctx, from_path, to.MountedPath, false)
  return read_end, err
}

func (self *btrfsVolumeManager) CreateSnapshot(subvol *pb.SubVolume) (*pb.SubVolume, error) {
  var err error
  var snap_root string
  snap_root, err = self.FindSnapRootForVol(subvol)
  if err != nil { return nil, err }
  ts_str    := time.Now().Format("20060201")
  snap_name := fmt.Sprintf("%s.%s.%d", fpmod.Base(subvol.MountedPath), ts_str, time.Now().Unix())
  snap_path := fpmod.Join(snap_root, snap_name)
  err = self.btrfsutil.CreateSnapshot(subvol.MountedPath, snap_path)
  if err != nil { return nil, err }
  return self.GetVolume(snap_path)
}

func IsReadOnlySnap(subvol *pb.SubVolume) bool {
  return len(subvol.ParentUuid) > 0 && subvol.ReadOnly
}

func (self *btrfsVolumeManager) DeleteSnapshot(subvol *pb.SubVolume) error {
  re_read_sv, err := self.GetVolume(subvol.MountedPath)
  if err != nil { return err }
  if !IsReadOnlySnap(re_read_sv) {
    return fmt.Errorf("%v is not a snapshot", subvol)
  }
  err = self.btrfsutil.DeleteSubvolume(subvol.MountedPath)
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

