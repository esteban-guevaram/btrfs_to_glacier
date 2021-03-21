package local

import (
  "fmt"
  "btrfs_to_glacier/shim"
  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
  "sort"
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
  if err != nil { return subvol, err }
  clone := *self.sysinfo
  subvol.OriginSys = &clone
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

func (self *btrfsVolumeManager) GetChangesBetweenSnaps(from *pb.Snapshot, to *pb.Snapshot) (*pb.SnapshotChanges, error) {
  if from.ParentUuid != to.ParentUuid {
    return nil, fmt.Errorf("Different parent uuid : '%s' != '%s'", from.ParentUuid, to.ParentUuid)
  }
  if from.Subvol.GenAtCreation < to.Subvol.GenAtCreation {
    return nil, fmt.Errorf("From is not older than To : '%d' / '%d'", from.Subvol.GenAtCreation, to.Subvol.GenAtCreation)
  }
  //send_ops, err := self.btrfsutil.ReadAndProcessSendStream()
  return nil, nil
}

// Implement interface for sorter
type ByPath []*pb.SnapshotChanges_Change
func (a ByPath) Len() int           { return len(a) }
func (a ByPath) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPath) Less(i, j int) bool { return a[i].Path < a[j].Path }

func SendDumpOpsToSnapChanges(state *types.SendDumpOperations) *pb.SnapshotChanges {
  changes := make([]*pb.SnapshotChanges_Change, 0, 64)
  for path,v := range state.New {
    if !v { continue }
    changes = append(changes, &pb.SnapshotChanges_Change {
      Type: pb.SnapshotChanges_NEW,
      Path: path,
    })
  }
  for path,v := range state.Written {
    if !v { continue }
    changes = append(changes, &pb.SnapshotChanges_Change {
      Type: pb.SnapshotChanges_WRITE,
      Path: path,
    })
  }
  for path,v := range state.Deleted {
    if !v { continue }
    changes = append(changes, &pb.SnapshotChanges_Change {
      Type: pb.SnapshotChanges_DELETE,
      Path: path,
    })
  }
  sort.Sort(ByPath(changes))
  proto := pb.SnapshotChanges{
    Changes: changes,
    ToUuid: state.ToUuid,
    FromUuid: state.FromUuid,
  }
  return &proto
}

