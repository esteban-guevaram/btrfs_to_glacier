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

func (self *btrfsVolumeManager) GetChangesBetweenSnaps() (*pb.SnapshotChanges, error) {
  return nil, nil
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

