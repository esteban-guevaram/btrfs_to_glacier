package local

import (
  "testing"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "google.golang.org/protobuf/proto"
)

func CloneSnap(snap *pb.Snapshot) *pb.Snapshot { return proto.Clone(snap).(*pb.Snapshot) }
func CloneSubvol(vol *pb.SubVolume) *pb.SubVolume { return proto.Clone(vol).(*pb.SubVolume) }

func buildTestManager() (*btrfsVolumeManager, *types.MockBtrfsutil, *types.MockLinuxutil) {
  conf := util.LoadTestConf()
  sys_info := &pb.SystemInfo{
    KernMajor: 1,
    KernMinor: 2,
    BtrfsUsrMajor: 3,
    BtrfsUsrMinor: 4,
    ToolGitCommit: "hash",
  }
  mock_subvol := &pb.SubVolume {
    Uuid: "uuid",
    MountedPath: "/tmp/subvol",
    GenAtCreation: 66,
    CreatedTs: uint64(time.Now().UnixNano()),
  }
  mock_snap1 := &pb.Snapshot {
    Subvol: &pb.SubVolume {
      Uuid: "uuid2",
      MountedPath: "/tmp/snap1",
      GenAtCreation: 99,
      CreatedTs: uint64(time.Now().UnixNano() + 10000),
    },
    ParentUuid: "uuid",
  }
  mock_snap2 := &pb.Snapshot {
    Subvol: &pb.SubVolume {
      Uuid: "uuid3",
      MountedPath: "/tmp/snap2",
      GenAtCreation: 77,
      CreatedTs: uint64(time.Now().UnixNano() + 5000),
    },
    ParentUuid: "uuid",
  }
  mock_snap3 := &pb.Snapshot {
    Subvol: &pb.SubVolume {
      Uuid: "uuid4",
      MountedPath: "/tmp/snap3",
      GenAtCreation: 55,
      CreatedTs: uint64(time.Now().UnixNano()),
    },
    ParentUuid: "uuid0",
  }
  mock_snap4 := &pb.Snapshot {
    Subvol: &pb.SubVolume {
      Uuid: "uuid0",
      MountedPath: "/tmp/subvol0",
      GenAtCreation: 33,
      CreatedTs: uint64(time.Now().UnixNano() - 10000),
    },
  }
  btrfsutil := &types.MockBtrfsutil {
    Subvol: CloneSubvol(mock_subvol),
    Snaps: []*pb.Snapshot{
      CloneSnap(mock_snap1),
      CloneSnap(mock_snap2),
      CloneSnap(mock_snap3),
      CloneSnap(mock_snap4),
    },
  }
  linuxutil := &types.MockLinuxutil {}
  volmgr    := &btrfsVolumeManager {
    btrfsutil,
    linuxutil,
    sys_info,
    conf,
  }
  return volmgr, btrfsutil, linuxutil
}

func TestGetVolume(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_subvol := CloneSubvol(btrfsutil.Subvol)
  expect_subvol.OriginSys = volmgr.sysinfo
  subvol, err := volmgr.GetVolume("")
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }
}

func TestGetSnapshotSeqForVolume(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_snaps := []*pb.Snapshot { CloneSnap(btrfsutil.Snaps[1]), CloneSnap(btrfsutil.Snaps[0]) }
  snapseq, err := volmgr.GetSnapshotSeqForVolume(btrfsutil.Subvol)
  if err != nil { t.Fatalf("%s", err) }
  if snapseq.Uuid != "" { t.Errorf("Expected empty snapseq uuid got %s", snapseq.Uuid) }
  if len(snapseq.Snaps) != len(expect_snaps) {
    t.Fatalf("expected len %d got %d", len(expect_snaps), len(snapseq.Snaps))
  }
  for idx, expect_snap := range expect_snaps {
    if !proto.Equal(expect_snap, snapseq.Snaps[idx]) {
      t.Errorf("\n%s\n !=\n %s", expect_snap, snapseq.Snaps[idx])
    }
  }
}

