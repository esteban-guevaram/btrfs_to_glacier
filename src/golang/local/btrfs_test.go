package local

import (
  "context"
  "fmt"
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
  newfile_ops := types.SendDumpOperations{
    Written: map[string]bool{
      "coucou": true,
    },
    New: map[string]bool{
      "o260-8-0": true,
    },
    NewDir: map[string]bool{},
    Deleted: map[string]bool{},
    DelDir: map[string]bool{},
    FromTo: map[string]string{
      "o260-8-0": "coucou",
    },
    ToUuid: "72124d274e1e67428b7eed6909b00537",
    FromUuid: "5196e8fc2aa495499e24d9df818ba8c7",
    Err: nil,
  }
  sys_info := &pb.SystemInfo{
    KernMajor: 1,
    KernMinor: 2,
    BtrfsUsrMajor: 3,
    BtrfsUsrMinor: 4,
    ToolGitCommit: "hash",
  }
  mock_subvol := &pb.Snapshot {
    Subvol: &pb.SubVolume {
      Uuid: "uuid",
      MountedPath: "/tmp/subvol",
      GenAtCreation: 66,
      CreatedTs: uint64(time.Now().UnixNano()),
    },
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
    Subvol: CloneSnap(mock_subvol),
    Snaps: []*pb.Snapshot{
      CloneSnap(mock_snap1),
      CloneSnap(mock_snap2),
      CloneSnap(mock_snap3),
      CloneSnap(mock_snap4),
    },
    DumpOps: &newfile_ops,
    SendStream: types.NewMockPreloadedPipe([]byte("somedata")),
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
  expect_subvol := CloneSnap(btrfsutil.Subvol)
  expect_subvol.Subvol.OriginSys = volmgr.sysinfo
  subvol, err := volmgr.GetVolume("")
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }
}

func TestGetSnapshotSeqForVolume(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_snaps := []*pb.Snapshot { CloneSnap(btrfsutil.Snaps[1]), CloneSnap(btrfsutil.Snaps[0]) }
  snapseq, err := volmgr.GetSnapshotSeqForVolume(btrfsutil.Subvol.Subvol)
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

func TestGetChangesBetweenSnaps(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_changes := &pb.SnapshotChanges{
    FromUuid: btrfsutil.DumpOps.FromUuid,
    ToUuid: btrfsutil.DumpOps.ToUuid,
    Changes: []*pb.SnapshotChanges_Change{
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW,
        Path: "coucou",
      },
    },
  }
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
  ch, err := volmgr.GetChangesBetweenSnaps(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err != nil { t.Errorf("GetChangesBetweenSnaps: %v", err) }
  select {
    case changes := <-ch:
      if changes.Err != nil { t.Fatalf("GetChangesBetweenSnaps: %v", err) }
      util.CompareAsStrings(t, changes.Val, expect_changes)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestGetChangesBetweenSnaps_ErrStartingStream(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  btrfsutil.Err = fmt.Errorf("problemo")
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  ch, err := volmgr.GetChangesBetweenSnaps(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err == nil { t.Errorf("GetChangesBetweenSnaps expected error") }
  if ch != nil { t.Errorf("Expected nil channel on error") }
}

func TestGetChangesBetweenSnaps_ErrReadingStream(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  btrfsutil.SendStream.WriteEnd().PutErr(fmt.Errorf("problemo"))
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  ch, err := volmgr.GetChangesBetweenSnaps(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err != nil { t.Errorf("GetChangesBetweenSnaps: %v", err) }
  select {
    case changes := <-ch:
      if changes.Err == nil { t.Errorf("GetChangesBetweenSnaps expected error") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestGetChangesBetweenSnaps_ErrParsingStream(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  btrfsutil.DumpOps.Err = fmt.Errorf("problemo")
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  ch, err := volmgr.GetChangesBetweenSnaps(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err != nil { t.Errorf("GetChangesBetweenSnaps: %v", err) }
  select {
    case changes := <-ch:
      if changes.Err == nil { t.Errorf("GetChangesBetweenSnaps expected error") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestCreateSnapshot(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_snap := CloneSnap(btrfsutil.Subvol)
  expect_snap.Subvol.OriginSys = volmgr.sysinfo
  snapshot, err := volmgr.CreateSnapshot(btrfsutil.Subvol.Subvol)
  if err != nil { t.Fatalf("%s", err) }
  util.CompareAsStrings(t, snapshot, expect_snap)
}

