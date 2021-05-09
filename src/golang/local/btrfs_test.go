package local

import (
  "context"
  "fmt"
  "io/ioutil"
  "testing"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  "google.golang.org/protobuf/proto"
)

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
  mock_subvol := &pb.SubVolume {
    Uuid: "uuid",
    MountedPath: "/tmp/subvol",
    GenAtCreation: 66,
    CreatedTs: uint64(time.Now().UnixNano()),
  }
  mock_snap1 := &pb.SubVolume {
    Uuid: "uuid2",
    MountedPath: "/tmp/snap1",
    GenAtCreation: 99,
    CreatedTs: uint64(time.Now().UnixNano() + 10000),
    ParentUuid: "uuid",
    ReceivedUuid: "uuid4",
    ReadOnly: true,
  }
  mock_snap2 := &pb.SubVolume {
    Uuid: "uuid3",
    MountedPath: "/tmp/snap2",
    GenAtCreation: 77,
    CreatedTs: uint64(time.Now().UnixNano() + 5000),
    ParentUuid: "uuid",
    ReadOnly: true,
  }
  mock_snap3 := &pb.SubVolume {
    Uuid: "uuid4",
    MountedPath: "/tmp/snap3",
    GenAtCreation: 55,
    CreatedTs: uint64(time.Now().UnixNano()),
    ParentUuid: "uuid0",
    ReadOnly: true,
  }
  mock_snap4 := &pb.SubVolume {
    Uuid: "uuid0",
    MountedPath: "/tmp/subvol0",
    GenAtCreation: 33,
    CreatedTs: uint64(time.Now().UnixNano() - 10000),
  }
  btrfsutil := &types.MockBtrfsutil {
    Subvol: CloneSubvol(mock_subvol),
    Snaps: []*pb.SubVolume{
      CloneSubvol(mock_snap1),
      CloneSubvol(mock_snap2),
      CloneSubvol(mock_snap3),
      CloneSubvol(mock_snap4),
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
  expect_subvol := CloneSubvol(btrfsutil.Subvol)
  expect_subvol.OriginSys = volmgr.sysinfo
  subvol, err := volmgr.GetVolume("")
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }
}

func TestFindVolume(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_subvol := CloneSubvol(btrfsutil.Snaps[0])
  subvol, err := volmgr.FindVolume("", types.ByReceivedUuid(expect_subvol.ReceivedUuid))
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }

  subvol, err = volmgr.FindVolume("", types.ByReceivedUuid("should not exist as a uuid"))
  if err != nil { t.Fatalf("%s", err) }
  if subvol != nil { t.Errorf("Expected to find no subvolume, got %s", subvol) }
}

func TestGetSnapshotSeqForVolume(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_snaps := []*pb.SubVolume { CloneSubvol(btrfsutil.Snaps[1]), CloneSubvol(btrfsutil.Snaps[0]) }
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
      util.EqualsOrFailTest(t, changes.Val, expect_changes)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestGetSnapshotStream(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  expect_stream := []byte("somedata")
  btrfsutil.SendStream = types.NewMockPreloadedPipe(expect_stream)

  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  read_pipe, err := volmgr.GetSnapshotStream(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err != nil { t.Errorf("GetSnapshotStream: %v", err) }

  done := make(chan []byte)
  go func() {
    defer close(done)
    defer read_pipe.Close()
    data, err := ioutil.ReadAll(read_pipe)
    if err != nil { t.Errorf("GetSnapshotStream bad stream data: %v", err) }
    done <- data
  }()

  select {
    case data := <-done:
      util.EqualsOrFailTest(t, data, expect_stream)
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
  expect_snap := CloneSubvol(btrfsutil.Subvol)
  expect_snap.OriginSys = volmgr.sysinfo
  snapshot, err := volmgr.CreateSnapshot(btrfsutil.Subvol)
  if err != nil { t.Fatalf("%s", err) }
  util.EqualsOrFailTest(t, snapshot, expect_snap)
}

func TestDeleteSnapshot(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  err := volmgr.DeleteSnapshot(btrfsutil.Subvol)
  if err == nil { t.Errorf("Expected error when deleting non-readonly subvolumes") }
  btrfsutil.Subvol = CloneSubvol(btrfsutil.Snaps[0])
  err = volmgr.DeleteSnapshot(btrfsutil.Snaps[0])
  if err != nil { t.Errorf("%s", err) }
}

func TestReceiveSendStream(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  read_pipe := types.NewMockPreloadedPipe([]byte("somedata")).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  mock_received := &pb.SubVolume {
    Uuid: "uuid_TestReceiveSendStream",
    MountedPath: "/tmp/mock_received",
    GenAtCreation: 99,
    CreatedTs: uint64(time.Now().UnixNano() + 10000),
    ReceivedUuid: btrfsutil.Snaps[1].Uuid,
    ReadOnly: true,
  }
  btrfsutil.Snaps = append(btrfsutil.Snaps, mock_received)

  ch, err := volmgr.ReceiveSendStream(ctx, btrfsutil.Snaps[1], read_pipe)
  if err != nil { t.Errorf("ReceiveSendStream: %v", err) }
  select {
    case sv_or_error := <-ch:
      if sv_or_error.Err != nil { t.Errorf("ReceiveSendStream: %v", sv_or_error.Err) }
      util.EqualsOrFailTest(t, sv_or_error.Val, mock_received)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestReceiveSendStream_ErrNothingCreated(t *testing.T) {
  volmgr, btrfsutil, _ := buildTestManager()
  read_pipe := types.NewMockPreloadedPipe([]byte("somedata")).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  ch, err := volmgr.ReceiveSendStream(ctx, btrfsutil.Snaps[1], read_pipe)
  if err != nil { t.Errorf("ReceiveSendStream: %v", err) }
  select {
    case sv_or_error := <-ch:
      if sv_or_error.Err == nil { t.Errorf("ReceiveSendStream expected error") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

