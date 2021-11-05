package volume_source

import (
  "context"
  "fmt"
  "io/ioutil"
  "testing"
  "time"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
  "google.golang.org/protobuf/proto"
)

func CloneSubvol(vol *pb.SubVolume) *pb.SubVolume { return proto.Clone(vol).(*pb.SubVolume) }

func buildTestManager() (*btrfsVolumeManager, *mocks.Btrfsutil) {
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
  }
  sys_info := &pb.SystemInfo{
    KernMajor: 1,
    KernMinor: 2,
    BtrfsUsrMajor: 3,
    BtrfsUsrMinor: 4,
    ToolGitCommit: "hash",
  }
  mock_subvol := util.DummySubVolume("uuid")
  mock_subvol.GenAtCreation = 66
  mock_snap1 := util.DummySnapshot("uuid2", "uuid")
  mock_snap1.ReceivedUuid = "uuid4"
  mock_snap1.GenAtCreation = 99
  mock_snap2 := util.DummySnapshot("uuid3", "uuid")
  mock_snap2.GenAtCreation = 77
  mock_snap3 := util.DummySnapshot("uuid4", "uuid0")
  mock_snap3.GenAtCreation = 55
  mock_snap4 := util.DummySubVolume("uuid0")
  mock_snap4.GenAtCreation = 33

  source := &pb.Source{
    Type: pb.Source_BTRFS,
    Paths: []*pb.Source_VolSnapPathPair{
      &pb.Source_VolSnapPathPair{
        VolPath: mock_subvol.MountedPath,
        SnapPath: "/snaps",
      },
    },
    History: &pb.Source_SnapHistory{
      DaysKeepAll: 30,
      KeepOnePeriodDays: 30,
    },
  }

  conf := util.LoadTestConf()
  conf.Sources = []*pb.Source{ source }
  btrfsutil := &mocks.Btrfsutil {
    Subvols: []*pb.SubVolume{ CloneSubvol(mock_subvol), },
    Snaps: []*pb.SubVolume{
      CloneSubvol(mock_snap1),
      CloneSubvol(mock_snap2),
      CloneSubvol(mock_snap3),
      CloneSubvol(mock_snap4),
    },
    DumpOps: &newfile_ops,
    SendStream: mocks.NewPreloadedPipe([]byte("somedata")),
  }
  volmgr    := &btrfsVolumeManager {
    btrfsutil,
    sys_info,
    conf,
  }
  return volmgr, btrfsutil
}

func defaultVolPath(conf *pb.Config) string {
  return conf.Sources[0].Paths[0].VolPath
}

func TestGetVolume(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  expect_subvol := CloneSubvol(btrfsutil.Subvols[0])
  expect_subvol.OriginSys = proto.Clone(volmgr.sysinfo).(*pb.SystemInfo)
  subvol, err := volmgr.GetVolume(defaultVolPath(volmgr.conf))
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }
  // Just test we are not reusing internal references
  volmgr.sysinfo.ToolGitCommit = "another_hash"
  util.EqualsOrFailTest(t, "Using internal refs", expect_subvol, subvol)
}

func TestFindVolume(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  expect_subvol := CloneSubvol(btrfsutil.Snaps[0])
  subvol, err := volmgr.FindVolume(defaultVolPath(volmgr.conf),
                                   types.ByReceivedUuid(expect_subvol.ReceivedUuid))
  if err != nil { t.Fatalf("%s", err) }
  if !proto.Equal(expect_subvol, subvol) {
    t.Errorf("\n%s\n !=\n %s", expect_subvol, subvol)
  }

  subvol, err = volmgr.FindVolume(defaultVolPath(volmgr.conf),
                                  types.ByReceivedUuid("should not exist as a uuid"))
  if err != nil { t.Fatalf("%s", err) }
  if subvol != nil { t.Errorf("Expected to find no subvolume, got %s", subvol) }
}

func TestGetSnapshotSeqForVolume(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  expect_snaps := []*pb.SubVolume { CloneSubvol(btrfsutil.Snaps[1]), CloneSubvol(btrfsutil.Snaps[0]) }
  snapseq, err := volmgr.GetSnapshotSeqForVolume(btrfsutil.Subvols[0])
  if err != nil { t.Fatalf("%s", err) }
  if len(snapseq) != len(expect_snaps) {
    t.Fatalf("expected len %d got %d", len(expect_snaps), len(snapseq))
  }
  for idx, expect_snap := range expect_snaps {
    if !proto.Equal(expect_snap, snapseq[idx]) {
      t.Errorf("\n%s\n !=\n %s", expect_snap, snapseq[idx])
    }
  }
}

func TestGetChangesBetweenSnaps(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
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
      util.EqualsOrFailTest(t, "Bad changes", changes.Val, expect_changes)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestGetSnapshotStream(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  expect_stream := []byte("somedata")
  btrfsutil.SendStream = mocks.NewPreloadedPipe(expect_stream)

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
      util.EqualsOrFailTest(t, "Bad snap data", data, expect_stream)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestGetChangesBetweenSnaps_ErrStartingStream(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  btrfsutil.Err = fmt.Errorf("problemo")
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  ch, err := volmgr.GetChangesBetweenSnaps(ctx, btrfsutil.Snaps[0], btrfsutil.Snaps[1])
  if err == nil { t.Errorf("GetChangesBetweenSnaps expected error") }
  if ch != nil { t.Errorf("Expected nil channel on error") }
}

func TestGetChangesBetweenSnaps_ErrParsingStream(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  btrfsutil.DumpErr = fmt.Errorf("problemo")
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
  volmgr, btrfsutil := buildTestManager()
  expect_cnt := len(btrfsutil.Snaps) + 1
  sv := CloneSubvol(btrfsutil.Subvols[0])

  snapshot, err := volmgr.CreateSnapshot(sv)
  if err != nil { t.Fatalf("%s", err) }
  util.EqualsOrFailTest(t, "CreateSnapshot not called", len(btrfsutil.Snaps), expect_cnt)

  if len(snapshot.MountedPath) < 1 { t.Errorf("created snapshot should return mounted path.") }
  for _,snap := range btrfsutil.Snaps {
    var expect pb.SubVolume = *snap
    if snapshot.Uuid == expect.Uuid {
      expect.OriginSys = volmgr.sysinfo
      util.EqualsOrFailTest(t, "Bad snapshot", snapshot, &expect)
      return
    }
  }
  t.Errorf("Snapshot was not created")
}

func TestDeleteSnapshot(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  err := volmgr.DeleteSnapshot(btrfsutil.Subvols[0])
  if err == nil { t.Errorf("Expected error when deleting non-readonly subvolumes") }
  //util.Debugf("err: %v", err)
  btrfsutil.Snaps[0].MountedPath = fmt.Sprintf("/snaps/%s", btrfsutil.Snaps[0].TreePath)
  err = volmgr.DeleteSnapshot(btrfsutil.Snaps[0])
  if err != nil { t.Errorf("%s", err) }
}

func TestReceiveSendStream(t *testing.T) {
  volmgr, btrfsutil := buildTestManager()
  read_pipe := mocks.NewPreloadedPipe([]byte("somedata")).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  mock_received := &pb.SubVolume {
    Uuid: "uuid_TestReceiveSendStream",
    TreePath: "/tmp/mock_received",
    MountedPath: "/tmp/mock_received",
    GenAtCreation: 99,
    CreatedTs: uint64(time.Now().UnixNano() + 10000),
    ReceivedUuid: btrfsutil.Snaps[1].Uuid,
    ReadOnly: true,
  }
  btrfsutil.Snaps = append(btrfsutil.Snaps, mock_received)

  ch, err := volmgr.ReceiveSendStream(ctx, "/tmp", mock_received.ReceivedUuid, read_pipe)
  if err != nil { t.Errorf("ReceiveSendStream: %v", err) }
  select {
    case sv_or_error := <-ch:
      if sv_or_error.Err != nil { t.Errorf("ReceiveSendStream: %v", sv_or_error.Err) }
      util.EqualsOrFailTest(t, "Bad received subvol", sv_or_error.Val, mock_received)
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestReceiveSendStream_ErrNothingCreated(t *testing.T) {
  volmgr, _ := buildTestManager()
  read_pipe := mocks.NewPreloadedPipe([]byte("somedata")).ReadEnd()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  ch, err := volmgr.ReceiveSendStream(ctx, "/tmp", "uuid", read_pipe)
  if err != nil { t.Errorf("ReceiveSendStream: %v", err) }
  select {
    case sv_or_error := <-ch:
      if sv_or_error.Err == nil { t.Errorf("ReceiveSendStream expected error") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

