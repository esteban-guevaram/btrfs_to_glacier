package restore_manager

import (
  "context"
  "fmt"
  "os"
  fpmod "path/filepath"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
)

const (
  ChunksPerSnap = 3
)

type Mocks struct {
  DstConf *pb.Restore
  Meta    *mocks.Metadata
  Store   *mocks.Storage
  Destination *mocks.VolumeManager
}

func (self *Mocks) AddFirstSnapsFromMetaInDst(seq_uuid string, count int) []*pb.SubVolume {
  added := make([]*pb.SubVolume, 0)
  for i,snap_uuid := range self.Meta.Seqs[seq_uuid].SnapUuids {
    if i >= count { break }
    snap := self.Meta.Snaps[snap_uuid]
    mnt_path := fpmod.Join(self.DstConf.RootRestorePath, snap.Uuid)
    added = append(added, self.Destination.CreateReceivedFrom(snap, mnt_path))
  }
  return added
}

func buildConfRestores(dst_count int) []*pb.Restore {
  dsts := []*pb.Restore{}
  for i:=0; i<dst_count; i+=1 {
    name := uuid.NewString()
    dst := &pb.Restore{
      Type: pb.Restore_BTRFS,
      Name: name,
      RootRestorePath: fpmod.Join(os.TempDir(), name),
    }
    dsts = append(dsts, dst)
  }
  return dsts
}

func buildRestoreManager(head_cnt int) (*RestoreManager, *Mocks) {
  const dst_count = 3
  conf := util.LoadTestConf()
  conf.Restores = buildConfRestores(dst_count)
  meta, store := mocks.DummyMetaAndStorage(head_cnt, head_cnt, head_cnt, ChunksPerSnap)
  dest := mocks.NewVolumeManager()
  mocks := &Mocks{
    DstConf: conf.Restores[0],
    Meta: meta,
    Store: store,
    Destination: dest,
  }
  mgr, err := NewRestoreManager(conf, conf.Restores[0].Name, meta, store, dest)
  if err != nil { util.Fatalf("NewRestoreManager: %v", err) }
  real_mgr := mgr.(*RestoreManager)
  real_mgr.BetweenRestoreChecks = util.SmallTimeout
  return real_mgr, mocks
}

func TestReadHeadAndSequenceMap(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/3)
  head_seq, err := mgr.ReadHeadAndSequenceMap(ctx)
  if err != nil { util.Fatalf("ReadHeadAndSequenceMap: %v", err) }
  util.EqualsOrFailTest(t, "bad head count", len(head_seq), len(mocks.Meta.Heads))
  for _,val := range head_seq {
    util.EqualsOrFailTest(t, "bad current seq", val.Cur, mocks.Meta.Seqs[val.Head.CurSeqUuid])
  }
}

func HelperTestRestoreCurrentSequence_Ok(t *testing.T, seq_len int, prev_len int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/seq_len)
  expect_meta_cnt := mocks.Meta.ObjCounts()
  vol_uuid := mocks.Meta.HeadKeys()[1]
  cur_uuid := mocks.Meta.Heads[vol_uuid].CurSeqUuid
  mocks.AddFirstSnapsFromMetaInDst(cur_uuid, prev_len)
  expect_dst_cnt := mocks.Destination.ObjCounts().IncReceived(seq_len-prev_len)

  pairs, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
  if err != nil { t.Fatalf("RestoreCurrentSequence: %v", err) }

  util.EqualsOrFailTest(t, "Should not create new objects", mocks.Meta.ObjCounts(), expect_meta_cnt)
  util.EqualsOrFailTest(t, "Bad restore len", len(pairs), seq_len - prev_len)
  util.EqualsOrFailTest(t, "Bad dst objcount", mocks.Destination.ObjCounts(), expect_dst_cnt)
  util.EqualsOrFailTest(t, "Bad restore reqs", mocks.Store.ObjCounts().Restored,
                        (seq_len - prev_len) * ChunksPerSnap)

  expect_rec_uuids := make(map[string]int)
  for i,u := range mocks.Meta.Seqs[cur_uuid].SnapUuids {
    expect_rec_uuids[u] = i
  }
  for i,pair := range pairs {
    expect_src := mocks.Meta.Snaps[pair.Src.Uuid]
    expect_dst := mocks.Destination.Received[prev_len+i]
    if pair.Src == nil || len(pair.Src.Data.Chunks) == 0 {
      t.Fatalf("Bad snap in pair, expect chunk data:\n%s", util.AsJson(pair.Src))
    }
    util.EqualsOrFailTest(t, "bad metadata snap", pair.Src, expect_src)
    util.EqualsOrFailTest(t, "bad destination snap", pair.Dst, expect_dst)

    expect_i, found := expect_rec_uuids[pair.Dst.ReceivedUuid]
    util.EqualsOrFailTest(t, "restored snap in wrong seq", found, true)
    util.EqualsOrFailTest(t, "restored snap in wrong order", prev_len + i, expect_i)

    for _,chunk := range expect_src.Data.Chunks {
      if !mocks.Store.Restored[chunk.Uuid] { t.Errorf("chunk %s not restored", chunk.Uuid) }
    }
  }
}

func TestRestoreCurrentSequence_Empty(t *testing.T) {
  const seq_len = 3
  const prev_len = 0
  HelperTestRestoreCurrentSequence_Ok(t, seq_len, prev_len)
}

func TestRestoreCurrentSequence_PreviousRestore(t *testing.T) {
  const seq_len = 4
  const prev_len = 2
  HelperTestRestoreCurrentSequence_Ok(t, seq_len, prev_len)
}

func TestRestoreCurrentSequence_Noop(t *testing.T) {
  const seq_len = 2
  const prev_len = 2
  HelperTestRestoreCurrentSequence_Ok(t, seq_len, prev_len)
}

func TestRestoreCurrentSequence_CtxTimeout(t *testing.T) {
  const seq_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/seq_len)
  vol_uuid := mocks.Meta.HeadKeys()[0]
  mocks.Store.DefRestoreStx = types.Pending

  done := make(chan error)
  go func() {
    defer close(done)
    _, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
    done <- err
  }()
  select {
    case <-ctx.Done(): break
  }
  select {
    case err := <-done:
      if err == nil { t.Errorf("Expected err after timeout") }
    case <-time.After(util.SmallTimeout):
      t.Errorf("Ctx Timeout but no return")
  }
}

func TestRestoreCurrentSequence_WaitForStorageRestore(t *testing.T) {
  const seq_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.LargeTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/seq_len)
  vol_uuid := mocks.Meta.HeadKeys()[0]
  mocks.Store.DefRestoreStx = types.Pending

  done := make(chan []types.RestorePair)
  go func() {
    defer close(done)
    pairs, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
    if err != nil { t.Fatalf("RestoreCurrentSequence: %v", err) }
    done <- pairs
  }()
  select {
    case <-time.After(util.SmallTimeout): mocks.Store.DefRestoreStx = types.Restored
    case <-ctx.Done(): t.Errorf("ctx timeout")
  }
  select {
    case pairs := <-done:
      util.EqualsOrFailTest(t, "Bad restore len", len(pairs), seq_len)
    case <-ctx.Done(): t.Errorf("ctx timeout")
  }
}

func TestRestoreCurrentSequence_PartialBecauseError(t *testing.T) {
  const seq_len = 3
  const ok_until = 2
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  call_count := 0
  err_inject := func(m interface{}) error {
    if mocks.MethodMatch(m, (types.BackupContent).ReadChunksIntoStream) {
      call_count += 1
      if call_count > ok_until { return fmt.Errorf("err_inject") }
    }
    return nil
  }

  mgr, mock := buildRestoreManager(/*head_cnt=*/seq_len)
  mock.Store.SetErrInject(err_inject)
  vol_uuid := mock.Meta.HeadKeys()[2]
  expect_dst_cnt := mock.Destination.ObjCounts().IncReceived(ok_until)

  pairs, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
  if err == nil { t.Fatalf("Expected error RestoreCurrentSequence") }

  util.EqualsOrFailTest(t, "Bad restore len", len(pairs), ok_until)
  util.EqualsOrFailTest(t, "Bad dst objcount", mock.Destination.ObjCounts(), expect_dst_cnt)
}

func TestRestoreCurrentSequence_StorageRestoreFailed(t *testing.T) {
  const seq_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.LargeTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/seq_len)
  vol_uuid := mocks.Meta.HeadKeys()[0]
  mocks.Store.DefRestoreStx = types.Unknown

  pairs, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
  if err == nil { t.Fatalf("Expected error RestoreCurrentSequence") }
  util.EqualsOrFailTest(t, "Bad restore len", len(pairs), 0)
}

