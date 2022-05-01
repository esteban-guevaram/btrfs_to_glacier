package restore_manager

import (
  "context"
  "os"
  fpmod "path/filepath"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type Mocks struct {
  DstConf *pb.Restore
  Meta    *mocks.Metadata
  Store   *mocks.Storage
  Destination *mocks.VolumeManager
}

func (self *Mocks) AddReceivedSnapInDst(src_snap *pb.SubVolume) *pb.SubVolume {
  sv := util.DummySnapshot(uuid.NewString(), "")
  sv.MountedPath = fpmod.Join(self.DstConf.RootRestorePath, sv.Uuid)
  sv.ReceivedUuid = src_snap.Uuid
  sv.Data = nil
  // Add received snapshot to existing sequence if possible
  vol_uuid := src_snap.ParentUuid
  self.Destination.Snaps[vol_uuid] = append(self.Destination.Snaps[vol_uuid], sv)
  clone := proto.Clone(sv).(*pb.SubVolume)
  return clone
}

func (self *Mocks) AddFirstSnapsFromMetaInDst(seq_uuid string, count int) []*pb.SubVolume {
  added := make([]*pb.SubVolume, 0)
  for i,snap_uuid := range self.Meta.Seqs[seq_uuid].SnapUuids {
    if i >= count { break }
    snap := self.Meta.Snaps[snap_uuid]
    added = append(added, self.AddReceivedSnapInDst(snap))
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
  meta, store := mocks.DummyMetaAndStorage(head_cnt, head_cnt, head_cnt, head_cnt)
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

func TestRestoreCurrentSequence_Empty(t *testing.T) {
  const seq_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildRestoreManager(/*head_cnt=*/seq_len)
  expect_cnt := mocks.Meta.ObjCounts()
  vol_uuid := mocks.Meta.HeadKeys()[1]
  snaps, err := mgr.RestoreCurrentSequence(ctx, vol_uuid)
  if err != nil { util.Fatalf("RestoreCurrentSequence: %v", err) }

  util.EqualsOrFailTest(t, "Should not create new objects", mocks.Meta.ObjCounts(), expect_cnt)
  util.EqualsOrFailTest(t, "Bad restore len", len(snaps), seq_len)
  util.EqualsOrFailTest(t, "Bad dst objcount", mocks.Destination.ObjCounts(),
                                               []int{/*vols=*/0, /*seqs=*/1, /*snaps=*/seq_len,})

  //cur_uuid := mocks.Meta.Heads[vol_uuid].CurSeqUuid
  //expect_rec_uuids := make(map[string]int)
  //for i,u := range mocks.Meta.Seqs[cur_uuid].SnapUuids {
  //  expect_rec_uuids[u] = i
  //}
  //for i,snap := range snaps {
  //  expect_snap := mocks.Meta.Snaps[snap.ReceivedUuid]
  //  util.EqualsOrFailTest(t, "bad restored snap (source)", snap, expect_snap)
  //  expect_i, found := expect_rec_uuids[snap.ReceivedUuid]
  //  util.EqualsOrFailTest(t, "restored snap in wrong seq", found, true)
  //  util.EqualsOrFailTest(t, "restored snap in wrong order", i, expect_i)
  //}
}

func TestRestoreCurrentSequence_PreviousRestore(t *testing.T) {
}

func TestRestoreCurrentSequence_Noop(t *testing.T) {
}

func TestRestoreCurrentSequence_CtxTimeout(t *testing.T) {
}

func TestRestoreCurrentSequence_WaitForStorageRestore(t *testing.T) {
}

func TestRestoreCurrentSequence_PartialBecauseError(t *testing.T) {
}

func TestRestoreCurrentSequence_StorageRestoreFailed(t *testing.T) {
}

