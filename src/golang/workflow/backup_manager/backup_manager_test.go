package backup_manager

import (
  "context"
  "errors"
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type Mocks struct {
  ConfSrc *pb.Source
  Meta *mocks.Metadata
  Store *mocks.Storage
  Source *mocks.VolumeManager
}

type MockCountState struct {
  Meta []int
  Store []int
  Source []int
}

func (self *Mocks) AddSubVolume(ppair *pb.Source_VolSnapPathPair, uuid_str string) *pb.SubVolume {
  sv := util.DummySubVolume(uuid_str)
  sv.MountedPath = ppair.VolPath
  sv.Data = nil
  self.Source.Vols[sv.MountedPath] = sv
  clone := proto.Clone(sv).(*pb.SubVolume)
  return clone
}

func (self *Mocks) AddSnapshot(sv *pb.SubVolume, uuid_str string) *pb.SubVolume {
  var ppair *pb.Source_VolSnapPathPair
  for _,pp := range self.ConfSrc.Paths { if pp.VolPath == sv.MountedPath { ppair = pp } }
  snap := util.DummySnapshot(uuid_str, sv.Uuid)
  snap.MountedPath = ppair.SnapPath
  snap.Data = nil
  self.Source.Snaps[sv.Uuid] = append(self.Source.Snaps[sv.Uuid], snap)
  clone := proto.Clone(snap).(*pb.SubVolume)
  return clone
}

func (self *Mocks) CountState() *MockCountState {
  return &MockCountState{
    Meta: self.Meta.ObjCounts(),
    Store: self.Store.ObjCounts(),
    Source: self.Source.ObjCounts(),
  }
}

func (self *MockCountState) Clone() *MockCountState {
  return &MockCountState{
    Meta: append([]int{}, self.Meta...),
    Store: append([]int{}, self.Store...),
    Source: append([]int{}, self.Source...),
  }
}

func (self *MockCountState) IncrementMeta(new_heads int, new_seqs int, new_snaps int) *MockCountState {
  clone := self.Clone()
  clone.Meta[0] += new_heads
  clone.Meta[1] += new_seqs
  clone.Meta[2] += new_snaps
  clone.Meta[3] += new_heads
  clone.Store[0] += new_snaps
  clone.Source[2] += new_snaps
  return clone
}

func (self *MockCountState) IncrementVers(add int) *MockCountState {
  clone := self.Clone()
  clone.Meta[3] += add
  return clone
}

func (self *MockCountState) IncrementAll(add int) *MockCountState {
  clone := self.Clone()
  clone.Meta[0] += add
  clone.Meta[1] += add
  clone.Meta[2] += add
  clone.Meta[3] += add
  clone.Store[0] += add
  clone.Source[1] += add
  clone.Source[2] += add
  return clone
}

func (self *Mocks) AddSvAndSnapsFromMetaInSrc() []*pb.SubVolume {
  heads := self.Meta.HeadKeys()
  expect_svs := []*pb.SubVolume{}
  for i,ppair := range self.ConfSrc.Paths {
    sv := self.AddSubVolume(ppair, heads[i])
    expect_svs = append(expect_svs, sv)

    head := self.Meta.Heads[heads[i]]
    if len(head.CurSeqUuid) == 0 { continue }
    for _,uuid_str := range self.Meta.Seqs[head.CurSeqUuid].SnapUuids {
      self.AddSnapshot(sv, uuid_str)
    }
  }
  return expect_svs
}

func buildConfSources(src_count int, path_count int) []*pb.Source {
  srcs := []*pb.Source{}
  for i:=0; i<src_count; i+=1 {
    source := &pb.Source{
      Type: pb.Source_BTRFS,
      Name: uuid.NewString(),
    }
    for j:=0; j<path_count; j+=1 {
      ppair := &pb.Source_VolSnapPathPair{
        VolPath:  fmt.Sprintf("/tmp/subvol_%d_%d", i, j),
        SnapPath: fmt.Sprintf("/tmp/snaps_%d_%d", i, j),
      }
      source.Paths = append(source.Paths, ppair)
    }
    srcs = append(srcs, source)
  }
  return srcs
}

func buildBackupManagerEmpty(path_count int) (*BackupManager, *Mocks) {
  conf := util.LoadTestConf()
  conf.Sources = buildConfSources(1, path_count)
  meta := mocks.NewMetadata()
  store := mocks.NewStorage()
  source := mocks.NewVolumeManager()
  mocks := &Mocks{
    ConfSrc: conf.Sources[0],
    Meta: meta,
    Store: store,
    Source: source,
  }
  mgr, err := NewBackupManager(conf, conf.Sources[0].Name, meta, store, source)
  if err != nil { util.Fatalf("BuildBackupManager: %v", err) }
  return mgr.(*BackupManager), mocks
}

func buildBackupManager(
    meta *mocks.Metadata, store *mocks.Storage, source *mocks.VolumeManager) (*BackupManager, *Mocks) {
  conf := util.LoadTestConf()
  conf.Sources = buildConfSources(1, len(meta.HeadKeys()))
  mocks := &Mocks{
    ConfSrc: conf.Sources[0],
    Meta: meta,
    Store: store,
    Source: source,
  }
  mocks.AddSvAndSnapsFromMetaInSrc()
  mgr, err := NewBackupManager(conf, conf.Sources[0].Name, meta, store, source)
  if err != nil { util.Fatalf("BuildBackupManager: %v", err) }
  return mgr.(*BackupManager), mocks
}

func ValidateBackupInVolumeSource(
    t *testing.T, mocks *Mocks, expect_sv *pb.SubVolume, pair types.BackupPair) {
  util.EqualsOrFailTest(t, "bad subvol", pair.Sv, expect_sv)
  src_snaps := mocks.Source.Snaps[expect_sv.Uuid]
  if len(src_snaps) < 1 { t.Fatalf("No snaps in source for '%s'", expect_sv.Uuid) }
  last_snap_src := src_snaps[len(src_snaps)-1]
  // Volume store should not contain chunk data, exclude from comparison
  res_without_data := proto.Clone(pair.Snap).(*pb.SubVolume)
  res_without_data.Data = nil
  util.EqualsOrFailTest(t, "bad snap [volume source]", res_without_data, last_snap_src)
}

func ValidateBackupInMetadata(
    t *testing.T, mocks *Mocks, expect_sv *pb.SubVolume, pair types.BackupPair) {
  head := mocks.Meta.Heads[expect_sv.Uuid]
  if head == nil {
    t.Fatalf("Bad head state:\n%s", util.AsJson(mocks.Meta))
  }
  cur_seq := mocks.Meta.Seqs[head.CurSeqUuid]
  if cur_seq == nil || len(cur_seq.SnapUuids) < 1 || cur_seq.Volume == nil {
    t.Fatalf("Bad snap seq state:\n%s", util.AsJson(mocks.Meta))
  }
  last_snap_meta,found := mocks.Meta.Snaps[cur_seq.SnapUuids[len(cur_seq.SnapUuids)-1]]
  if !found { t.Fatalf("Bad snap state:\n%s", util.AsJson(mocks.Meta)) }
  util.EqualsOrFailTest(t, "bad snap with meta", pair.Snap, last_snap_meta)
}

func ValidateMetadataAndStorageConsistency(
    t *testing.T, mocks *Mocks, expect_sv *pb.SubVolume) {
  head := mocks.Meta.Heads[expect_sv.Uuid]
  seq_uuids := head.PrevSeqUuid
  seq_uuids = append(seq_uuids, head.CurSeqUuid)
  for _,seq_uuid := range seq_uuids {
    seq,found := mocks.Meta.Seqs[seq_uuid]
    if !found { t.Fatalf("Bad sequence state:\n%s", util.AsJson(mocks.Meta)) }
    for _,snap_uuid := range seq.SnapUuids {
      snap, found := mocks.Meta.Snaps[snap_uuid]
      if !found { t.Fatalf("Bad snap state:\n%s", util.AsJson(mocks.Meta)) }
      if snap.ParentUuid != seq.Volume.Uuid || len(snap.Data.Chunks) < 1 {
        t.Fatalf("Bad snap:\n%s", util.AsJson(snap))
      }
      for _,chunk := range snap.Data.Chunks {
        _, found := mocks.Store.Chunks[chunk.Uuid]
        if !found { t.Fatalf("Bad store state:\n%s", util.AsJson(mocks.Store)) }
      }
    } // snaps in sequence
  }   // sequences in head
}

func ValidateBackupPairs(
    t *testing.T, mocks *Mocks, expect_svs []*pb.SubVolume, pairs []types.BackupPair) {
  vol_uuid_to_pair := make(map[string]types.BackupPair)
  for _,p := range pairs { vol_uuid_to_pair[p.Sv.Uuid] = p }

  for _,expect_sv := range expect_svs {
    pair, found := vol_uuid_to_pair[expect_sv.Uuid]
    if !found {
      t.Fatalf("expect_svs/pairs mismatch:\n%s\n%s", util.AsJson(expect_svs), util.AsJson(pairs))
    }
    ValidateBackupInVolumeSource(t, mocks, expect_sv, pair)
    ValidateBackupInMetadata(t, mocks, expect_sv, pair)
    ValidateMetadataAndStorageConsistency(t, mocks, expect_sv)
  }
}

func ValidateObjectCounts(
    t *testing.T, mocks *Mocks, expect *MockCountState) {
  util.EqualsOrFailTest(t, "bad obj count [volume source]",
                        mocks.Source.ObjCounts(), expect.Source)
  util.EqualsOrFailTest(t, "bad obj count [metadata]",
                        mocks.Meta.ObjCounts(), expect.Meta)
  util.EqualsOrFailTest(t, "bad obj count [storage]",
                        mocks.Store.ObjCounts(), expect.Store)
}

func HelperBackupAllToCurrentSequences_NewSeq_NoSnaps(t *testing.T, vol_count int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildBackupManagerEmpty(vol_count)
  expect_svs := []*pb.SubVolume{}
  for i:=0; i<vol_count; i+=1 {
    expect_svs = append(expect_svs, mocks.AddSubVolume(mocks.ConfSrc.Paths[i], uuid.NewString()))
  }
  init_state := mocks.CountState()
  pairs, err := mgr.BackupAllToCurrentSequences(ctx)
  if err != nil { util.Fatalf("BackupAllToCurrentSequences: %v", err) }

  ValidateBackupPairs(t, mocks, expect_svs, pairs)
  new_state := init_state.IncrementAll(vol_count)
  ValidateObjectCounts(t, mocks, new_state)

  // Idempotency
  _, err = mgr.BackupAllToCurrentSequences(ctx)
  if err != nil { util.Fatalf("BackupAllToCurrentSequences: %v", err) }
  ValidateObjectCounts(t, mocks, new_state.IncrementVers(vol_count))
}

func TestBackupAllToCurrentSequences_NewSeq_NoSnaps_SingleVol(t *testing.T) {
  const vol_count = 1
  HelperBackupAllToCurrentSequences_NewSeq_NoSnaps(t, vol_count)
}

func TestBackupAllToCurrentSequences_NewSeq_NoSnaps_MultiVol(t *testing.T) {
  const vol_count = 3
  HelperBackupAllToCurrentSequences_NewSeq_NoSnaps(t, vol_count)
}

func TestBackupAllToCurrentSequences_SeqInMetaButNoSnapInSrc(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  meta, store := mocks.DummyMetaAndStorage(1,1,1,1)
  mgr, mocks := buildBackupManager(meta, store, mocks.NewVolumeManager())
  mocks.Source.ClearSnaps()
  _, err := mgr.BackupAllToCurrentSequences(ctx)
  if !errors.Is(err, ErrSnapsMismatchWithSrc) { util.Fatalf("BackupAllToCurrentSequences: %v", err) }
}

func HelperBackupAllToCurrentSequences_NewSeq_OldSnaps(t *testing.T, vol_count int) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  meta, store := mocks.DummyMetaAndStorage(vol_count,1,1,1)
  mgr, mocks := buildBackupManager(meta, store, mocks.NewVolumeManager())
  expect_svs := mocks.Source.AllVols()
  mocks.Meta.Clear()
  mocks.Store.Clear()
  init_state := mocks.CountState()
  pairs, err := mgr.BackupAllToCurrentSequences(ctx)
  if err != nil { util.Fatalf("BackupAllToCurrentSequences: %v", err) }

  ValidateBackupPairs(t, mocks, expect_svs, pairs)
  ValidateObjectCounts(t, mocks, init_state.IncrementMeta(vol_count, vol_count, vol_count))
}

func TestBackupAllToCurrentSequences_NewSeq_OldSnaps_SingleVol(t *testing.T) {
  const vol_count = 1
  HelperBackupAllToCurrentSequences_NewSeq_OldSnaps(t, vol_count)
}

func TestBackupAllToCurrentSequences_NewSeq_OldSnaps_MultiVol(t *testing.T) {
  const vol_count = 3
  HelperBackupAllToCurrentSequences_NewSeq_OldSnaps(t, vol_count)
}

func TestBackupAllToCurrentSequences_AllNew(t *testing.T) { }
func TestBackupAllToCurrentSequences_Noop(t *testing.T) { }

