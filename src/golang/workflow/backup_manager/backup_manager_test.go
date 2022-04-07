package backup_manager

import (
  "context"
  "testing"

  pb "btrfs_to_glacier/messages"
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

func (self *Mocks) AddSubVolume() *pb.SubVolume {
  idx := len(self.Source.Vols)
  sv := util.DummySubVolume(uuid.NewString())
  sv.MountedPath = self.ConfSrc.Paths[idx].VolPath
  sv.Data = nil
  self.Source.Vols[sv.MountedPath] = sv
  clone := proto.Clone(sv).(*pb.SubVolume)
  return clone
}

func buildBackupManagerEmpty() (*BackupManager, *Mocks) {
  conf := util.LoadTestConf()
  meta := mocks.NewMetadata()
  store := mocks.NewStorage()
  source := mocks.NewVolumeManager()
  mgr, err := NewBackupManager(conf, conf.Sources[0].Name, meta, store, source)
  if err != nil { util.Fatalf("BuildBackupManager: %v", err) }
  mocks := &Mocks{
    ConfSrc: conf.Sources[0],
    Meta: meta,
    Store: store,
    Source: source,
  }
  return mgr.(*BackupManager), mocks
}

func TestBackupAllToCurrentSequences_NewSeq_NoSnaps_SingleVol(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()

  mgr, mocks := buildBackupManagerEmpty()
  expect_sv := mocks.AddSubVolume() 
  pairs, err := mgr.BackupAllToCurrentSequences(ctx)
  if err != nil { util.Fatalf("BackupAllToCurrentSequences: %v", err) }
  if len(pairs) != 1 { util.Fatalf("bad pairs:\n%s", util.AsJson(pairs)) }
  util.EqualsOrFailTest(t, "bad subvol", pairs[0].Sv, expect_sv)

  src_snaps := mocks.Source.Snaps[expect_sv.Uuid]
  if len(src_snaps) != 1 {
    t.Fatalf("Bad snaps in source:\n%s", util.AsJson(src_snaps))
  }
  // Volume store should not contain chunk data
  res_without_data := proto.Clone(pairs[0].Snap).(*pb.SubVolume)
  res_without_data.Data = nil
  util.EqualsOrFailTest(t, "bad snap with volume source", res_without_data, src_snaps[0])

  head := mocks.Meta.Heads[expect_sv.Uuid]
  if head == nil {
    t.Fatalf("Bad head state:\n%s", util.AsJson(mocks.Meta))
  }
  cur_seq := mocks.Meta.Seqs[head.CurSeqUuid]
  if cur_seq == nil || len(cur_seq.SnapUuids) != 1 || cur_seq.Volume == nil {
    t.Fatalf("Bad snap seq state:\n%s", util.AsJson(mocks.Meta))
  }
  for _,snap_uuid := range cur_seq.SnapUuids {
    snap, found := mocks.Meta.Snaps[snap_uuid]
    if !found { t.Fatalf("Bad snap state:\n%s", util.AsJson(mocks.Meta)) }
    if snap.ParentUuid != cur_seq.Volume.Uuid || len(snap.Data.Chunks) < 1 {
      t.Fatalf("Bad snap:\n%s", util.AsJson(snap))
    }
    if snap.Uuid == pairs[0].Snap.Uuid {
      util.EqualsOrFailTest(t, "bad snap with meta", pairs[0].Snap, snap)
    }
    for _,chunk := range snap.Data.Chunks {
      _, found := mocks.Store.Chunks[chunk.Uuid]
      if !found { t.Fatalf("Bad store state:\n%s", util.AsJson(mocks.Store)) }
    }
  }
}

func TestBackupAllToCurrentSequences_AllNew(t *testing.T) { }
func TestBackupAllToCurrentSequences_Noop(t *testing.T) { }

