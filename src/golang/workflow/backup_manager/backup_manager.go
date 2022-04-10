package backup_manager

import (
  "context"
  "errors"
  "fmt"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
)

const (
  MinIntervalBetweenSnaps = 24 * time.Hour
)

var ErrSnapsMismatchWithSrc = errors.New("snapshot_mismatch_between_meta_and_source")
var ErrNoIncrementalBackup = errors.New("no_parent_snapshot_for_inc_backup")

// Meta and Store must already been setup
// Should we handle getting CAP_SYS_ADMIN at this level ?
type BackupManager struct {
  Conf     *pb.Config
  SrcName  string
  Meta     types.Metadata
  Store    types.Storage
  Source   types.VolumeSource
  MinInterval time.Duration
}

func NewBackupManager(
    conf *pb.Config, src_name string, meta types.Metadata, store types.Storage, vol_src types.VolumeSource) (types.BackupManager, error) {
  mgr := &BackupManager{
    Conf: conf,
    SrcName: src_name,
    Meta: meta,
    Store: store,
    Source: vol_src,
    MinInterval: MinIntervalBetweenSnaps,
  }
  _, err := mgr.GetSource()
  return mgr, err
}

func (self *BackupManager) GetSource() (*pb.Source, error) {
  for _, src := range self.Conf.Sources {
    if src.Name == self.SrcName { return src, nil }
  }
  return nil, fmt.Errorf("Source '%s' is not in configuration", self.SrcName)
}

// Does NOT write anything to self.Meta: writes should be ordered chunk -> snap -> seq -> head
func (self *BackupManager) GetSequenceFor(
    ctx context.Context, sv *pb.SubVolume, new_seq bool) (*pb.SnapshotSequence, error) {
  seq := &pb.SnapshotSequence{
    Uuid: uuid.NewString(),
    Volume: sv,
  }
  if new_seq { return seq, nil }

  head, err := self.Meta.ReadSnapshotSeqHead(ctx, sv.Uuid)
  if errors.Is(err, types.ErrNotFound) { return seq, nil }
  if err != nil { return nil, err }

  return self.Meta.ReadSnapshotSeq(ctx, head.CurSeqUuid)
}

func (self *BackupManager) CreateNewSnapshotOrUseRecent(
    sv *pb.SubVolume, use_recent bool) ([]*pb.SubVolume, *pb.SubVolume, error) {
  old_snaps, err := self.Source.GetSnapshotSeqForVolume(sv)
  if err != nil { return nil, nil, err }
  if use_recent && len(old_snaps) > 0 {
    top_snap := old_snaps[len(old_snaps)-1]
    age := time.Now().Sub(time.Unix(int64(top_snap.CreatedTs), 0))
    if age <= self.MinInterval { return old_snaps, top_snap, nil }
  }
  snap, err := self.Source.CreateSnapshot(sv)
  util.Debugf("Backup snap %s for subvolume %s", snap.Uuid, sv.Uuid)
  return old_snaps, snap, err
}

// IMPORTANT: this does not check the SnapshotSequence is up-to-date.
// The sequence should be written independently of this method result.
// Returns a non-nil snapshot if there is no need to write the snapshot data to storage.
func (self *BackupManager) IsBackupAlreadyInStorage(
    ctx context.Context, old_snaps []*pb.SubVolume, snap *pb.SubVolume) (*pb.SubVolume, error) {
  // We just created snapshot, it cannot be in storage
  if len(old_snaps) == 0 || old_snaps[len(old_snaps)-1].Uuid != snap.Uuid { return nil, nil }
  full_snap, err := self.Meta.ReadSnapshot(ctx, snap.Uuid)
  if errors.Is(err, types.ErrNotFound) { return nil, nil }
  if err != nil { return nil, err }
  util.Debugf("Found snapshot already in storage: %s", full_snap.Uuid)
  // We consider that all snapshots are written in a single call,
  // If the list of chunks is not empty then the full data is in storage.
  if full_snap.Data == nil || len(full_snap.Data.Chunks) == 0 { return nil, nil }
  return full_snap, nil
}

func (self *BackupManager) DetermineParentForIncremental(
    old_snaps []*pb.SubVolume, seq *pb.SnapshotSequence) (*pb.SubVolume, error) {
  if len(seq.SnapUuids) == 0 || len(old_snaps) == 0 { return nil, nil }
  for i:=len(seq.SnapUuids)-1; i>=0; i-=1 {
    store_uuid := seq.SnapUuids[i]
    for j:=len(old_snaps)-1; j>=0; j-=1 {
      src_uuid := old_snaps[j].Uuid
      if src_uuid == store_uuid {
        util.Debugf("For incremental backup parent: %s", src_uuid)
        return old_snaps[j], nil
      }
    }
  }
  return nil, nil
}

func (self *BackupManager) BackupSingleSvToSequence(
    ctx context.Context, sv *pb.SubVolume, seq *pb.SnapshotSequence) (*pb.SubVolume, error) {
  old_snaps, snap, err := self.CreateNewSnapshotOrUseRecent(sv, /*use_recent=*/true)
  if err != nil { return nil, err }
  if len(seq.SnapUuids) != 0 && len(old_snaps) == 0 {
    // This may happen if you delete past snapshots too aggresively in the source ?
    return nil, fmt.Errorf("%w: %s", ErrSnapsMismatchWithSrc, sv.Uuid)
  }
  full_snap, err := self.IsBackupAlreadyInStorage(ctx, old_snaps, snap)
  if err != nil { return nil, err }
  if full_snap != nil { return full_snap, nil }
  par_snap, err := self.DetermineParentForIncremental(old_snaps, seq)
  if err != nil { return nil, err }
  stream, err := self.Source.GetSnapshotStream(ctx, par_snap, snap)
  if err != nil { return nil, err }
  chunks, err := self.Store.WriteStream(ctx, /*offset=*/0, stream)
  if err != nil { return nil, err }
  full_snap, err = self.Meta.AppendChunkToSnapshot(ctx, snap, chunks)
  return full_snap, util.Coalesce(stream.GetErr(), err)
}

// Only if this method succeeds we can guarantee the incremental backup data is correctly stored.
func (self *BackupManager) PersistSequence(
    ctx context.Context, snap *pb.SubVolume, seq *pb.SnapshotSequence) (*pb.SnapshotSequence, error) {
  new_seq, err := self.Meta.AppendSnapshotToSeq(ctx, seq, snap)
  if err != nil { return nil, err }
  _, err = self.Meta.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { return nil, err }
  version, err := self.Meta.PersistCurrentMetadataState(ctx)
  util.Infof("BackupManager.PersistSequence: %s -> %s", snap.ParentUuid, version)
  return new_seq, nil
}

func (self *BackupManager) BackupAllHelper(ctx context.Context, new_seq bool) ([]types.BackupPair, error) {
  backups := []types.BackupPair{}
  src, err := self.GetSource()
  if err != nil { return backups, err }

  for _, p_pair := range src.Paths {
    sv, err := self.Source.GetVolume(p_pair.VolPath) 
    if err != nil { return backups, err }
    seq, err := self.GetSequenceFor(ctx, sv, new_seq)
    if err != nil { return backups, err }
    snap, err := self.BackupSingleSvToSequence(ctx, sv, seq)
    if err != nil { return backups, err }
    _, err = self.PersistSequence(ctx, snap, seq)
    if err != nil { return backups, err }
    backups = append(backups, types.BackupPair{ Sv: sv, Snap: snap, })
  }
  return backups, nil
}

func (self *BackupManager) BackupAllToCurrentSequences(ctx context.Context) ([]types.BackupPair, error) {
  return self.BackupAllHelper(ctx, /*new_seq=*/false)
}

func (self *BackupManager) BackupAllToNewSequences(ctx context.Context) ([]types.BackupPair, error) {
  return self.BackupAllHelper(ctx, /*new_seq=*/true)
}

func (self *BackupManager) BackupToCurrentSequenceUnrelatedVol(
    ctx context.Context, src *pb.SubVolume, dst_uuid string) (*pb.SubVolume, error) {
  head, err := self.Meta.ReadSnapshotSeqHead(ctx, dst_uuid)
  if err != nil { return nil, err }
  seq, err := self.Meta.ReadSnapshotSeq(ctx, head.CurSeqUuid)
  if err != nil { return nil, err }

  old_snaps, snap, err := self.CreateNewSnapshotOrUseRecent(src, /*use_recent=*/false)
  if err != nil { return nil, err }
  if len(old_snaps) < 1 {
    return nil, fmt.Errorf("%w: %s -> %s", ErrNoIncrementalBackup, src.Uuid, dst_uuid)
  }
  par_snap := old_snaps[len(old_snaps)-1]
  // Hack to make it look as if it all comes from the same subvolume
  snap.ParentUuid = dst_uuid
  par_snap.ParentUuid = dst_uuid

  full_snap, err := self.IsBackupAlreadyInStorage(ctx, old_snaps, snap)
  if err != nil { return nil, err }
  if full_snap != nil {
    _, err = self.PersistSequence(ctx, full_snap, seq)
    return full_snap, err
  }

  stream, err := self.Source.GetSnapshotStream(ctx, par_snap, snap)
  if err != nil { return nil, err }
  chunks, err := self.Store.WriteStream(ctx, /*offset=*/0, stream)
  if err != nil { return nil, err }
  full_snap, err = self.Meta.AppendChunkToSnapshot(ctx, snap, chunks)
  if err != nil { return nil, err }
  _, err = self.PersistSequence(ctx, full_snap, seq)
  if err != nil { return nil, err }
  return full_snap, util.Coalesce(stream.GetErr(), err)
}

