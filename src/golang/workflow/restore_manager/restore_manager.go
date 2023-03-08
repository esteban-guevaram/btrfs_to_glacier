package restore_manager

import (
  "context"
  "errors"
  "fmt"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "google.golang.org/protobuf/proto"
)

const (
  BetweenRestoreChecks = 1 * time.Hour
)

var ErrNoCurSeqForUuid = errors.New("current_snap_sequence_for_uuid_not_found")
var ErrNothingToRestore = errors.New("nothing_to_restore_sequence_empty")
var ErrEmptySnapshot = errors.New("snapshot_contains_no_chunks")
var ErrUnavailableChunks = errors.New("could_not_restore_missing_chunks")

// Meta and Content must already been setup
type RestoreManager struct {
  Conf        *pb.Config
  DstConf     *pb.Restore
  Meta        types.Metadata
  Content     types.BackupContent
  Destination types.VolumeDestination
  BetweenRestoreChecks time.Duration
}

func NewRestoreManager(
    conf *pb.Config, dst_name string, meta types.Metadata, content types.BackupContent, vol_dst types.VolumeDestination) (types.RestoreManager, error) {
  mgr := &RestoreManager{
    Conf: conf,
    Meta: meta,
    Content: content,
    Destination: vol_dst,
    BetweenRestoreChecks: BetweenRestoreChecks,
  }
  var err error
  mgr.DstConf, err = mgr.GetDestination(dst_name)
  return mgr, err
}

func (self *RestoreManager) GetDestination(dst_name string) (*pb.Restore, error) {
  for _, dst := range self.Conf.Restores {
    if dst.Name == dst_name && dst.Type == pb.Restore_BTRFS {
      return dst, nil
    }
  }
  return nil, fmt.Errorf("Destination '%s' is not in configuration", dst_name)
}

func (self *RestoreManager) ReadHeadAndSequenceMap(
    ctx context.Context) (types.HeadAndSequenceMap, error) {
  it, err := self.Meta.ListAllSnapshotSeqHeads(ctx)
  if err != nil { return nil, err }

  head := pb.SnapshotSeqHead{}
  heads := make(types.HeadAndSequenceMap, 100)
  for it.Next(ctx, &head) {
    seq, err := self.Meta.ReadSnapshotSeq(ctx, head.CurSeqUuid)
    if err != nil { return nil, err }
    heads[head.Uuid] = types.HeadAndSequence{
      Head: proto.Clone(&head).(*pb.SnapshotSeqHead),
      Cur: seq,
    }
  }
  return heads, it.Err()
}

// The order of uuids is oldest snapshot first.
func (self *RestoreManager) UuidsToBeRestored_InOrder(
    ctx context.Context, seq *pb.SnapshotSequence) ([]string, error) {
  if len(seq.SnapUuids) < 0 { return nil, ErrNothingToRestore }
  existing_vols, err := self.Destination.ListVolumes(self.DstConf.RootRestorePath)
  if err != nil { return nil, err }

  received_uuids := make(map[string]bool, 100)
  for _, vol := range existing_vols {
    if len(vol.ReceivedUuid) == 0 { continue }
    received_uuids[vol.ReceivedUuid] = true
  }

  to_restore := make([]string, 0, 100)
  for i:=len(seq.SnapUuids)-1; i>=0; i-=1 {
    uuid := seq.SnapUuids[i]
    if received_uuids[uuid] { break }
    to_restore = append(to_restore, uuid)
  }
  for i, j := 0, len(to_restore)-1; i < j; i, j = i+1, j-1 {
    to_restore[i], to_restore[j] = to_restore[j], to_restore[i]
  }
  return to_restore, nil
}

// Returns the volumes in the order they should be restored.
func (self *RestoreManager) QueueRestoreChunksForUuids(
    ctx context.Context, uuids []string) ([]*pb.SubVolume, types.RestoreResult, error) {
  from_vols := make([]*pb.SubVolume, 0, 100)
  to_queue := make([]string, 0, 1000)

  for _, uuid := range uuids {
    vol, err := self.Meta.ReadSnapshot(ctx, uuid)
    if err != nil { return from_vols, nil, err }
    if len(vol.Data.Chunks) < 1 {
      return from_vols, nil, fmt.Errorf("%w: %s", ErrEmptySnapshot, uuid)
    }
    for _,chunk := range vol.Data.Chunks {
      to_queue = append(to_queue, chunk.Uuid)
    }
    from_vols = append(from_vols, vol)
  }
  restore_res := self.Content.QueueRestoreObjects(ctx, to_queue)
  return from_vols, restore_res, nil
}

func mergeMaps(super types.RestoreResult, sub types.RestoreResult) types.RestoreResult {
  r := make(types.RestoreResult, len(super))
  for k,v := range super {
    v2, found := sub[k]
    if found { r[k] = v2 }
    /*else*/ if !found { r[k] = v }
  }
  return r
}

func (self *RestoreManager) WaitUntilAvailableAndRestoreSingle(
    ctx context.Context, from *pb.SubVolume, restore_res types.RestoreResult) (*pb.SubVolume, error) {
  ticker := time.NewTicker(self.BetweenRestoreChecks)
  defer ticker.Stop()

  for {
    uuid_unavailable := make([]string, 0, len(restore_res))
    for _,chunk := range from.Data.Chunks {
      stx, found := restore_res[chunk.Uuid]
      if !found {
        return nil, fmt.Errorf("%w: missing %s %s", ErrUnavailableChunks, from.Uuid, chunk.Uuid)
      }
      if stx.Stx == types.Unknown || stx.Err != nil {
        return nil, fmt.Errorf("%w: error %s %s %v", ErrUnavailableChunks, from.Uuid, chunk.Uuid, stx.Err)
      }
      if stx.Stx != types.Restored {
        uuid_unavailable = append(uuid_unavailable, chunk.Uuid)
      }
    }
    if len(uuid_unavailable) == 0 { break }
    select {
      case <- ticker.C:
        restore_res = mergeMaps(restore_res,
                                self.Content.QueueRestoreObjects(ctx, uuid_unavailable))
        util.Debugf("Checking chunks available for %s", from.Uuid)
      case <-ctx.Done(): return nil, ctx.Err()
    }
  }

  read_end, err := self.Content.ReadChunksIntoStream(ctx, from.Data)
  if err != nil { return nil, err}
  to, err := self.Destination.ReceiveSendStream(ctx,
                                                self.DstConf.RootRestorePath,
                                                from,
                                                read_end)
  if err != nil { return nil, util.Coalesce(read_end.GetErr(), err) }
  util.Infof("Restored %s -> %s", from.Uuid, to.Uuid)
  return to, read_end.GetErr()
}

func (self *RestoreManager) RestoreSequenceHelper(
    ctx context.Context, seq *pb.SnapshotSequence) ([]types.RestorePair, error) {
  var err error
  uuids, err := self.UuidsToBeRestored_InOrder(ctx, seq)
  if err != nil { return nil, err }

  from_vols, restore_res, err := self.QueueRestoreChunksForUuids(ctx, uuids)
  if err != nil { return nil, err }

  ok_pairs := make([]types.RestorePair, 0, len(from_vols))
  for _, from := range from_vols {
    var to *pb.SubVolume
    to, err = self.WaitUntilAvailableAndRestoreSingle(ctx, from, restore_res)
    if err != nil { break }
    ok_pairs = append(ok_pairs, types.RestorePair{Src:from, Dst:to,})
  }
  util.Infof("Restored %d/%d volumes", len(ok_pairs), len(from_vols))
  return ok_pairs, err
}

func (self *RestoreManager) RestoreCurrentSequence(
    ctx context.Context, vol_uuid string) ([]types.RestorePair, error) {
  heads, err := self.ReadHeadAndSequenceMap(ctx)
  if err != nil { return nil, err }
  head_seq, found := heads[vol_uuid]
  if !found { return nil, fmt.Errorf("%w: %s", ErrNoCurSeqForUuid, vol_uuid) }
  return self.RestoreSequenceHelper(ctx, head_seq.Cur)
}

