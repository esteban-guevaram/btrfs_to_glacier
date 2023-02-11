package garbage_collector

import (
  "context"
  "errors"
  "fmt"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "google.golang.org/protobuf/proto"
)

type garbageCollector struct {
  conf *pb.Config
  metadata types.AdminMetadata
  storage types.AdminBackupContent
}

func NewGarbageCollector(
    conf *pb.Config, metadata types.AdminMetadata, storage types.AdminBackupContent) (types.GarbageCollector, error) {
  collector := &garbageCollector{
    conf: conf,
    metadata: metadata,
    storage: storage,
  }
  return collector, nil
}

func (self *garbageCollector) collectAllChunkUuidsFromMetadata(
    ctx context.Context) (map[string]bool, error) {
  snap := &pb.SubVolume{}
  chunk_uuids := make(map[string]bool)
  snaps_it, err := self.metadata.ListAllSnapshots(ctx)
  if err != nil { return chunk_uuids, err }

  for snaps_it.Next(ctx, snap) {
    for _,chunk := range snap.Data.Chunks {
      if _,found := chunk_uuids[chunk.Uuid]; found {
        return nil, fmt.Errorf("Duplicated chunk in metadata: %s", chunk.Uuid)
      }
      chunk_uuids[chunk.Uuid] = true
    }
  }
  return chunk_uuids, snaps_it.Err()
}

func (self *garbageCollector) collectUnreachableChunksFromStorage(
    ctx context.Context, meta_uuids map[string]bool) ([]*pb.SnapshotChunks_Chunk, error) {
  var chunks []*pb.SnapshotChunks_Chunk
  chunk := &pb.SnapshotChunks_Chunk{}
  chunks_it, err := self.storage.ListAllChunks(ctx)
  if err != nil { return chunks, err }

  for chunks_it.Next(ctx, chunk) {
    if _,found := meta_uuids[chunk.Uuid]; found { continue }
    chunks = append(chunks, proto.Clone(chunk).(*pb.SnapshotChunks_Chunk))
  }
  return chunks, chunks_it.Err()
}

func (self *garbageCollector) deleteStorageItems(
    ctx context.Context, dry_run bool, to_del_chunks []*pb.SnapshotChunks_Chunk) error {
  if !dry_run || len(to_del_chunks) < 1 {
    return self.storage.DeleteChunks(ctx, to_del_chunks)
  }
  return nil
}

func (self *garbageCollector) CleanUnreachableChunks(
    ctx context.Context, dry_run bool) ([]*pb.SnapshotChunks_Chunk, error) {
  var err error
  var chunk_uuids map[string]bool
  var to_del_chunks []*pb.SnapshotChunks_Chunk
  chunk_uuids, err = self.collectAllChunkUuidsFromMetadata(ctx)
  if err != nil { return nil, err }
  to_del_chunks, err = self.collectUnreachableChunksFromStorage(ctx, chunk_uuids)
  if err != nil { return nil, err }

  err = self.deleteStorageItems(ctx, dry_run, to_del_chunks)
  if err != nil { return to_del_chunks, err }

  util.Infof("Deleted (dry_run:%v) chunks:\n%s", dry_run, util.AsJson(to_del_chunks))
  return to_del_chunks, nil
}

// Returns a reverse mapping: seq -> head
func (self *garbageCollector) collectAllRootSequences(
    ctx context.Context) (map[string]string, error) {
  head := &pb.SnapshotSeqHead{}
  seq_to_head := make(map[string]string)
  heads_it, err := self.metadata.ListAllSnapshotSeqHeads(ctx)
  if err != nil { return nil, err }

  for heads_it.Next(ctx, head) {
    if _,found := seq_to_head[head.CurSeqUuid]; found {
      return nil, fmt.Errorf("Duplicated sequence in metadata: %s", head.CurSeqUuid)
    }
    seq_to_head[head.CurSeqUuid] = head.Uuid

    for _,uuid := range head.PrevSeqUuid {
      if _,found := seq_to_head[uuid]; found {
        return nil, fmt.Errorf("Duplicated sequence in metadata: %s", uuid)
      }
      seq_to_head[uuid] = head.Uuid
    }
  }
  return seq_to_head, heads_it.Err()
}

// Returns a list of unreachable sequences.
// Returns a reverse mapping: snap -> seq
func (self *garbageCollector) collectUnreachableSequencesAndReachableSnaps(
    ctx context.Context, seq_to_head map[string]string) ([]*pb.SnapshotSequence, map[string]string, error) {
  var seqs []*pb.SnapshotSequence
  snap_to_seq := make(map[string]string)
  seqs_it, err := self.metadata.ListAllSnapshotSeqs(ctx)
  if err != nil { return nil, nil, err }

  for {
    seq := &pb.SnapshotSequence{}
    if !seqs_it.Next(ctx, seq) { break }
    if _,found := seq_to_head[seq.Uuid]; !found { seqs = append(seqs, seq) }

    for _,uuid := range seq.SnapUuids {
      if _,found := snap_to_seq[uuid]; found {
        return nil, nil, fmt.Errorf("Duplicated snapshot in metadata: %s", uuid)
      }
      snap_to_seq[uuid] = seq.Uuid
    }
  }
  return seqs, snap_to_seq, seqs_it.Err()
}

// Returns a list of unreachable snapshots.
func (self *garbageCollector) collectUnreachableSnapshots(
    ctx context.Context, seq_to_head map[string]string, snap_to_seq map[string]string) ([]*pb.SubVolume, error) {
  var snaps []*pb.SubVolume
  snaps_it, err := self.metadata.ListAllSnapshots(ctx)
  if err != nil { return nil, err }

  for {
    snap := &pb.SubVolume{}
    if !snaps_it.Next(ctx, snap) { break }
    if seq_uuid,found := snap_to_seq[snap.Uuid]; found {
      if _,found := seq_to_head[seq_uuid]; found { continue }
    }
    snaps = append(snaps, snap)
  }
  return snaps, snaps_it.Err()
}

// Simply copies the slice references `seqs` and `snaps` in the returned value.
// Modifying the arguments in the calling code will have funky results...
func (self *garbageCollector) deleteMetaItems_ForwardsArgsInReturn(
    ctx context.Context, dry_run bool, seqs []*pb.SnapshotSequence, snaps []*pb.SubVolume) (*types.DeletedItems, error) {
  result := &types.DeletedItems{
    Seqs: seqs,
    Snaps: snaps,
  }
  if !dry_run {
    seq_uuids := make([]string, 0, len(seqs))
    for _,seq := range seqs { seq_uuids = append(seq_uuids, seq.Uuid) }
    snap_uuids := make([]string, 0, len(snaps))
    for _,snap := range snaps { snap_uuids = append(snap_uuids, snap.Uuid) }

    err := self.metadata.DeleteMetadataUuids(ctx, seq_uuids, snap_uuids)
    return result, err
  }
  return result, nil
}

func (self *garbageCollector) CleanUnreachableMetadata(
    ctx context.Context, dry_run bool) (*types.DeletedItems, error) {
  var err error
  var seq_to_head, snap_to_seq map[string]string
  var seqs []*pb.SnapshotSequence
  var snaps []*pb.SubVolume

  seq_to_head, err = self.collectAllRootSequences(ctx)
  if err != nil { return nil, err }
  seqs, snap_to_seq, err = self.collectUnreachableSequencesAndReachableSnaps(ctx, seq_to_head)
  if err != nil { return nil, err }
  snaps, err = self.collectUnreachableSnapshots(ctx, seq_to_head, snap_to_seq)
  if err != nil { return nil, err }

  result, err := self.deleteMetaItems_ForwardsArgsInReturn(ctx, dry_run, seqs, snaps)
  if !dry_run {
    _, persist_err := self.metadata.PersistCurrentMetadataState(ctx)
    if persist_err != nil { return nil, persist_err }
  }
  util.Infof("Deleted (dry_run:%v) metadata:\n%s", dry_run, util.AsJson(result))
  return result, err
}

func (self *garbageCollector) collectChildSnapshotsAndChunks(
    ctx context.Context, seq *pb.SnapshotSequence) ([]*pb.SubVolume, []*pb.SnapshotChunks_Chunk, error) {
  var snaps []*pb.SubVolume
  var chunks []*pb.SnapshotChunks_Chunk

  for _,uuid := range seq.SnapUuids {
    snap, err := self.metadata.ReadSnapshot(ctx, uuid)
    if err != nil { return nil, nil, err }
    snaps = append(snaps, snap)
    for _,chunk := range snap.Data.Chunks { chunks = append(chunks, chunk) }
  }
  return snaps, chunks, nil
}

func (self *garbageCollector) removeSeqFromHead(
    ctx context.Context, dry_run bool, head *pb.SnapshotSeqHead, seq *pb.SnapshotSequence) error {
  if dry_run { return nil }

  new_head := proto.Clone(head).(*pb.SnapshotSeqHead)
  new_head.PrevSeqUuid = nil
  for _,uuid := range head.PrevSeqUuid {
    if uuid == seq.Uuid { continue }
    new_head.PrevSeqUuid = append(new_head.PrevSeqUuid, uuid)
  }
  if len(new_head.PrevSeqUuid) >= len(head.PrevSeqUuid) {
    // May happen if we re-try after a failure.
    util.Warnf("Sequence %s not found in head %s", seq.Uuid, head.Uuid)
    return nil
  }
  _,err := self.metadata.ReplaceSnapshotSeqHead(ctx, new_head)
  return err
}

func (self *garbageCollector) DeleteSnapshotSequence(
    ctx context.Context, dry_run bool, uuid string) (*types.DeletedItems, error) {
  var err error
  var head *pb.SnapshotSeqHead
  var seq *pb.SnapshotSequence
  var snaps []*pb.SubVolume
  var to_del_chunks []*pb.SnapshotChunks_Chunk

  seq, err = self.metadata.ReadSnapshotSeq(ctx, uuid)
  if errors.Is(err, types.ErrNotFound) { return &types.DeletedItems{}, nil }
  if err != nil { return nil, err }

  head, err = self.metadata.ReadSnapshotSeqHead(ctx, seq.Volume.Uuid)
  if err != nil { return nil, err }
  if head.CurSeqUuid == seq.Uuid {
    // If you really need this, you can either:
    // * Create a new sequence as the current one for that head.
    // * Delete the head manually and then call CleanUnreachableMetadata.
    return nil, fmt.Errorf("Cannot delete the current head sequence: %s for subvol: %s", seq.Uuid, head.Uuid)
  }

  snaps, to_del_chunks, err = self.collectChildSnapshotsAndChunks(ctx, seq)
  if err != nil { return nil, err }

  // Delete order: head -> seq,snap -> chunks
  // This way any failure in the middle will leave unreachable items but reachable items remaining consistent.
  err = self.removeSeqFromHead(ctx, dry_run, head, seq)
  if err != nil { return nil, err }

  seqs := []*pb.SnapshotSequence{ seq, }
  result, err := self.deleteMetaItems_ForwardsArgsInReturn(ctx, dry_run, seqs, snaps)
  if err != nil { return result, err }

  err = self.deleteStorageItems(ctx, dry_run, to_del_chunks)
  result.Chunks = to_del_chunks
  if !dry_run {
    _, persist_err := self.metadata.PersistCurrentMetadataState(ctx)
    if persist_err != nil { return nil, persist_err }
  }

  util.Infof("Deleted (dry_run:%v) sequence:\n%s", dry_run, util.AsJson(result))
  return result, err
}

