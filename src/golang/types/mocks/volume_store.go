package mocks

import (
  "context"
  "fmt"
  "io"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"

  uuid_mod "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

// In mem metadata storage
// Simple implementation does not do any input validation.
type Metadata struct {
  Heads map[string]*pb.SnapshotSeqHead
  Seqs map[string]*pb.SnapshotSequence
  Snaps map[string]*pb.SubVolume
}

// In mem metadata storage
// Simple implementation does not do any input validation.
type Storage struct {
  ChunkLen  uint64
  Chunks    map[string][]byte
  Restored  map[string]bool
}

func NewMetadata() *Metadata {
  return &Metadata{
    Heads: make(map[string]*pb.SnapshotSeqHead),
    Seqs: make(map[string]*pb.SnapshotSequence),
    Snaps: make(map[string]*pb.SubVolume),
  }
}

func NewStorage() *Storage {
  return &Storage{
    ChunkLen: 256,
    Chunks: make(map[string][]byte),
    Restored: make(map[string]bool),
  }
}

func (self *Metadata) RecordSnapshotSeqHead(
    ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error) {
  head_uuid := new_seq.Volume.Uuid
  head,found := self.Heads[head_uuid]
  if !found {
    head = &pb.SnapshotSeqHead{ Uuid:head_uuid, }
    self.Heads[head_uuid] = head
  }
  if head.CurSeqUuid == new_seq.Uuid {
    return proto.Clone(head).(*pb.SnapshotSeqHead), nil
 }
  if len(head.CurSeqUuid) > 0 {
    head.PrevSeqUuid = append(head.PrevSeqUuid, head.CurSeqUuid)
  }
  head.CurSeqUuid = new_seq.Uuid
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *Metadata) AppendSnapshotToSeq(
    ctx context.Context, seq *pb.SnapshotSequence, snap *pb.SubVolume) (*pb.SnapshotSequence, error) {
  new_seq,found :=  self.Seqs[seq.Uuid]
  if !found {
    new_seq = proto.Clone(seq).(*pb.SnapshotSequence)
    self.Seqs[seq.Uuid] = new_seq
  }
  if len(seq.SnapUuids) > 0 && seq.SnapUuids[len(seq.SnapUuids)-1] == snap.Uuid {
    return proto.Clone(seq).(*pb.SnapshotSequence), nil
  }
  new_seq.SnapUuids = append(new_seq.SnapUuids, snap.Uuid)
  return proto.Clone(new_seq).(*pb.SnapshotSequence), nil
}

func (self *Metadata) AppendChunkToSnapshot(
    ctx context.Context, snap *pb.SubVolume, data *pb.SnapshotChunks) (*pb.SubVolume, error) {
  new_snap,found :=  self.Snaps[snap.Uuid]
  if !found {
    new_snap = proto.Clone(snap).(*pb.SubVolume)
    self.Snaps[snap.Uuid] = new_snap
  }
  if store.IsFullyContainedInSubvolume(snap, data) {
    return proto.Clone(snap).(*pb.SubVolume), nil
  }
  new_snap.Data.Chunks = append(new_snap.Data.Chunks, data.Chunks...)
  return proto.Clone(new_snap).(*pb.SubVolume), nil
}

func (self *Metadata) ReadSnapshotSeqHead(
    ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error) {
  head,found := self.Heads[uuid]
  if !found { return nil, types.ErrNotFound }
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *Metadata) ReadSnapshotSeq(
    ctx context.Context, uuid string) (*pb.SnapshotSequence, error) {
  seq,found := self.Seqs[uuid]
  if !found { return nil, types.ErrNotFound }
  return proto.Clone(seq).(*pb.SnapshotSequence), nil
}

func (self *Metadata) ReadSnapshot(
    ctx context.Context, uuid string) (*pb.SubVolume, error) {
  snap,found := self.Snaps[uuid]
  if !found { return nil, types.ErrNotFound }
  return proto.Clone(snap).(*pb.SubVolume), nil
}

type SnapshotSeqHeadIterator struct {
  NextF func(context.Context, *pb.SnapshotSeqHead) bool
}
func (self *SnapshotSeqHeadIterator) Err() error { return nil }
func (self *SnapshotSeqHeadIterator) Next(
    ctx context.Context, msg *pb.SnapshotSeqHead) bool {
  return self.NextF(ctx, msg)
}
func (self *Metadata) ListAllSnapshotSeqHeads(
    ctx context.Context) (types.SnapshotSeqHeadIterator, error) {
  idx := 0
  keys := make([]string, 0, len(self.Heads))
  for k,_ := range self.Heads { keys = append(keys, k) }

  next_f := func(ctx context.Context, msg *pb.SnapshotSeqHead) bool {
    if ctx.Err() != nil || idx >= len(keys) { return false }
    msg.Reset()
    proto.Merge(msg, self.Heads[keys[idx]])
    idx += 1
    return true
  }
  return &SnapshotSeqHeadIterator{ next_f }, nil
}

type SnapshotSequenceIterator struct {
  NextF func(context.Context, *pb.SnapshotSequence) bool
}
func (self *SnapshotSequenceIterator) Err() error { return nil }
func (self *SnapshotSequenceIterator) Next(
    ctx context.Context, msg *pb.SnapshotSequence) bool {
  return self.NextF(ctx, msg)
}
func (self *Metadata) ListAllSnapshotSeqs(
    ctx context.Context) (types.SnapshotSequenceIterator, error) {
  idx := 0
  keys := make([]string, 0, len(self.Seqs))
  for k,_ := range self.Seqs { keys = append(keys, k) }

  next_f := func(ctx context.Context, msg *pb.SnapshotSequence) bool {
    if ctx.Err() != nil || idx >= len(keys) { return false }
    msg.Reset()
    proto.Merge(msg, self.Seqs[keys[idx]])
    idx += 1
    return true
  }
  return &SnapshotSequenceIterator{ next_f }, nil
}

type SnapshotIterator struct {
  NextF func(context.Context, *pb.SubVolume) bool
}
func (self *SnapshotIterator) Err() error { return nil }
func (self *SnapshotIterator) Next(
    ctx context.Context, msg *pb.SubVolume) bool {
  return self.NextF(ctx, msg)
}
func (self *Metadata) ListAllSnapshots(
    ctx context.Context) (types.SnapshotIterator, error) {
  idx := 0
  keys := make([]string, 0, len(self.Snaps))
  for k,_ := range self.Snaps { keys = append(keys, k) }

  next_f := func(ctx context.Context, msg *pb.SubVolume) bool {
    if ctx.Err() != nil || idx >= len(keys) { return false }
    msg.Reset()
    proto.Merge(msg, self.Snaps[keys[idx]])
    idx += 1
    return true
  }
  return &SnapshotIterator{ next_f }, nil
}

func (self *Metadata) SetupMetadata(ctx context.Context) (<-chan error) {
  return util.WrapInChan(ctx.Err())
}

func (self *Metadata) DeleteSnapshotSeqHead(ctx context.Context, uuid string) error {
  _,found := self.Heads[uuid]
  if !found { return types.ErrNotFound }
  delete(self.Heads, uuid)
  return nil
}

func (self *Metadata) DeleteSnapshotSeq(ctx context.Context, uuid string) error {
  _,found := self.Seqs[uuid]
  if !found { return types.ErrNotFound }
  delete(self.Seqs, uuid)
  return nil
}

func (self *Metadata) DeleteSnapshot(ctx context.Context, uuid string) error {
  _,found := self.Snaps[uuid]
  if !found { return types.ErrNotFound }
  delete(self.Snaps, uuid)
  return nil
}

func (self *Metadata) DeleteMetadataUuids(
    ctx context.Context, seq_uuids []string, snap_uuids []string) (<-chan error) {
  for _,uuid := range seq_uuids { delete(self.Seqs, uuid) }
  for _,uuid := range snap_uuids { delete(self.Snaps, uuid) }
  return util.WrapInChan(ctx.Err())
}

func (self *Metadata) ReplaceSnapshotSeqHead(
    ctx context.Context, head *pb.SnapshotSeqHead) (*pb.SnapshotSeqHead, error) {
  old_head,found := self.Heads[head.Uuid]
  if !found { return nil, types.ErrNotFound }
  self.Heads[head.Uuid] = proto.Clone(head).(*pb.SnapshotSeqHead)
  return old_head, nil
}

///////////////////////// Storage //////////////////////////

func (self *Storage) WriteStream(
    ctx context.Context, offset uint64, read_pipe io.ReadCloser) (<-chan types.ChunksOrError, error) {
  result := types.ChunksOrError{
    Val: &pb.SnapshotChunks{ KeyFingerprint: uuid_mod.NewString(), },
  }
  done := make(chan types.ChunksOrError, 1)
  start := offset
  defer read_pipe.Close()
  defer close(done)

  if offset > 0 {
    limit_read := &io.LimitedReader{ R:read_pipe, N:int64(offset), }
    _,err := io.Copy(io.Discard, limit_read)
    if err != nil { return nil, err }
  }
  for {
    if ctx.Err() != nil { result.Err = ctx.Err(); break }
    uuid := uuid_mod.NewString()
    limit_read := &io.LimitedReader{ R:read_pipe, N:int64(self.ChunkLen), }
    chunk,err := io.ReadAll(limit_read)
    if err != nil { result.Err = err; break }
    if len(chunk) < 1 { break }

    self.Chunks[uuid] = chunk
    chunk_pb := &pb.SnapshotChunks_Chunk{
      Uuid: uuid,
      Start: start,
      Size: uint64(len(chunk)),
    }
    result.Val.Chunks = append(result.Val.Chunks, chunk_pb)
    start += uint64(len(chunk))
  }
  done <- result
  return done, nil
}

func (self *Storage) QueueRestoreObjects(
    ctx context.Context, uuids []string) (<-chan types.RestoreResult, error) {
  done := make(chan types.RestoreResult, 1)
  result := make(types.RestoreResult)
  defer close(done)

  for _,uuid := range uuids {
    if ctx.Err() != nil { return nil, ctx.Err() }
    _,found := self.Chunks[uuid]
    if !found { continue }
    self.Restored[uuid] = true
    result[uuid] = types.ObjRestoreOrErr { Stx: types.Restored, }
  }
  done <- result
  return done, nil
}

func (self *Storage) ReadChunksIntoStream(
    ctx context.Context, data *pb.SnapshotChunks) (io.ReadCloser, error) {
  pipe := util.NewInMemPipe(ctx)
  go func() {
    var err error
    defer func() { util.ClosePipeWithError(pipe, err) }()

    for _,chunk := range data.Chunks {
      if ctx.Err() != nil { err = ctx.Err(); return }
      blob,found := self.Chunks[chunk.Uuid]
      restored := self.Restored[chunk.Uuid]
      if !restored || !found { err = fmt.Errorf("Unavailable: %s", chunk.Uuid); return }
      _,err = pipe.WriteEnd().Write(blob)
      if err != nil { return }
    }
  }()
  return pipe.ReadEnd(), nil
}

type SnapshotChunksIterator struct {
  NextF func(context.Context, *pb.SnapshotChunks_Chunk) bool
}
func (self *SnapshotChunksIterator) Err() error { return nil }
func (self *SnapshotChunksIterator) Next(
    ctx context.Context, msg *pb.SnapshotChunks_Chunk) bool {
  return self.NextF(ctx, msg)
}
func (self *Storage) ListAllChunks(
    ctx context.Context) (types.SnapshotChunksIterator, error) {
  idx := 0
  keys := make([]string, 0, len(self.Chunks))
  for k,_ := range self.Chunks { keys = append(keys, k) }

  next_f := func(ctx context.Context, msg *pb.SnapshotChunks_Chunk) bool {
    if ctx.Err() != nil || idx >= len(keys) { return false }
    msg.Reset()
    msg.Uuid = keys[idx]
    msg.Size = uint64(len(self.Chunks[keys[idx]]))
    idx += 1
    return true
  }
  return &SnapshotChunksIterator{ next_f }, nil
}

func (self *Storage) SetupStorage(ctx context.Context) (<-chan error) {
  return util.WrapInChan(ctx.Err())
}

func (self *Storage) DeleteChunks(
    ctx context.Context, chunks []*pb.SnapshotChunks_Chunk) (<-chan error) {
  for _,chunk := range chunks {
    delete(self.Chunks, chunk.Uuid)
    delete(self.Restored, chunk.Uuid)
  }
  return util.WrapInChan(ctx.Err())
}

///////////////////////// Fill out mock ////////////////////////

func (self *Metadata) ObjCounts() []int {
  return []int{ len(self.Heads), len(self.Seqs), len(self.Snaps), }
}

func (self *Metadata) HeadKeys() []string {
  l := make([]string, 0, len(self.Heads))
  for uuid,_ := range self.Heads { l = append(l, uuid) }
  return l
}

func (self *Metadata) SeqKeys() []string {
  l := make([]string, 0, len(self.Seqs))
  for uuid,_ := range self.Seqs { l = append(l, uuid) }
  return l
}

func (self *Metadata) SnapKeys() []string {
  l := make([]string, 0, len(self.Snaps))
  for uuid,_ := range self.Snaps { l = append(l, uuid) }
  return l
}

func (self *Metadata) CloneHeads() map[string]*pb.SnapshotSeqHead {
  m := make(map[string]*pb.SnapshotSeqHead)
  for k,v := range self.Heads { m[k] = proto.Clone(v).(*pb.SnapshotSeqHead) }
  return m
}

// Builds metadata and storage with `head_cnt` heads each containing `seq_cnt` sequences
// and so forth including storage chunks.
func DummyMetaAndStorage(
    head_cnt int, seq_cnt int, snap_cnt int, chunk_cnt int) (*Metadata, *Storage) {
  storage := NewStorage()
  metadata := NewMetadata()
  total_seqs := head_cnt*seq_cnt
  total_snaps := total_seqs*snap_cnt
  total_chunks := total_snaps*chunk_cnt
  chunks_per_par_vol := total_chunks / head_cnt
  chunk_uuids := make([]string, 0, total_chunks)
  snap_uuids := make([]string, 0, total_snaps)
  seq_uuids := make([]string, 0, total_seqs)

  for {
    if len(chunk_uuids) >= total_chunks { break }
    uuid := uuid_mod.NewString()
    storage.Chunks[uuid] = util.GenerateRandomTextData(int(storage.ChunkLen))
    chunk_uuids = append(chunk_uuids, uuid)
  }

  cur_start := 0
  var cur_snap *pb.SubVolume
  var cur_head_uuid string
  for idx,chunk_uuid := range chunk_uuids {
    if idx % chunks_per_par_vol == 0 { cur_head_uuid = uuid_mod.NewString() }
    if idx % chunk_cnt == 0 {
      cur_start = 0
      snap_uuid := uuid_mod.NewString()
      cur_snap = &pb.SubVolume{
         Uuid: snap_uuid,
         MountedPath: fmt.Sprintf("/snap/%s", snap_uuid),
         CreatedTs: uint64(idx),
         ParentUuid: cur_head_uuid,
         Data: &pb.SnapshotChunks {
           KeyFingerprint: snap_uuid,
           Chunks: make([]*pb.SnapshotChunks_Chunk, 0, chunk_cnt),
         },
      }
      snap_uuids = append(snap_uuids, snap_uuid)
      metadata.Snaps[snap_uuid] = cur_snap
    }
    chunk := &pb.SnapshotChunks_Chunk {
      Uuid: chunk_uuid,
      Start: uint64(cur_start),
      Size: uint64(len(storage.Chunks[chunk_uuid])),
    }
    cur_snap.Data.Chunks = append(cur_snap.Data.Chunks, chunk)
    cur_start += int(chunk.Size)
  }

  var cur_seq *pb.SnapshotSequence
  for idx,snap_uuid := range snap_uuids {
    if idx % snap_cnt == 0 {
      par_uuid := metadata.Snaps[snap_uuid].ParentUuid
      seq_uuid := uuid_mod.NewString()
      cur_seq = &pb.SnapshotSequence{
        Uuid: seq_uuid,
        Volume: &pb.SubVolume{
           Uuid: par_uuid,
           MountedPath: fmt.Sprintf("/vol/%s", par_uuid),
           CreatedTs: uint64(idx),
        },
        SnapUuids: make([]string, 0, snap_cnt),
      }
      metadata.Seqs[seq_uuid] = cur_seq
      seq_uuids = append(seq_uuids, seq_uuid)
    }
    cur_seq.SnapUuids = append(cur_seq.SnapUuids, snap_uuid)
  }

  var cur_head *pb.SnapshotSeqHead
  for idx,seq_uuid := range seq_uuids {
    if idx % head_cnt == 0 {
      head_uuid := metadata.Seqs[seq_uuid].Volume.Uuid
      cur_head = &pb.SnapshotSeqHead{
        Uuid: head_uuid,
        CurSeqUuid: seq_uuid,
        //PrevSeqUuid: make([]string, 0, seq_cnt-1),
      }
      metadata.Heads[head_uuid] = cur_head
    } else {
      cur_head.PrevSeqUuid = append(cur_head.PrevSeqUuid, seq_uuid)
    }
  }
  return metadata, storage
}

