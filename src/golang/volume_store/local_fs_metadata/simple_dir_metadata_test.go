package local_fs_metadata

import (
  "context"
  "errors"
  "os"
  fpmod "path/filepath"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

type SimpleDirRw struct {
  Part *pb.LocalFs_Partition
}

func (self *SimpleDirRw) PutState(state *pb.AllMetadata) error {
  store_path := fpmod.Join(MetaDir(self.Part), "dummystate")
  if err := util.MarshalGzProto(store_path, state); err != nil { return nil }
  return os.Symlink(store_path, SymLink(self.Part))
}

func (self *SimpleDirRw) GetState() *pb.AllMetadata {
  state := &pb.AllMetadata{}
  err := util.UnmarshalGzProto(SymLink(self.Part), state)
  if err != nil && util.IsNotExist(err) { return nil }
  if err != nil { util.Fatalf("SimpleDirRw.GetState: %v", err) }
  return state
}

func buildTestMetadataWithConf(t *testing.T, conf *pb.Config) (*SimpleDirMetadata, *SimpleDirRw) {
  part := conf.LocalFs.Partitions[0]
  client := &SimpleDirRw{part}

  meta := &SimpleDirMetadata{
    Conf: conf,
    DirInfo: part,
    SymLink: SymLink(part),
    KeepLast: KeepLast,
    State: &pb.AllMetadata{},
  }
  return meta, client
}

func buildTestMetadataWithState(t *testing.T, state *pb.AllMetadata) (*SimpleDirMetadata, *SimpleDirRw, func()) {
  var err error
  local_fs, clean_f := util.TestSimpleDirLocalFs()
  conf := util.LoadTestConfWithLocalFs(local_fs)
  meta, client := buildTestMetadataWithConf(t, conf)

  err = client.PutState(state)
  if err != nil { t.Fatalf("failed to set init state: %v", err) }
  meta.State = state
  return meta, client, clean_f
}

func compareStates(t *testing.T, msg string, left *pb.AllMetadata, right *pb.AllMetadata) {
  util.EqualsOrFailTest(t, msg, left.Heads, right.Heads)
  util.EqualsOrFailTest(t, msg, left.Sequences, right.Sequences)
  util.EqualsOrFailTest(t, msg, left.Snapshots, right.Snapshots)
}

func TestLoadPreviousStateFromDir_NoPartition(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  local_fs, clean_f := util.TestSimpleDirLocalFs()
  defer clean_f()
  conf := util.LoadTestConfWithLocalFs(local_fs)
  _, err := NewMetadata(ctx, conf, uuid.NewString())
  if err == nil { t.Errorf("Expected error got: %v", err) }
}

func TestLoadPreviousStateFromDir_NoIniState(t *testing.T) {
  local_fs,clean_f := util.TestSimpleDirLocalFs()
  defer clean_f()
  conf := util.LoadTestConfWithLocalFs(local_fs)
  meta, client := buildTestMetadataWithConf(t, conf)
  meta.State = nil

  meta.LoadPreviousStateFromDir(context.TODO())
  util.EqualsOrFailTest(t, "Bad object", client.GetState(), nil)
  compareStates(t, "expected empty state", meta.State, &pb.AllMetadata{})
}

func TestLoadPreviousStateFromDir_PreviousState(t *testing.T) {
  _, expect_state := util.DummyAllMetadata()
  meta,_,clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  meta.State = nil

  meta.LoadPreviousStateFromDir(context.TODO())
  compareStates(t, "expected empty state", meta.State, expect_state)
}

func TestSaveCurrentStateToDir_NoPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  meta,client,clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()

  version, err := meta.SaveCurrentStateToDir(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }

  persisted_state := client.GetState()
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToDir_WithPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, prev_state := util.DummyAllMetadata()
  var expect_state pb.AllMetadata = *prev_state
  meta, client,clean_f := buildTestMetadataWithState(t, prev_state)
  defer clean_f()

  new_seq := util.DummySnapshotSequence(vol_uuid, uuid.NewString())
  head, err := meta.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Fatalf("RecordSnapshotSeqHead error: %v", err) }
  expect_state.Heads[0] = head

  version, err := meta.SaveCurrentStateToDir(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }

  persisted_state := client.GetState()
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToDir_Err(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, prev_state := util.DummyAllMetadata()
  meta,_,clean_f := buildTestMetadataWithState(t, prev_state)
  defer clean_f()
  meta.DirInfo.MetadataDir = uuid.NewString() // this dir should not exist

  _, err := meta.SaveCurrentStateToDir(ctx)
  if err == nil { t.Errorf("Expected error got: %v", err) }
}

func TestRecordSnapshotSeqHead_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()
  new_seq := util.DummySnapshotSequence("vol", "seq")
  expect_head := util.DummySnapshotSeqHead(new_seq)
  head, err := meta.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSeqHead", head, expect_head)
}

func TestRecordSnapshotSeqHead_Add(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  new_seq := util.DummySnapshotSequence(vol_uuid, "seq2")

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  var expect_head pb.SnapshotSeqHead = *expect_state.Heads[0]
  expect_head.CurSeqUuid = new_seq.Uuid
  expect_head.PrevSeqUuid = []string{expect_state.Sequences[0].Uuid}

  head, err := metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSeqHead", head, &expect_head)
}

func TestRecordSnapshotSeqHead_Noop(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  var expect_head pb.SnapshotSeqHead = *expect_state.Heads[0]
  var new_seq pb.SnapshotSequence = *expect_state.Sequences[0]

  head, err := metadata.RecordSnapshotSeqHead(ctx, &new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSeqHead", head, &expect_head)
}

func TestAppendSnapshotToSeq_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  var expect_seq pb.SnapshotSequence = *expect_state.Sequences[0]
  expect_state.Sequences[0].SnapUuids = nil
  snap_to_add := util.DummySnapshot(expect_seq.SnapUuids[0], vol_uuid)

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  new_seq, err := metadata.AppendSnapshotToSeq(ctx, expect_state.Sequences[0], snap_to_add)

  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSequence", new_seq, &expect_seq)
}

func TestAppendSnapshotToSeq_Noop(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  var expect_seq pb.SnapshotSequence = *expect_state.Sequences[0]
  snap_to_add := util.DummySnapshot(expect_seq.SnapUuids[0], vol_uuid)

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  new_seq, err := metadata.AppendSnapshotToSeq(ctx, expect_state.Sequences[0], snap_to_add)

  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSequence", new_seq, &expect_seq)
}

func TestAppendSnapshotToChunk_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  expect_state.Snapshots = append(expect_state.Snapshots,
                                  util.DummySnapshot(uuid.NewString(), vol_uuid))
  chunk := util.DummyChunks("chunk_uuid")
  var expect_snap pb.SubVolume = *expect_state.Snapshots[1]
  expect_snap.Data = proto.Clone(chunk).(*pb.SnapshotChunks)

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  new_snap, err := metadata.AppendChunkToSnapshot(ctx, expect_state.Snapshots[1], chunk)
  if err != nil { t.Errorf("Returned error: %v", err) }

  persisted_snap := metadata.State.Snapshots[1]
  util.EqualsOrFailTest(t, "Persisted Snapshot", persisted_snap, &expect_snap)
  util.EqualsOrFailTest(t, "New Snapshot", new_snap, &expect_snap)
}

func TestAppendSnapshotToChunk_Append(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  chunk := util.DummyChunks("chunk_uuid2")
  expect_state.Snapshots[0].Data = util.DummyChunks("chunk_uuid1")
  chunk.Chunks[0].Start = store.SubVolumeDataLen(expect_state.Snapshots[0])
  var expect_snap pb.SubVolume = *expect_state.Snapshots[0]
  expect_snap.Data.Chunks = append(expect_snap.Data.Chunks,
                                   proto.Clone(chunk.Chunks[0]).(*pb.SnapshotChunks_Chunk))

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  new_snap, err := metadata.AppendChunkToSnapshot(ctx, expect_state.Snapshots[0], chunk)
  if err != nil { t.Errorf("Returned error: %v", err) }

  persisted_snap := metadata.State.Snapshots[0]
  util.EqualsOrFailTest(t, "Persisted Snapshot", persisted_snap, &expect_snap)
  util.EqualsOrFailTest(t, "New Snapshot", new_snap, &expect_snap)
}

func TestAppendSnapshotToChunk_Noop(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  chunk := util.DummyChunks("chunk_uuid")
  expect_state.Snapshots[0].Data = proto.Clone(chunk).(*pb.SnapshotChunks)
  var expect_snap pb.SubVolume = *expect_state.Snapshots[0]

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  new_snap, err := metadata.AppendChunkToSnapshot(ctx, expect_state.Snapshots[0], chunk)
  if err != nil { t.Errorf("Returned error: %v", err) }

  persisted_snap := metadata.State.Snapshots[0]
  util.EqualsOrFailTest(t, "Persisted Snapshot", persisted_snap, &expect_snap)
  util.EqualsOrFailTest(t, "New Snapshot", new_snap, &expect_snap)
}

func TestAppendSnapshotToChunk_Errors(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  chunk := util.DummyChunks("chunk_uuid")
  snap := expect_state.Snapshots[0]
  snap.Data = proto.Clone(chunk).(*pb.SnapshotChunks)
  var err error
  var expect_snap pb.SubVolume = *snap

  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()

  chunk_1 := util.DummyChunks("chunk_uuid1")
  chunk_1.Chunks[0].Start += 1
  _, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_1)
  if err == nil { t.Errorf("Expected error: %v", err) }

  chunk_2 := util.DummyChunks("chunk_uuid2")
  chunk_2.Chunks[0].Size += 1
  _, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_2)
  if err == nil { t.Errorf("Expected error: %v", err) }

  chunk_3 := util.DummyChunks("chunk_uuid3")
  chunk_3.Chunks[0].Start = store.SubVolumeDataLen(snap) + 1
  _, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_3)
  if err == nil { t.Errorf("Expected error: %v", err) }

  chunk_4 := util.DummyChunks("chunk_uuid4")
  chunk_4.KeyFingerprint = snap.Data.KeyFingerprint + "_wrong_keyfp"
  _, err = metadata.AppendChunkToSnapshot(ctx, snap, chunk_4)
  if err == nil { t.Errorf("Expected error: %v", err) }

  persisted_snap := metadata.State.Snapshots[0]
  util.EqualsOrFailTest(t, "Persisted Snapshot", persisted_snap, &expect_snap)
}

func TestReadSnapshotSeqHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  vol_uuid, expect_state := util.DummyAllMetadata()
  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  var expect_head pb.SnapshotSeqHead = *expect_state.Heads[0]

  head, err := metadata.ReadSnapshotSeqHead(ctx, vol_uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSeqHead", head, &expect_head)
}

func TestReadSnapshotSeqHead_NoHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()

  _, err := metadata.ReadSnapshotSeqHead(ctx, "does_not_exist")
  if errors.Is(err, types.ErrNotFound) { return }
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestReadSnapshotSeq(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  _, expect_state := util.DummyAllMetadata()
  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  var expect_seq pb.SnapshotSequence = *expect_state.Sequences[0]

  seq, err := metadata.ReadSnapshotSeq(ctx, expect_seq.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSequence", seq, &expect_seq)
}

func TestReadSnapshotSeq_NoSequence(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()

  _, err := metadata.ReadSnapshotSeq(ctx, "does_not_exist")
  if errors.Is(err, types.ErrNotFound) { return }
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestReadSnapshot(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  _, expect_state := util.DummyAllMetadata()
  expect_state.Snapshots[0].Data = util.DummyChunks("chunk_uuid")
  metadata, _, clean_f := buildTestMetadataWithState(t, expect_state)
  defer clean_f()
  var expect_snap pb.SubVolume = *expect_state.Snapshots[0]

  snap, err := metadata.ReadSnapshot(ctx, expect_snap.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "Snapshot", snap, &expect_snap)
}

func TestReadSnapshotSeq_NoSnap(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()

  _, err := metadata.ReadSnapshot(ctx, "does_not_exist")
  if errors.Is(err, types.ErrNotFound) { return }
  if err != nil { t.Errorf("Returned error: %v", err) }
}

type create_pb_f = func(*pb.AllMetadata) (string, proto.Message)
type iterate_f = func(context.Context, types.Metadata) (map[string]proto.Message, error)
func testMetadataListAll_Helper(t *testing.T, total int, pb_f create_pb_f, iter_f iterate_f) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  state := &pb.AllMetadata{}
  expect_objs := make(map[string]proto.Message)

  for i:=0; i<total; i+=1 {
    key,obj := pb_f(state)
    expect_objs[key] = proto.Clone(obj)
  }

  metadata, _, clean_f := buildTestMetadataWithState(t, state)
  defer clean_f()
  got_objs, err := iter_f(ctx, metadata)

  if err != nil { t.Fatalf("failed while iterating: %v", err) }
  util.EqualsOrFailTest(t, "Bad len", len(got_objs), len(expect_objs))
  for key,expect := range expect_objs {
    util.EqualsOrFailTest(t, "Bad obj", got_objs[key], expect)
  }
}

func head_pb_f(state *pb.AllMetadata) (string, proto.Message) {
  new_seq := util.DummySnapshotSequence(uuid.NewString(), uuid.NewString())
  new_head := util.DummySnapshotSeqHead(new_seq)
  state.Heads = append(state.Heads, new_head)
  return new_head.Uuid, new_head
}
func head_iter_f(ctx context.Context, metadata types.Metadata) (map[string]proto.Message, error) {
  got_objs := make(map[string]proto.Message)
  it, err := metadata.ListAllSnapshotSeqHeads(ctx)
  if err != nil { return nil, err }
  obj := &pb.SnapshotSeqHead{}
  for it.Next(ctx, obj) { got_objs[obj.Uuid] = proto.Clone(obj) }
  return got_objs, it.Err()
}

func TestListAllSnapshotSeqHeads(t *testing.T) {
  const total = 3
  testMetadataListAll_Helper(t, total, head_pb_f, head_iter_f)
}
func TestListAllSnapshotSeqHeads_NoObjects(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()
  got_objs, err := head_iter_f(ctx, metadata)
  if err != nil { t.Errorf("failed while iterating: %v", err) }
  if len(got_objs) > 0 { t.Errorf("should not return any object") }
}

func seq_pb_f(state *pb.AllMetadata) (string, proto.Message) {
  new_seq := util.DummySnapshotSequence(uuid.NewString(), uuid.NewString())
  state.Sequences = append(state.Sequences, new_seq)
  return new_seq.Uuid, new_seq
}
func seq_iter_f(ctx context.Context, metadata types.Metadata) (map[string]proto.Message, error) {
  got_objs := make(map[string]proto.Message)
  it, err := metadata.ListAllSnapshotSeqs(ctx)
  if err != nil { return nil, err }
  obj := &pb.SnapshotSequence{}
  for it.Next(ctx, obj) { got_objs[obj.Uuid] = proto.Clone(obj) }
  return got_objs, it.Err()
}

func TestListAllSnapshotSeqs(t *testing.T) {
  const total = 3
  testMetadataListAll_Helper(t, total, seq_pb_f, seq_iter_f)
}
func TestListAllSnapshotSeqs_NoObjects(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()
  got_objs, err := seq_iter_f(ctx, metadata)
  if err != nil { t.Errorf("failed while iterating: %v", err) }
  if len(got_objs) > 0 { t.Errorf("should not return any object") }
}

func snap_pb_f(state *pb.AllMetadata) (string, proto.Message) {
  new_sv := util.DummySubVolume(uuid.NewString())
  state.Snapshots = append(state.Snapshots, new_sv)
  return new_sv.Uuid, new_sv
}
func snap_iter_f(ctx context.Context, metadata types.Metadata) (map[string]proto.Message, error) {
  got_objs := make(map[string]proto.Message)
  it, err := metadata.ListAllSnapshots(ctx)
  if err != nil { return nil, err }
  obj := &pb.SubVolume{}
  for it.Next(ctx, obj) { got_objs[obj.Uuid] = proto.Clone(obj) }
  return got_objs, it.Err()
}

func TestListAllSnapshots(t *testing.T) {
  const total = 3
  testMetadataListAll_Helper(t, total, snap_pb_f, snap_iter_f)
}
func TestListAllSnapshots_NoObjects(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _, clean_f := buildTestMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()
  got_objs, err := snap_iter_f(ctx, metadata)
  if err != nil { t.Errorf("failed while iterating: %v", err) }
  if len(got_objs) > 0 { t.Errorf("should not return any object") }
}

