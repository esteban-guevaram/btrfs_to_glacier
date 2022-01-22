package aws_s3_metadata

import (
  "context"
  "errors"
  "fmt"
  "testing"
  "time"

  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

func buildTestMetadataWithConf(t *testing.T, conf *pb.Config) (*S3Metadata, *s3_common.MockS3Client) {
  client := &s3_common.MockS3Client {
    AccountId: "some_random_string",
    Data: make(map[string][]byte),
    Class: make(map[string]s3_types.StorageClass),
    RestoreStx: make(map[string]string),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  aws_conf, err := util.NewAwsConfig(context.TODO(), conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }
  common, err := s3_common.NewS3Common(conf, aws_conf, client)
  if err != nil { t.Fatalf("Failed build common setup: %v", err) }
  common.BucketWait = 10 * time.Millisecond
  common.AccountId = client.AccountId

  meta := &S3Metadata{
    Conf: conf,
    AwsConf: aws_conf,
    Client: client,
    Common: common,
    State: &pb.AllMetadata{},
  }
  meta.injectConstants()
  return meta, client
}

func buildTestMetadataWithState(t *testing.T, state *pb.AllMetadata) (*S3Metadata, *s3_common.MockS3Client) {
  var err error
  conf := util.LoadTestConf()
  meta, client := buildTestMetadataWithConf(t, conf)
  client.Buckets[conf.Aws.S3.MetadataBucketName] = true
  err = client.PutProto(meta.Key, state, s3_types.StorageClassStandard, false)
  if err != nil { t.Fatalf("failed to set init state: %v", err) }
  meta.State = state
  return meta, client
}

func compareStates(t *testing.T, msg string, left *pb.AllMetadata, right *pb.AllMetadata) {
  util.EqualsOrFailTest(t, msg, left.Heads, right.Heads)
  util.EqualsOrFailTest(t, msg, left.Sequences, right.Sequences)
  util.EqualsOrFailTest(t, msg, left.Snapshots, right.Snapshots)
}

func TestLoadPreviousStateFromS3_NoBucket(t *testing.T) {
  conf := util.LoadTestConf()
  meta, client := buildTestMetadataWithConf(t, conf)
  meta.State = nil
  meta.LoadPreviousStateFromS3(context.TODO())
  util.EqualsOrFailTest(t, "Bad bucket", client.Buckets[conf.Aws.S3.MetadataBucketName], false)
  util.EqualsOrFailTest(t, "Bad object", client.Data[meta.Key], nil)
  compareStates(t, "expected empty state", meta.State, &pb.AllMetadata{})
}

func TestLoadPreviousStateFromS3_NoKey(t *testing.T) {
  meta, client := buildTestMetadataWithState(t, &pb.AllMetadata{})
  meta.State = nil
  client.DelObject(meta.Key)
  meta.LoadPreviousStateFromS3(context.TODO())
  util.EqualsOrFailTest(t, "Bad object", client.Data[meta.Key], nil)
  compareStates(t, "expected empty state", meta.State, &pb.AllMetadata{})
}

func TestLoadPreviousStateFromS3_PreviousState(t *testing.T) {
  _, expect_state := util.DummyAllMetadata()
  meta,_ := buildTestMetadataWithState(t, expect_state)
  meta.State = nil
  meta.LoadPreviousStateFromS3(context.TODO())
  compareStates(t, "expected empty state", meta.State, expect_state)
}

func TestSaveCurrentStateToS3_NoPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  meta, client := buildTestMetadataWithState(t, proto.Clone(expect_state).(*pb.AllMetadata))
  client.DelObject(meta.Key)

  version, err := meta.SaveCurrentStateToS3(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }
  persisted_state := &pb.AllMetadata{}
  err = client.GetProto(meta.Key, persisted_state)
  if err != nil { t.Errorf("client.GetProto error: %v", err) }
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToS3_WithPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, prev_state := util.DummyAllMetadata()
  var expect_state pb.AllMetadata = *prev_state
  meta, client := buildTestMetadataWithState(t, prev_state)

  new_seq := util.DummySnapshotSequence(vol_uuid, uuid.NewString())
  head, err := meta.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Fatalf("RecordSnapshotSeqHead error: %v", err) }
  expect_state.Heads[0] = head

  version, err := meta.SaveCurrentStateToS3(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }
  persisted_state := &pb.AllMetadata{}
  err = client.GetProto(meta.Key, persisted_state)
  if err != nil { t.Errorf("client.GetProto error: %v", err) }
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToS3_Err(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, prev_state := util.DummyAllMetadata()
  meta, client := buildTestMetadataWithState(t, prev_state)
  client.Err = fmt.Errorf("TestSaveCurrentStateToS3_Err")

  _, err := meta.SaveCurrentStateToS3(ctx)
  if err == nil { t.Errorf("Expected error got: %v", err) }
}

func TestRecordSnapshotSeqHead_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)
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

  metadata, _ := buildTestMetadataWithState(t, expect_state)

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
  metadata, _ := buildTestMetadataWithState(t, expect_state)
  var expect_head pb.SnapshotSeqHead = *expect_state.Heads[0]

  head, err := metadata.ReadSnapshotSeqHead(ctx, vol_uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSeqHead", head, &expect_head)
}

func TestReadSnapshotSeqHead_NoHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})

  _, err := metadata.ReadSnapshotSeqHead(ctx, "does_not_exist")
  if errors.Is(err, types.ErrNotFound) { return }
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestReadSnapshotSeq(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  _, expect_state := util.DummyAllMetadata()
  metadata, _ := buildTestMetadataWithState(t, expect_state)
  var expect_seq pb.SnapshotSequence = *expect_state.Sequences[0]

  seq, err := metadata.ReadSnapshotSeq(ctx, expect_seq.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "SnapshotSequence", seq, &expect_seq)
}

func TestReadSnapshotSeq_NoSequence(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})

  _, err := metadata.ReadSnapshotSeq(ctx, "does_not_exist")
  if errors.Is(err, types.ErrNotFound) { return }
  if err != nil { t.Errorf("Returned error: %v", err) }
}

func TestReadSnapshot(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  _, expect_state := util.DummyAllMetadata()
  expect_state.Snapshots[0].Data = util.DummyChunks("chunk_uuid")
  metadata, _ := buildTestMetadataWithState(t, expect_state)
  var expect_snap pb.SubVolume = *expect_state.Snapshots[0]

  snap, err := metadata.ReadSnapshot(ctx, expect_snap.Uuid)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, "Snapshot", snap, &expect_snap)
}

func TestReadSnapshotSeq_NoSnap(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})

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

  metadata, _ := buildTestMetadataWithState(t, state)
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
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})
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
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})
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
  metadata, _ := buildTestMetadataWithState(t, &pb.AllMetadata{})
  got_objs, err := snap_iter_f(ctx, metadata)
  if err != nil { t.Errorf("failed while iterating: %v", err) }
  if len(got_objs) > 0 { t.Errorf("should not return any object") }
}

