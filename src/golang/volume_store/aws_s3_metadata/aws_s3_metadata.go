package aws_s3_metadata

import (
  "bytes"
  "context"
  "fmt"
  "io"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"
  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
)

const (
  MetadataKey = "subvolume_metadata"
)

// The subset of the s3 client used.
// Convenient for unittesting purposes.
type usedS3If interface {
  GetObject          (
    context.Context, *s3.GetObjectInput,     ...func(*s3.Options)) (*s3.GetObjectOutput, error)
  PutBucketVersioning(
    context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
  PutBucketLifecycleConfiguration(
    context.Context, *s3.PutBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
  PutObject          (
    context.Context, *s3.PutObjectInput,     ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type S3Metadata struct {
  Conf       *pb.Config
  AwsConf    *aws.Config
  Common     *s3_common.S3Common
  Client     usedS3If
  Key        string
  State      *pb.AllMetadata
}

func NewMetadata(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) (types.Metadata, error) {
  client := s3.NewFromConfig(*aws_conf)
  common, err := s3_common.NewS3Common(conf, aws_conf, client)
  if err != nil { return nil, err }

  metadata := &S3Metadata{
    Conf: conf,
    AwsConf: aws_conf,
    Client: client,
    Common: common,
  }
  metadata.injectConstants()
  err = metadata.LoadPreviousStateFromS3(ctx)
  return metadata, err
}

func (self *S3Metadata) injectConstants() {
  self.Key = MetadataKey
}

func (self *S3Metadata) LoadPreviousStateFromS3(ctx context.Context) error {
  if self.State != nil { util.Fatalf("Cannot load state twice") }
  self.State = &pb.AllMetadata{
    CreatedTs: uint64(time.Now().Unix()),
  }

  get_in := &s3.GetObjectInput{
    Bucket: &self.Conf.Aws.S3.MetadataBucketName,
    Key: &self.Key,
  }
  get_out, err := self.Client.GetObject(ctx, get_in)
  if s3_common.IsS3Error(new(s3_types.NoSuchKey), err) { return nil }
  // If this is the first time we use the metadata bucket.
  if s3_common.IsS3Error(new(s3_types.NoSuchBucket), err) { return nil }
  if err != nil { return err }

  defer get_out.Body.Close()
  data, err := io.ReadAll(get_out.Body)
  if err != nil { return err }
  err = proto.Unmarshal(data, self.State)
  return err
}

func (self *S3Metadata) SaveCurrentStateToS3(ctx context.Context) (string, error) {
  if self.State == nil { util.Fatalf("Cannot store nil state") }
  self.State.CreatedTs = uint64(time.Now().Unix())

  content_type := "application/octet-stream"
  data, err := proto.Marshal(self.State)
  if err != nil { return "", err }
  reader := bytes.NewReader(data)

  put_in := &s3.PutObjectInput{
    Bucket: &self.Conf.Aws.S3.MetadataBucketName,
    Key:    &self.Key,
    Body:   reader,
    ACL:    s3_types.ObjectCannedACLBucketOwnerFullControl,
    ContentType:  &content_type,
    StorageClass: s3_types.StorageClassStandard,
  }

  put_out, err := self.Client.PutObject(ctx, put_in)
  if err != nil { return "", err }
  if put_out.VersionId == nil {
    return "", fmt.Errorf("Got bad PutObjectOutput: %s", util.AsJson(put_out))
  }

  util.Infof("Saved metadata version: '%v'", *put_out.VersionId)
  return *put_out.VersionId, nil
}

func (self *S3Metadata) findHead(uuid string) (int,*pb.SnapshotSeqHead) {
  if self.State == nil { util.Fatalf("state not loaded") }
  for idx,head := range self.State.Heads {
    if head.Uuid == uuid { return idx,head }
  }
  return 0, nil
}
func (self *S3Metadata) findOrAppendHead(uuid string) *pb.SnapshotSeqHead {
  if _,head := self.findHead(uuid); head != nil { return head }
  head := &pb.SnapshotSeqHead{ Uuid: uuid, }
  self.State.Heads = append(self.State.Heads, head)
  return head
}

func (self *S3Metadata) findSeq(uuid string) *pb.SnapshotSequence {
  if self.State == nil { util.Fatalf("state not loaded") }
  for _,meta_seq := range self.State.Sequences {
    if meta_seq.Uuid == uuid { return meta_seq }
  }
  return nil
}
func (self *S3Metadata) findOrCloneSeq(seq *pb.SnapshotSequence) *pb.SnapshotSequence {
  if meta_seq := self.findSeq(seq.Uuid); meta_seq != nil { return meta_seq }
  meta_seq := proto.Clone(seq).(*pb.SnapshotSequence)
  self.State.Sequences = append(self.State.Sequences, meta_seq)
  return meta_seq
}

func (self *S3Metadata) findSnap(uuid string) *pb.SubVolume {
  if self.State == nil { util.Fatalf("state not loaded") }
  for _,meta_snap := range self.State.Snapshots {
    if meta_snap.Uuid == uuid { return meta_snap }
  }
  return nil
}
func (self *S3Metadata) findOrCloneSnap(snap *pb.SubVolume) *pb.SubVolume {
  if meta_snap := self.findSnap(snap.Uuid); meta_snap != nil { return meta_snap }
  meta_snap := proto.Clone(snap).(*pb.SubVolume)
  self.State.Snapshots = append(self.State.Snapshots, meta_snap)
  return meta_snap
}

func (self *S3Metadata) RecordSnapshotSeqHead(
    ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error) {
  err := store.ValidateSnapshotSequence(new_seq)
  if err != nil { return nil, err }

  uuid := new_seq.Volume.Uuid
  head := self.findOrAppendHead(uuid)

  if head.CurSeqUuid == new_seq.Uuid {
    util.PbInfof("Noop already current seq in head: %v", head)
    return proto.Clone(head).(*pb.SnapshotSeqHead), nil
  }

  if len(head.CurSeqUuid) > 0 { head.PrevSeqUuid = append(head.PrevSeqUuid, head.CurSeqUuid) }
  head.CurSeqUuid = new_seq.Uuid

  err = store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  util.PbInfof("Wrote head: %v", head)
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *S3Metadata) AppendSnapshotToSeq(
    ctx context.Context, seq *pb.SnapshotSequence, snap *pb.SubVolume) (*pb.SnapshotSequence, error) {
  err := store.ValidateSubVolume(store.CheckSnapNoContent, snap)
  if err != nil { return nil, err }

  new_seq := self.findOrCloneSeq(seq)
  if len(seq.SnapUuids) > 0 {
    last := seq.SnapUuids[len(seq.SnapUuids) - 1]
    if last == snap.Uuid {
      util.PbInfof("Noop already last snap in seq: %v", seq)
      return proto.Clone(new_seq).(*pb.SnapshotSequence), nil
    }
  }

  new_seq.SnapUuids = append(new_seq.SnapUuids, snap.Uuid)

  err = store.ValidateSnapshotSequence(new_seq)
  if err != nil { return nil, err }
  if new_seq.Volume.Uuid != snap.ParentUuid {
    return nil, util.PbErrorf("Sequence volume and snap parent do not match: %v, %v", new_seq, snap)
  }
  if new_seq.Volume.CreatedTs >= snap.CreatedTs {
    return nil, util.PbErrorf("Sequence volume created after snap: %v, %v", new_seq, snap)
  }

  util.PbInfof("Wrote sequence: %v", new_seq)
  return proto.Clone(new_seq).(*pb.SnapshotSequence), nil
}

func (self *S3Metadata) AppendChunkToSnapshot(
    ctx context.Context, snap *pb.SubVolume, data *pb.SnapshotChunks) (*pb.SubVolume, error) {
  err := store.ValidateSnapshotChunks(store.CheckChunkNotFirst, data)
  if err != nil { return nil, err }

  new_snap := self.findOrCloneSnap(snap)
  if new_snap.Data != nil && new_snap.Data.KeyFingerprint != data.KeyFingerprint {
    return nil, util.PbErrorf("Snapshot chunk key mismatch: %v, %v", new_snap, data)
  }
  if store.IsFullyContainedInSubvolume(new_snap, data) {
    util.PbInfof("Noop already last chunk in snap: %v", new_snap)
    return proto.Clone(new_snap).(*pb.SubVolume), nil
  }

  data_len := store.SubVolumeDataLen(snap)
  if data.Chunks[0].Start != data_len {
    return nil, util.PbErrorf("Snapshot chunk not contiguous: %v, %v", snap, data)
  }

  if new_snap.Data == nil {
    new_snap.Data = &pb.SnapshotChunks { KeyFingerprint: data.KeyFingerprint }
  }
  new_snap.Data.Chunks = append(new_snap.Data.Chunks, data.Chunks...)

  err = store.ValidateSubVolume(store.CheckSnapWithContent, new_snap)
  if err != nil { return nil, err }

  util.PbInfof("Wrote snapshot: %v", new_snap)
  return proto.Clone(new_snap).(*pb.SubVolume), nil
}

func (self *S3Metadata) ReadSnapshotSeqHead(
    ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshotSeqHead: uuid is nil") }

  _,head := self.findHead(uuid)
  if head == nil { return nil, types.ErrNotFound }

  err := store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  util.PbInfof("Read head: %v", head)
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *S3Metadata) ReadSnapshotSeq(
    ctx context.Context, uuid string) (*pb.SnapshotSequence, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshotSeq: uuid is nil") }

  seq := self.findSeq(uuid)
  if seq == nil { return nil, types.ErrNotFound }

  err := store.ValidateSnapshotSequence(seq)
  if err != nil { return nil, err }

  util.PbInfof("Read sequence: %v", seq)
  return proto.Clone(seq).(*pb.SnapshotSequence), nil
}

func (self *S3Metadata) ReadSnapshot(
    ctx context.Context, uuid string) (*pb.SubVolume, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshot: uuid is nil") }

  snap := self.findSnap(uuid)
  if snap == nil { return nil, types.ErrNotFound }

  err := store.ValidateSubVolume(store.CheckSnapWithContent, snap)
  if err != nil { return nil, err }

  util.PbInfof("Read subvolume: %v", snap)
  return proto.Clone(snap).(*pb.SubVolume), nil
}

type SnapshotSeqHeadIterator struct { List []*pb.SnapshotSeqHead; Idx int }
func (self *SnapshotSeqHeadIterator) Next(ctx context.Context, o *pb.SnapshotSeqHead) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotSeqHeadIterator) Err() error { return nil }

type SnapshotSequenceIterator struct { List []*pb.SnapshotSequence; Idx int }
func (self *SnapshotSequenceIterator) Next(ctx context.Context, o *pb.SnapshotSequence) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotSequenceIterator) Err() error { return nil }

type SnapshotIterator struct { List []*pb.SubVolume; Idx int }
func (self *SnapshotIterator) Next(ctx context.Context, o *pb.SubVolume) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotIterator) Err() error { return nil }

func (self *S3Metadata) ListAllSnapshotSeqHeads(
    ctx context.Context) (types.SnapshotSeqHeadIterator, error) {
  return &SnapshotSeqHeadIterator{ List: self.State.Heads, }, nil
}

func (self *S3Metadata) ListAllSnapshotSeqs(
    ctx context.Context) (types.SnapshotSequenceIterator, error) {
  return &SnapshotSequenceIterator{ List: self.State.Sequences, }, nil
}

func (self *S3Metadata) ListAllSnapshots(
    ctx context.Context) (types.SnapshotIterator, error) {
  return &SnapshotIterator{ List: self.State.Snapshots, }, nil
}

func (self *S3Metadata) PersistCurrentMetadataState(ctx context.Context) (string, error) {
  return self.SaveCurrentStateToS3(ctx)
}

