package aws_s3_metadata

import (
  "context"
  "fmt"
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
  old_version_days = 365
  rule_name_suffix = "version.lifecycle"
)

type S3MetadataAdmin struct {
  *S3Metadata
  remove_multipart_days int32
  old_version_days      int32
  rule_name_suffix      string
}

func NewMetadataAdmin(
    ctx context.Context, conf *pb.Config, aws_conf *aws.Config) (types.AdminMetadata, error) {
  metadata, err := NewMetadata(ctx, conf, aws_conf)
  if err != nil { return nil, err }

  admin := &S3MetadataAdmin{ S3Metadata: metadata.(*S3Metadata), }
  admin.injectConstants()
  return admin, nil
}

func (self *S3MetadataAdmin) injectConstants() {
  self.S3Metadata.injectConstants()
  self.remove_multipart_days = s3_common.RemoveMultipartDays
  self.old_version_days = old_version_days
  self.rule_name_suffix = rule_name_suffix
}

func (self *S3MetadataAdmin) SetupMetadata(
    ctx context.Context) (<-chan error) {
  bucket_name := self.Conf.Aws.S3.MetadataBucketName
  done := make(chan error, 1)
  go func() {
    defer close(done)
    exists, err := self.Common.CheckBucketExistsAndIsOwnedByMyAccount(ctx, bucket_name)
    if err != nil { done <- err ; return }
    if exists { return }

    err = self.Common.CreateBucket(ctx, bucket_name)
    if err != nil { done <- err ; return }
    err = self.enableVersioning(ctx, bucket_name)
    if err != nil { done <- err ; return }
    err = self.createLifecycleRule(ctx, bucket_name)
    if err != nil { done <- err ; return }
  }()
  return done
}

func (self *S3MetadataAdmin) enableVersioning(
    ctx context.Context, bucket_name string) error {
  versioning_in := &s3.PutBucketVersioningInput{
    Bucket: &bucket_name,
    VersioningConfiguration: &s3_types.VersioningConfiguration{
      Status: s3_types.BucketVersioningStatusEnabled,
    },
  }
  _, err := self.Client.PutBucketVersioning(ctx, versioning_in)
  if err != nil { return err }
  return nil
}

// Bucket lifecycle configuration
// * Keep non current versions for X days
// * Multipart uploads are removed after Y days
func (self *S3MetadataAdmin) createLifecycleRule(
    ctx context.Context, bucket_name string) error {
  name := fmt.Sprintf("%s.%s.%d",
                      bucket_name, self.rule_name_suffix,
                      time.Now().Unix())
  expiration := &s3_types.NoncurrentVersionExpiration{
    NoncurrentDays: self.old_version_days,
  }
  global_filter := &s3_types.LifecycleRuleFilterMemberPrefix{ Value: "", }
  rule := s3_types.LifecycleRule{
    ID: &name,
    Status: s3_types.ExpirationStatusEnabled,
    Filter: global_filter,
    AbortIncompleteMultipartUpload: &s3_types.AbortIncompleteMultipartUpload{
      DaysAfterInitiation: self.remove_multipart_days,
    },
    Expiration: nil,
    NoncurrentVersionExpiration: expiration,
    NoncurrentVersionTransitions: nil,
    Transitions: nil,
  }
  lifecycle_in := &s3.PutBucketLifecycleConfigurationInput{
    Bucket: &bucket_name,
    LifecycleConfiguration: &s3_types.BucketLifecycleConfiguration{
      Rules: []s3_types.LifecycleRule{ rule },
    },
  }
  _, err := self.Client.PutBucketLifecycleConfiguration(ctx, lifecycle_in)
  if err != nil { return err }
  return nil
}

func (self *S3MetadataAdmin) DeleteMetadataUuids(
    ctx context.Context, seq_uuids []string, snap_uuids []string) (<-chan error) {
  seq_set := make(map[string]bool)
  for _,uuid := range seq_uuids { seq_set[uuid] = true }
  snap_set := make(map[string]bool)
  for _,uuid := range snap_uuids { snap_set[uuid] = true }

  new_seqs := make([]*pb.SnapshotSequence, 0, len(self.State.Sequences))
  new_snaps := make([]*pb.SubVolume, 0, len(self.State.Snapshots))

  for _,seq := range self.State.Sequences {
    if seq_set[seq.Uuid] { continue }
    new_seqs = append(new_seqs, seq)
  }
  for _,snap := range self.State.Snapshots {
    if snap_set[snap.Uuid] { continue }
    new_snaps = append(new_snaps, snap)
  }

  self.State.Sequences = new_seqs
  self.State.Snapshots = new_snaps
  return util.WrapInChan(nil)
}

func (self *S3MetadataAdmin) ReplaceSnapshotSeqHead(
    ctx context.Context, head *pb.SnapshotSeqHead) (*pb.SnapshotSeqHead, error) {
  err := store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  idx, prev_head := self.findHead(head.Uuid)
  if prev_head == nil { return nil, fmt.Errorf("%w uuid=%v", types.ErrNotFound, head.Uuid) }

  self.State.Heads[idx] = proto.Clone(head).(*pb.SnapshotSeqHead)
  return prev_head, nil
}

// see `TestOnlyGetInnerClientToAvoidConsistencyFails` for s3 storage.
func TestOnlyGetInnerClientToAvoidConsistencyFails(metadata types.Metadata) *s3.Client {
  if metadata == nil { util.Fatalf("metadata == nil") }
  s3_impl,ok := metadata.(*S3MetadataAdmin)
  if !ok { util.Fatalf("called with the wrong impl") }
  client,ok := s3_impl.Client.(*s3.Client)
  if !ok { util.Fatalf("storage does not contain a real aws client") }
  return client
}

func TestOnlySetInnerState(metadata types.Metadata, state *pb.AllMetadata) {
  if metadata == nil { util.Fatalf("metadata == nil") }
  s3_impl,ok := metadata.(*S3MetadataAdmin)
  if !ok { util.Fatalf("called with the wrong impl") }
  s3_impl.State = proto.Clone(state).(*pb.AllMetadata)
}

