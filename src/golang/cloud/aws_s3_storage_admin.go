package cloud

import (
  "context"
  "fmt"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
  // see https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Client.DeleteObjects
  delete_objects_max = 1000
)

type s3AdminStorage struct {
  *s3Storage
}

func NewAdminStorage(conf *pb.Config, aws_conf *aws.Config, codec types.Codec) (types.AdminStorage, error) {
  storage, err := NewStorage(conf, aws_conf, codec)
  if err != nil { return nil, err }

  del_storage := &s3AdminStorage{ s3Storage: storage.(*s3Storage), }
  return del_storage, nil
}

// Returns false if the bucket does not exist.
func (self *s3AdminStorage) checkBucketExistsAndIsOwnedByMyAccount(ctx context.Context) (bool, error) {
  var err error
  if len(self.account_id) < 1 {
    self.account_id, err = GetAccountId(ctx, self.aws_conf)
    if err != nil { return false, err }
  }

  head_in := &s3.HeadBucketInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    ExpectedBucketOwner: &self.account_id,
  }
  _, err = self.client.HeadBucket(ctx, head_in)

  if IsS3Error(new(s3_types.NotFound), err) || IsS3Error(new(s3_types.NoSuchBucket), err) {
    util.Debugf("Bucket '%s' does not exist", self.conf.Aws.S3.BucketName)
    return false, nil
  }
  if err != nil { return false, err }
  return true, nil
}

func (self *s3AdminStorage) locationConstraintFromConf() (s3_types.BucketLocationConstraint, error) {
  var invalid s3_types.BucketLocationConstraint = ""
  for _,region := range s3_types.BucketLocationConstraintEu.Values() {
    if string(region) == self.conf.Aws.Region { return region, nil }
  }
  return invalid, fmt.Errorf("region '%s' does not match any location constraint",
                             self.conf.Aws.Region)
}

// Bucket creation parameters:
// * no server side encryption
// * no object lock, not versioning for objects
// * block all public access
func (self *s3AdminStorage) createBucket(ctx context.Context) error {
  var err error
  var bucket_location s3_types.BucketLocationConstraint

  bucket_location, err = self.locationConstraintFromConf()
  if err != nil { return err }

  create_in := &s3.CreateBucketInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    ACL: s3_types.BucketCannedACLPrivate,
    CreateBucketConfiguration: &s3_types.CreateBucketConfiguration{
      LocationConstraint: bucket_location,
    },
    ObjectLockEnabledForBucket: false,
  }
  _, err = self.client.CreateBucket(ctx, create_in)
  if err != nil { return err }

  waiter := s3.NewBucketExistsWaiter(self.client)
  wait_rq := &s3.HeadBucketInput{ Bucket: &self.conf.Aws.S3.BucketName, }
  err = waiter.Wait(ctx, wait_rq, self.bucket_wait)
  if err != nil { return err }

  access_in := &s3.PutPublicAccessBlockInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    PublicAccessBlockConfiguration: &s3_types.PublicAccessBlockConfiguration{
      BlockPublicAcls: true,
      IgnorePublicAcls: true,
      BlockPublicPolicy: true,
      RestrictPublicBuckets: true,
    },
  }
  _, err = self.client.PutPublicAccessBlock(ctx, access_in)
  if err != nil { return err }
  return nil
}

func (self *s3AdminStorage) getTransitionType() s3_types.TransitionStorageClass {
  for _,t := range s3_types.TransitionStorageClassDeepArchive.Values() {
    if string(self.archive_storage_class) == string(t) { return t }
  }
  util.Fatalf("No transition types corresponding to %v", self.archive_storage_class)
  return s3_types.TransitionStorageClassDeepArchive
}

// Bucket lifecycle configuration
// * Storage class applies to all objects in bucket (no prefix)
// * Transition for current objects Standard -> Deep Glacier after X days
// * Multipart uploads are removed after Y days
func (self *s3AdminStorage) createLifecycleRule(ctx context.Context) error {
  name := fmt.Sprintf("%s.%s.%d",
                      self.conf.Aws.S3.BucketName, self.rule_name_suffix,
                      time.Now().Unix())
  transition := s3_types.Transition{
    Days: self.deep_glacier_trans_days,
    StorageClass: self.getTransitionType(),
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
    NoncurrentVersionExpiration: nil,
    NoncurrentVersionTransitions: nil,
    Transitions: []s3_types.Transition{ transition },
  }
  lifecycle_in := &s3.PutBucketLifecycleConfigurationInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    LifecycleConfiguration: &s3_types.BucketLifecycleConfiguration{
      Rules: []s3_types.LifecycleRule{ rule },
    },
  }
  _, err := self.client.PutBucketLifecycleConfiguration(ctx, lifecycle_in)
  if err != nil { return err }
  return nil
}

// object standard tier
// object no tags no metadata (that way we only need a simple kv store)
func (self *s3AdminStorage) SetupStorage(ctx context.Context) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    exists, err := self.checkBucketExistsAndIsOwnedByMyAccount(ctx)
    if err != nil { done <- err ; return }
    if exists { return }

    err = self.createBucket(ctx)
    if err != nil { done <- err ; return }
    err = self.createLifecycleRule(ctx)
    if err != nil { done <- err ; return }
  }()
  return done
}

// Although operations on objects have read-after-write consistency, that does not apply to buckets.
// Deleting and creating buckets in quick succession and reading objects on that bucket
// with a **different client object** may return NoSuchBucket errors.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
func TestOnlyGetInnerClientToAvoidConsistencyFails(storage types.Storage) *s3.Client {
  s3_impl,ok := storage.(*s3AdminStorage)
  if !ok { util.Fatalf("called with the wrong impl") }
  client,ok := s3_impl.client.(*s3.Client)
  if !ok { util.Fatalf("storage does not contain a real aws client") }
  return client
}

func (self *s3AdminStorage) deleteBatch(
    ctx context.Context, low_bound int, up_bound int, chunks *pb.SnapshotChunks) error {
  del_in := &s3.DeleteObjectsInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    Delete: &s3_types.Delete{
      Objects: make([]s3_types.ObjectIdentifier, up_bound-low_bound),
      Quiet: true,
    },
  }
  for i:=low_bound; i<up_bound; i+=1 {
    del_in.Delete.Objects[i-low_bound].Key = &chunks.Chunks[i].Uuid
  }
  del_out,err := self.client.DeleteObjects(ctx, del_in)
  if err != nil { return err }
  if len(del_out.Errors) > 0 { return fmt.Errorf("failed to delete %d keys", len(del_out.Errors)) }
  return nil
}

func (self *s3AdminStorage) DeleteChunks(ctx context.Context, chunks *pb.SnapshotChunks) (<-chan error) {
  if len(chunks.Chunks) < 1 { return util.WrapInChan(fmt.Errorf("cannot delete 0 keys")) }
  done := make(chan error, 1)

  go func() {
    var err error
    defer close(done)
    for low_bound:=0; low_bound<len(chunks.Chunks); low_bound+=delete_objects_max {
      up_bound := low_bound + delete_objects_max
      if up_bound > len(chunks.Chunks) { up_bound = len(chunks.Chunks) }
      err := self.deleteBatch(ctx, low_bound, up_bound, chunks)
      if err != nil { break }
    }
    util.Infof("Deleted %d keys: '%s'...'%s'",
               len(chunks.Chunks), chunks.Chunks[0].Uuid, chunks.Chunks[len(chunks.Chunks)-1].Uuid)
    done <- err
  }()
  return done
}

