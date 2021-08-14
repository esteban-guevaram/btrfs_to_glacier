package cloud
// AFAIK S3, Glacier S3 and Glacier are all different *incompatible* APIs.
// This implementation will use the vanilla S3 API.
// * All objects are written to Standard class and the transitioned to Glacier using lifecycle rules.
//   * S3 has an extra deep Glacier class.
//   * Can use Standard class for testing without waiting for restores.
// * All objects can be listed no matter the storage class.
//   * Convenient but comes at a cost of 40Kb (see S3 docs)

import (
  "context"
  "errors"
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
  bucket_wait_secs = 5
  deep_glacier_trans_days = 30
  remove_multipart_days = 3
  rule_name_suffix = "chunk.lifecycle"
)

// The subset of the s3 client used.
// Convenient for unittesting purposes.
type usedS3If interface {
  CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
  HeadBucket  (context.Context, *s3.HeadBucketInput,   ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
  ListBuckets (context.Context, *s3.ListBucketsInput,  ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
  PutBucketLifecycleConfiguration(context.Context, *s3.PutBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
}

type s3Storage struct {
  conf        *pb.Config
  codec       types.Codec
  aws_conf    *aws.Config
  client      usedS3If
  bucket_wait time.Duration
  deep_glacier_trans_days int32
  remove_multipart_days   int32
  rule_name_suffix        string
}

func NewStorage(conf *pb.Config, aws_conf *aws.Config, codec types.Codec) (types.Storage, error) {
  storage := &s3Storage{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: s3.NewFromConfig(*aws_conf),
    bucket_wait: bucket_wait_secs * time.Second,
    deep_glacier_trans_days: deep_glacier_trans_days,
    remove_multipart_days: remove_multipart_days,
    rule_name_suffix: rule_name_suffix,
  }
  return storage, nil
}

// Returns false if the bucket does not exist.
func (self *s3Storage) checkBucketExistsAndIsOwnedByMyAccount(ctx context.Context) (bool, error) {
  var err error
  var list_out *s3.ListBucketsOutput

  list_out, err = self.client.ListBuckets(ctx, &s3.ListBucketsInput{})
  if err != nil { return false, err }
  if list_out.Owner == nil {
    return false, fmt.Errorf("Expected owner in response from HeadBucket")
  }
  // Horrible terminology, this ID is an obfuscated version of the account number.
  canonical_account_id := *(list_out.Owner.ID)

  head_in := &s3.HeadBucketInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    ExpectedBucketOwner: &canonical_account_id,
  }
  _, err = self.client.HeadBucket(ctx, head_in)

  no_bucket_err := new(s3_types.NoSuchBucket)
  if errors.As(err, &no_bucket_err) {
    util.Debugf("'%s' does not exist", self.conf.Aws.S3.BucketName)
    return false, nil
  }
  if err != nil { return false, err }
  return true, nil
}

func (self *s3Storage) locationConstraintFromConf() (s3_types.BucketLocationConstraint, error) {
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
func (self *s3Storage) createBucket(ctx context.Context) error {
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
  return nil
}

// Bucket lifecycle configuration
// * Storage class applies to all objects in bucket (no prefix)
// * Transition for current objects Standard -> Deep Glacier after X days
// * Multipart uploads are removed after Y days
func (self *s3Storage) createLifecycleRule(ctx context.Context) error {
  name := fmt.Sprintf("%s.%s.%d",
                      self.conf.Aws.S3.BucketName, self.rule_name_suffix,
                      time.Now().Unix())
  transition := s3_types.Transition{
    Days: self.deep_glacier_trans_days,
    StorageClass: s3_types.TransitionStorageClassDeepArchive,
  }
  rule := s3_types.LifecycleRule{
    ID: &name,
    Status: s3_types.ExpirationStatusEnabled,
    Filter: nil, //global
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
func (self *s3Storage) SetupStorage(ctx context.Context) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    not_exist, err := self.checkBucketExistsAndIsOwnedByMyAccount(ctx)
    if err != nil { done <- err ; return }
    if !not_exist { done <- nil ; return }

    err = self.createBucket(ctx)
    if err != nil { done <- err ; return }
    err = self.createLifecycleRule(ctx)
    if err != nil { done <- err ; return }
    done <- nil
  }()
  return done
}

func (self *s3Storage) WriteStream() error { return nil }
func (self *s3Storage) SendRestoreRq() error { return nil }
func (self *s3Storage) ReadStream() error { return nil }
func (self *s3Storage) DeleteBlob() error { return nil }

