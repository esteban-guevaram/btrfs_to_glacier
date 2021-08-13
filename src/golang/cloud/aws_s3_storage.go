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
  iam_types "github.com/aws/aws-sdk-go-v2/service/iam/types"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
  bucket_wait_millis = 2000
)

// The subset of the s3 client used.
// Convenient for unittesting purposes.
type usedS3If interface {
  CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
  GetBucketAcl(context.Context, *s3.GetBucketAclInput, ...func(*s3.Options)) (*s3.GetBucketAclOutput, error)
  // Needed by the bucket waiter helper
  HeadBucket  (context.Context, *s3.HeadBucketInput,   ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

type s3Storage struct {
  conf        *pb.Config
  codec       types.Codec
  aws_conf    *aws.Config
  client      usedS3If
  bucket_wait time.Duration
}

func NewStorage(conf *pb.Config, aws_conf *aws.Config, codec types.Codec) (types.Storage, error) {
  storage := &s3Storage{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: s3.NewFromConfig(*aws_conf),
    bucket_wait: bucket_wait_millis * time.Millisecond,
  }
  return storage, nil
}

// Returns false if the bucket does not exist.
func (self *s3Storage) checkBucketExistsAndIsOwnedByMyAccount(ctx context.Context) (bool, error) {
  var err error
  var get_acl_out *s3.GetBucketAclOutput
  var owner, my_user *iam_types.User

  get_acl_in := &s3.GetBucketAclInput{ Bucket: &self.conf.Aws.S3.BucketName, }
  // We use GetBucketAcl instead of HeadBucket to get the owner and check we are not writing to a public bucket.
  get_acl_out, err = self.client.GetBucketAcl(ctx, get_acl_in)

  no_bucket_err := new(s3_types.NoSuchBucket)
  if errors.As(err, &no_bucket_err) {
    util.Debugf("'%s' does not exist", self.conf.Aws.S3.BucketName)
    return false, nil
  }
  if err != nil { return false, err }
  if get_acl_out.Owner == nil || get_acl_out.Owner.DisplayName == nil {
    return false, fmt.Errorf("Expected owner in response from GetBucketAcl")
  }

  owner_name := *(get_acl_out.Owner.DisplayName)
  owner, err = GetUserByName(ctx, self.aws_conf, &owner_name)
  if err != nil { return false, err }
  util.Debugf("'%s' is owned by '%s'", self.conf.Aws.S3.BucketName, *(owner.UserName))
  my_user, err = GetUserByName(ctx, self.aws_conf, nil)
  if err != nil { return false, err }

  owner_account := GetAccountIdForUser(owner)
  my_account := GetAccountIdForUser(my_user)
  if my_account != owner_account {
    return false, fmt.Errorf("'%s' is not a bucket we own.", self.conf.Aws.S3.BucketName)
  }
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
func (self *s3Storage) createBucketIdempotent(ctx context.Context) error {
  var err error
  var not_exist bool
  var bucket_location s3_types.BucketLocationConstraint

  not_exist, err = self.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err != nil { return err }
  if !not_exist { return nil }

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

func (self *s3Storage) createLifecycleRulesIdempotent(ctx context.Context) error {
  return nil
}

// object standard tier
// object no tags no metadata (that way we only need a simple kv store)
// lifecycle no prefix (all bucket)
// lifecycle direct standard -> deep after 30d
func (self *s3Storage) SetupStorage(ctx context.Context) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    err := self.createBucketIdempotent(ctx)
    if err != nil {
      done <- err
      return
    }
    err = self.createLifecycleRulesIdempotent(ctx)
    if err != nil {
      done <- err
      return
    }
  }()
  return done
}

func (self *s3Storage) WriteStream() error { return nil }
func (self *s3Storage) SendRestoreRq() error { return nil }
func (self *s3Storage) ReadStream() error { return nil }
func (self *s3Storage) DeleteBlob() error { return nil }

