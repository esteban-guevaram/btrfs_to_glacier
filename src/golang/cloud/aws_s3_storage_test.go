package cloud

import (
  "context"
  "fmt"
  "testing"
  "time"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockS3Client struct {
  Err error
  CanonId string
  Data map[string][]byte
  Buckets map[string]bool
  HeadAlwaysEmpty bool
  HeadAlwaysAccessDenied bool
  LastLifecycleIn *s3.PutBucketLifecycleConfigurationInput
}

func (self *mockS3Client) CreateBucket(
    ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
  self.Buckets[*in.Bucket] = true
  return &s3.CreateBucketOutput{}, self.Err
}
func (self *mockS3Client) ListBuckets (
    context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
  buckets := make([]s3_types.Bucket, 0, len(self.Buckets))
  for name,_ := range self.Buckets {
    buckets = append(buckets, s3_types.Bucket{ Name: &name, })
  }
  rs := &s3.ListBucketsOutput{
    Buckets: buckets,
    Owner: &s3_types.Owner{ ID: &self.CanonId, },
  }
  return rs, self.Err
}
func (self *mockS3Client) HeadBucket(
    ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
  _,found := self.Buckets[*(in.Bucket)]
  if self.HeadAlwaysEmpty || !found {
    return nil, new(s3_types.NoSuchBucket)
  }
  bad_owner := in.ExpectedBucketOwner != nil && *(in.ExpectedBucketOwner) != self.CanonId
  if self.HeadAlwaysAccessDenied || bad_owner {
    // Error model is too complex to mock
    // https://aws.github.io/aws-sdk-go-v2/docs/handling-errors/#api-error-responses
    return nil, fmt.Errorf("AccessDenied")
  }
  return &s3.HeadBucketOutput{}, self.Err
}
func (self *mockS3Client) PutBucketLifecycleConfiguration(
    ctx context.Context, in *s3.PutBucketLifecycleConfigurationInput, opts ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
  self.LastLifecycleIn = in
  rs := &s3.PutBucketLifecycleConfigurationOutput{}
  return rs, self.Err
}

func buildTestStorage(t *testing.T) (*s3Storage, *mockS3Client) {
  conf := util.LoadTestConf()
  client := &mockS3Client {
    CanonId: "some_random_string",
    Data: make(map[string][]byte),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  codec := new(types.MockCodec)
  aws_conf, err := NewAwsConfig(context.TODO(), conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }

  storage := &s3Storage{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: client,
    bucket_wait: 10 * time.Millisecond,
    deep_glacier_trans_days: deep_glacier_trans_days,
    remove_multipart_days: remove_multipart_days,
    rule_name_suffix: rule_name_suffix,
  }
  return storage, client
}

func TestBucketCreation_Immediate(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorage(t)
  err := storage.createBucket(ctx)
  if err != nil { t.Fatalf("Failed aws create bucket: %v", err) }
}

func TestBucketCreation_Timeout(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  client.HeadAlwaysEmpty = true
  err := storage.createBucket(ctx)
  if err == nil { t.Fatalf("Expected create bucket to timeout") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_NoBucket(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorage(t)
  exists, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if exists { t.Fatalf("there should have been no bucket") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_BadOwner(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  client.HeadAlwaysAccessDenied = true
  client.Buckets[storage.conf.Aws.S3.BucketName] = true
  _, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err == nil { t.Fatalf("Expected wrong bucket owner") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_Exists(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  client.Buckets[storage.conf.Aws.S3.BucketName] = true
  exists, err := storage.checkBucketExistsAndIsOwnedByMyAccount(ctx)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if !exists { t.Fatalf("there should have been an existing bucket") }
}

func TestCreateLifecycleRule(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  err := storage.createLifecycleRule(ctx)
  if err != nil { t.Fatalf("Failed lifecycle creation: %v", err) }
  if client.LastLifecycleIn.LifecycleConfiguration == nil { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
  lf_conf := client.LastLifecycleIn.LifecycleConfiguration
  if len(lf_conf.Rules) != 1 { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
}

func TestSetupStorage(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,_ := buildTestStorage(t)
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

func TestSetupStorage_Fail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  storage,client := buildTestStorage(t)
  client.Err = fmt.Errorf("an unfortunate error")
  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err == nil { t.Errorf("Expected error in SetupStorage") }
    case <-ctx.Done():
      t.Fatalf("TestSetupStorage timeout")
  }
}

