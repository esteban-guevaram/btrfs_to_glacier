package aws_s3_common

import (
  "context"
  "testing"

  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func buildTestAdminsetup(t *testing.T) (*S3Common, *MockS3Client) {
  conf := util.LoadTestConf()
  client := &MockS3Client {
    AccountId: "some_random_string",
    Data: make(map[string][]byte),
    Class: make(map[string]s3_types.StorageClass),
    RestoreStx: make(map[string]string),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  aws_conf, err := util.NewAwsConfigFromStaticCreds(conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }
  common, err := NewS3Common(conf, aws_conf, conf.Backups[0].Name, client)
  if err != nil { t.Fatalf("Failed build common setup: %v", err) }
  common.BucketWait = util.TestTimeout
  common.AccountId = client.AccountId

  return common, client
}

func TestBucketCreation_Immediate(t *testing.T) {
  const bucket = "bucket_name"
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,client := buildTestAdminsetup(t)
  err := setup.CreateBucket(ctx, bucket)
  if err != nil { t.Fatalf("Failed aws create bucket: %v", err) }
  if _,ok := client.Buckets[bucket]; !ok {
    t.Fatalf("Create bucket did not do a thing: %v", err)
  }
  block_conf := client.LastPublicAccessBlockIn.PublicAccessBlockConfiguration
  if !block_conf.BlockPublicAcls { t.Fatalf("Malformed request: %v", *(client.LastPublicAccessBlockIn)) }
}

func TestBucketCreation_Timeout(t *testing.T) {
  const bucket = "bucket_name"
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,client := buildTestAdminsetup(t)
  client.HeadAlwaysEmpty = true
  err := setup.CreateBucket(ctx, bucket)
  if err == nil { t.Fatalf("Expected create bucket to timeout") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_NoBucket(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,_ := buildTestAdminsetup(t)
  bucket := setup.BackupConf.StorageBucketName
  exists, err := setup.CheckBucketExistsAndIsOwnedByMyAccount(ctx, bucket)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if exists { t.Fatalf("there should have been no bucket") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_BadOwner(t *testing.T) {
  const bucket = "bucket_name"
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,client := buildTestAdminsetup(t)
  client.HeadAlwaysAccessDenied = true
  client.Buckets[bucket] = true
  _, err := setup.CheckBucketExistsAndIsOwnedByMyAccount(ctx, bucket)
  if err == nil { t.Fatalf("Expected wrong bucket owner") }
}

func TestCheckBucketExistsAndIsOwnedByMyAccount_Exists(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,client := buildTestAdminsetup(t)
  bucket := setup.BackupConf.MetadataBucketName
  client.Buckets[bucket] = true
  exists, err := setup.CheckBucketExistsAndIsOwnedByMyAccount(ctx, bucket)
  if err != nil { t.Fatalf("Failed to check for existing bucket: %v", err) }
  if !exists { t.Fatalf("there should have been an existing bucket") }
}

