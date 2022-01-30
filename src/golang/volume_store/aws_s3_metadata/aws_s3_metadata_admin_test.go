package aws_s3_metadata

import (
  "context"
  "fmt"
  "testing"
  "time"

  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"
  "btrfs_to_glacier/util"
)

func buildTestAdminMetadata_WithNilState(t *testing.T) (*S3MetadataAdmin, *s3_common.MockS3Client) {
  conf := util.LoadTestConf()
  metadata,client := buildTestMetadataWithConf(t, conf)
  metadata.State = nil
  admin := &S3MetadataAdmin{ S3Metadata:metadata, }
  admin.injectConstants()
  return admin, client
}

func TestCreateLifecycleRule(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,client := buildTestAdminMetadata_WithNilState(t)
  bucket := meta_admin.Conf.Aws.S3.MetadataBucketName
  client.Buckets[bucket] = true
  err := meta_admin.createLifecycleRule(ctx, bucket)
  if err != nil { t.Fatalf("Failed lifecycle creation: %v", err) }
  lf_conf := client.LastLifecycleIn.LifecycleConfiguration
  if len(lf_conf.Rules) != 1 { t.Fatalf("Malformed request: %v", *(client.LastLifecycleIn)) }
  if lf_conf.Rules[0].NoncurrentVersionExpiration == nil {
    t.Fatalf("Malformed request (no version expire): %v", *(client.LastLifecycleIn))
  }
}

func TestSetupMetadata(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,client := buildTestAdminMetadata_WithNilState(t)
  bucket := meta_admin.Conf.Aws.S3.MetadataBucketName
  done := meta_admin.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      if client.LastPublicAccessBlockIn == nil { t.Errorf("did not block ppublic access: %v", err) }
      if client.LastPutBucketVersioning == nil { t.Errorf("did not enable versions: %v", err) }
      if len(client.Buckets) != 1 { t.Errorf("Bad bucket creation: %v", err) } 
      if _,found := client.Buckets[bucket]; !found { t.Errorf("Bad bucket name: %v", err) } 
    case <-ctx.Done():
      t.Fatalf("TestSetupmeta_admin timeout")
  }
}

func TestSetupMetadata_Fail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,client := buildTestAdminMetadata_WithNilState(t)
  client.Err = fmt.Errorf("an unfortunate error")
  done := meta_admin.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err == nil { t.Errorf("Expected error in SetupMetadata") }
    case <-ctx.Done():
      t.Fatalf("TestSetupmeta_admin timeout")
  }
}

func TestSetupMetadata_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,client := buildTestAdminMetadata_WithNilState(t)
  bucket := meta_admin.Conf.Aws.S3.MetadataBucketName
  client.Buckets[bucket] = true
  done := meta_admin.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      if len(client.Buckets) != 1 { t.Errorf("Bad bucket creation: %v", err) } 
    case <-ctx.Done():
      t.Fatalf("TestSetupMetadata_Idempotent timeout")
  }
}

