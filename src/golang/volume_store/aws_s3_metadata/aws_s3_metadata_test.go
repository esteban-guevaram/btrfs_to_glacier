package aws_s3_metadata

import (
  "context"
  "testing"
  "time"

  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
  }
  meta.injectConstants()
  return meta, client
}


