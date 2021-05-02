package cloud

import (
  "context"
  pb "btrfs_to_glacier/messages"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/credentials"
)

func NewAwsConfig(ctx context.Context, conf *pb.Config) (*aws.Config, error) {
  creds := credentials.StaticCredentialsProvider{
    Value: aws.Credentials{
      AccessKeyID: conf.Aws.AccessKeyId,
      SecretAccessKey: conf.Aws.SecretAccessKey,
      SessionToken: conf.Aws.SessionToken,
    },
  }
  cfg, err := config.LoadDefaultConfig(ctx,
    config.WithCredentialsProvider(creds),
    config.WithDefaultRegion(conf.Aws.Region),
  )
  return &cfg, err
}

