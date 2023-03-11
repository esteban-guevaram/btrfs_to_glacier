package encryption

import (
  "context"
  "fmt"

  pb "btrfs_to_glacier/messages"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/credentials"
)

func NewAwsConfigFromTempCreds(
    conf *pb.Config, user pb.Aws_UserType) (*aws.Config, error) {
  return nil, fmt.Errorf("not_implemented")
}

func TestOnlyAwsConfFromPlainKey(
    conf *pb.Config, access_key string, secret_key string, session string) (*aws.Config, error) {
  creds := credentials.StaticCredentialsProvider{
    Value: aws.Credentials{
      AccessKeyID: access_key,
      SecretAccessKey: secret_key,
      SessionToken: session,
    },
  }
  cfg, err := config.LoadDefaultConfig(
    context.Background(),
    config.WithCredentialsProvider(creds),
    config.WithDefaultRegion(conf.Aws.Region),
  )
  return &cfg, err
}

