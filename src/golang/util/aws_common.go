package util

import (
  "context"

  pb "btrfs_to_glacier/messages"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/aws/arn"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/credentials"
  "github.com/aws/aws-sdk-go-v2/service/sts"
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

func GetAccountId(ctx context.Context, aws_conf *aws.Config) (string, error) {
  var err error
  var res_name arn.ARN
  var ident_out *sts.GetCallerIdentityOutput

  client := sts.NewFromConfig(*aws_conf)
  ident_in := &sts.GetCallerIdentityInput{}
  ident_out, err = client.GetCallerIdentity(ctx, ident_in)
  if err != nil { return "", err }

  res_name, err = arn.Parse(*(ident_out.Arn))
  if err != nil { return "", err }
  return res_name.AccountID, nil
}

