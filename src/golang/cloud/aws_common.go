package cloud

import (
  "context"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/aws/arn"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/credentials"
  "github.com/aws/aws-sdk-go-v2/service/iam"
  iam_types "github.com/aws/aws-sdk-go-v2/service/iam/types"
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

// If `name` is nil then query for the running user.
func GetUserByName(ctx context.Context, aws_conf *aws.Config, name *string) (*iam_types.User, error) {
  client := iam.NewFromConfig(*aws_conf)
  get_user_in := &iam.GetUserInput{ UserName: name, }
  get_user_rs, err := client.GetUser(ctx, get_user_in)
  if err != nil { return nil, err }
  util.Debugf("'%v' is user '%s'", get_user_in, *(get_user_rs.User.UserName))
  return get_user_rs.User, nil
}

func GetAccountIdForUser(user *iam_types.User) string {
  arn, err := arn.Parse(*(user.Arn))
  if err != nil { util.Fatalf("Failed to parse user arn: %r", err) }
  return arn.AccountID
}

