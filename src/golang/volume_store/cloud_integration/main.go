package main

import (
  "context"
  "fmt"
  "strconv"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"

  "google.golang.org/protobuf/proto"
)

func main() {
  util.Infof("cloud_integration run")

  ctx := context.Background()
  //conf := useUniqueInfrastructureNames(util.LoadTestConf())
  conf := util.LoadTestConf()

  aws_conf, err := util.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }

  TestCallerIdentity(ctx, conf, aws_conf)
  TestAllS3Storage(ctx, conf, aws_conf)
  TestAllDynamoDbMetadata(ctx, conf, aws_conf)
  util.Infof("ALL DONE")
}

func TestCallerIdentity(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  var err error
  var id_int int
  var account_id string
  account_id, err = util.GetAccountId(ctx, aws_conf)
  if err != nil { util.Fatalf("%v", err) }
  id_int, err = strconv.Atoi(account_id)
  if err != nil || id_int < 1 { util.Fatalf("invalid account id") }
}

func useUniqueInfrastructureNames(conf *pb.Config) *pb.Config {
  new_conf := proto.Clone(conf).(*pb.Config)
  new_conf.Aws.DynamoDb.TableName = fmt.Sprintf("%s%d", conf.Aws.DynamoDb.TableName,
                                                time.Now().Unix())
  new_conf.Aws.S3.BucketName = fmt.Sprintf("%s%d", conf.Aws.S3.BucketName,
                                           time.Now().Unix())
  return new_conf
}
