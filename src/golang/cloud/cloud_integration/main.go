package main

import (
  "context"
  "flag"
  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
)

var access_flag string
var secret_flag string
var session_flag string
var region_flag string
var table_flag string

func init() {
  flag.StringVar(&access_flag, "access", "", "Access Key ID")
  flag.StringVar(&secret_flag, "secret", "", "Secret access key")
  flag.StringVar(&session_flag, "session", "", "Session token for temporal credentials")
  flag.StringVar(&region_flag, "region", "", "Default AWS region")
  flag.StringVar(&table_flag,  "table",  "", "Dynamodb table name")
}

func loadConfig() *pb.Config {
  aws := &pb.Aws{
    AccessKeyId: access_flag,
    SecretAccessKey: secret_flag,
    SessionToken: session_flag,
    Region: region_flag,
    DynamoTableName: table_flag,
  }
  conf := &pb.Config{ Aws: aws, }
  util.Infof("Config: %v", aws)
  return conf
}

func main() {
  util.Infof("cloud_integration run")
  flag.Parse()

  var err error
  var aws_conf *aws.Config
  var metadata types.Metadata
  ctx := context.Background()
  conf := loadConfig()
  codec := new(types.MockCodec)
  aws_conf, err = cloud.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }
  metadata, err = cloud.NewMetadata(conf, aws_conf, codec)
  if err != nil { util.Fatalf("%v", err) }

  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }
  util.Infof("ALL DONE")
}

