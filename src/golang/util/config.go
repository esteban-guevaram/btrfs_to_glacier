package util

import (
  "flag"
  pb "btrfs_to_glacier/messages"
)

var access_flag string
var secret_flag string
var session_flag string
var region_flag string
var table_flag string
var bucket_flag string

func init() {
  flag.StringVar(&access_flag,  "access",  "", "Access Key ID")
  flag.StringVar(&secret_flag,  "secret",  "", "Secret access key")
  flag.StringVar(&session_flag, "session", "", "Session token for temporal credentials")
  flag.StringVar(&region_flag,  "region",  "", "Default AWS region")
  flag.StringVar(&table_flag,   "table",   "", "Dynamodb table name")
  flag.StringVar(&bucket_flag,  "bucket",  "", "S3 bucket name")
}

func Load() (*pb.Config, error) {
  conf := &pb.Config{}
  overwriteWithFlags(conf)
  err := Validate(conf)
  return conf, err
}

func overwriteWithFlags(conf *pb.Config) {
  flag.Parse()
  if access_flag  != "" { conf.Aws.AccessKeyId = access_flag }
  if secret_flag  != "" { conf.Aws.SecretAccessKey = secret_flag }
  if session_flag != "" { conf.Aws.SessionToken = session_flag }
  if region_flag  != "" { conf.Aws.Region = region_flag }
  if table_flag   != "" { conf.Aws.DynamoDb.TableName = table_flag }
  if bucket_flag  != "" { conf.Aws.S3.BucketName = bucket_flag }
}

func LoadTestConf() *pb.Config {
  source := &pb.Source{
    Type: pb.Source_BTRFS,
    Paths: []*pb.Source_VolSnapPathPair{
      &pb.Source_VolSnapPathPair{
        VolPath: "/tmp/subvol1",
        SnapPath: "/tmp/snaps",
      },
    },
    History: &pb.Source_SnapHistory{
      DaysKeepAll: 30,
      KeepOnePeriodDays: 30,
    },
  }
  conf := pb.Config {
    Sources: []*pb.Source{ source, },
    Aws: &pb.Aws {
      AccessKeyId: "coucou",
      SecretAccessKey: "coucou",
      Region: "eu-central-1", // needs to be valid for unittests
      DynamoDb: &pb.Aws_DynamoDb{ TableName: "coucou", },
      S3: &pb.Aws_S3{ BucketName: "coucou", ChunkLen: 1024*1024, },
    },
  }
  overwriteWithFlags(&conf)
  return &conf
}

func Validate(conf *pb.Config) error {
  // Each subvolume should be mentioned in only 1 source
  // Sources, Stores, Restores, Workflows, Tools have a unique name
  // Reference by name not dangling
  return nil
}

