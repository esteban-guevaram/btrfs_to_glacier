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

var subvol_flag string

func init() {
  flag.StringVar(&access_flag, "access", "", "Access Key ID")
  flag.StringVar(&secret_flag, "secret", "", "Secret access key")
  flag.StringVar(&session_flag, "session", "", "Session token for temporal credentials")
  flag.StringVar(&region_flag, "region", "", "Default AWS region")
  flag.StringVar(&table_flag,  "table",  "", "Dynamodb table name")

  flag.StringVar(&subvol_flag, "subvol",  "", "the fullpath to the btrfs subvolume")
}

func Load() (*pb.Config, error) {
  conf := pb.Config{}
  overwriteWithFlags(&conf)
  return &conf, nil
}

func overwriteWithFlags(conf *pb.Config) {
  flag.Parse()
  if subvol_flag != "" { conf.SubvolPaths = []string{subvol_flag} }
  if access_flag != "" { conf.Aws.AccessKeyId = access_flag }
  if secret_flag != "" { conf.Aws.SecretAccessKey = secret_flag }
  if session_flag != "" { conf.Aws.SessionToken = session_flag }
  if region_flag != "" { conf.Aws.Region = region_flag }
  if table_flag != "" { conf.Aws.DynamoDb.TableName = table_flag }
}

func LoadTestConf() *pb.Config {
  conf := pb.Config {
    RootSnapPath: "/tmp",
    SubvolPaths: []string { "/tmp/subvol1", },
    Aws: &pb.Aws {
      AccessKeyId: "coucou",
      SecretAccessKey: "coucou",
      Region: "coucou",
      DynamoDb: &pb.Aws_DynamoDb{ TableName: "coucou", },
    },
  }
  overwriteWithFlags(&conf)
  return &conf
}

