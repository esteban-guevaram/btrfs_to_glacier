package util

import (
  pb "btrfs_to_glacier/messages"
)

func Load() (*pb.Config, error) {
  conf := pb.Config{}
  return &conf, nil
}

func LoadTestConf() *pb.Config {
  return &pb.Config {
    RootSnapPath: "/tmp",
    SubvolPaths: []string { "/tmp/subvol1", },
    Aws: &pb.Aws {
      AccessKeyId: "coucou",
      SecretAccessKey: "coucou",
      Region: "coucou",
      DynamoTableName: "coucou",
    },
  }
}

