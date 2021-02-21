package util

import (
  "btrfs_to_glacier/types"
  pb "btrfs_to_glacier/messages"
)

type protoFileConfig struct {
  config pb.Config
}

func Load() (types.Config, error) {
  conf := protoFileConfig{}
  return &conf, nil
}

