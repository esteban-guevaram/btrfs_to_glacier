package util

import (
  "btrfs_to_glacier/types"
)

type iniFileConfig struct {}

func Load() (types.Config, error) {
  conf := iniFileConfig{}
  return &conf, nil
}

