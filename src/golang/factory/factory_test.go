package factory

import (
  "testing"

  "btrfs_to_glacier/util"
)

// A factory should be created without any prerequiste environment
// like mounted filesystems, access to AWS, etc...
func TestCanCreateFactoryWithoutAnyDeps(t *testing.T) {
  conf := util.LoadTestConf()
  _, err := NewFactory(conf)
  if err != nil { t.Errorf("NewFactory: %v", err) }
}

