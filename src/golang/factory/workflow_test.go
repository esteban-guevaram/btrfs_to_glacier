package factory

import (
  "context"
  "testing"

  "btrfs_to_glacier/util"
)

func TestBackupManagerBadConfig_ConflictingConfs(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  conf := util.LoadTestConf()
  bkp := conf.Backups[0]
  fs, cleaner := util.LoadTestSimpleDirBackupConf()
  defer cleaner()
  bkp.Fs = fs
  factory, _ := NewFactory(conf)
  _, err := factory.BuildBackupManager(ctx, bkp.Name)
  if err == nil { t.Errorf("BuildBackupManagerAdmin bad err: %v", err) }
}

