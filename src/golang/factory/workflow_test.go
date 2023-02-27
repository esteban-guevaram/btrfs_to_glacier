package factory

import (
  "context"
  "errors"
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
  _, err :=  BuildBackupManagerAdmin(ctx, conf, bkp.Name)
  if !errors.Is(err, ErrBadConfig) { t.Errorf("BuildBackupManagerAdmin bad err: %v", err) }
}

