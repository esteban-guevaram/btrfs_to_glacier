package backup_manager

import (
  "testing"

  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

func BuildBackupManager() *BackupManager {
  conf := util.LoadTestConf()
  meta := mocks.NewMetadata()
  store := mocks.NewStorage()
  source := mocks.NewVolumeManager()
  mgr, err := NewBackupManager(conf, "", meta, store, source)
  if err != nil { util.Fatalf("BuildBackupManager: %v", err) }
  return mgr.(*BackupManager)
}

func TestBackupAllToCurrentSequences_NewSeq(t *testing.T) { }
func TestBackupAllToCurrentSequences_AllNew(t *testing.T) { }
func TestBackupAllToCurrentSequences_Noop(t *testing.T) { }

