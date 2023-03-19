package types

import (
  "context"
)

type DeferBackupManager struct {
  Create   func() (BackupManagerAdmin, error)
  TearDown func() error
}

type DeferRestoreManager struct {
  Create   func() (RestoreManager, error)
}

type Factory interface {
  BuildBackupManager(context.Context, string) (DeferBackupManager, error)
  BuildRestoreManager(context.Context, string) (DeferRestoreManager, error)
}

