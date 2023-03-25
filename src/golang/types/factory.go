package types

import (
  "context"
)

type DeferBackupManager struct {
  Create   func(context.Context) (BackupManagerAdmin, error)
  TearDown func(context.Context) error
}

type DeferRestoreManager = func(context.Context) (RestoreManager, error)

type Factory interface {
  BuildBackupManager(context.Context, string) (DeferBackupManager, error)
  BuildRestoreManager(context.Context, string) (DeferRestoreManager, error)
}

