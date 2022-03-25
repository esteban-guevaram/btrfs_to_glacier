package backup_restore_canary

import (
  "testing"
)

func TestBackupRestoreCanary_Setup_OK(t *testing.T) {}
func TestBackupRestoreCanary_Setup_Noop(t *testing.T) {}
func TestBackupRestoreCanary_Setup_NewChain(t *testing.T) {}

func TestBackupRestoreCanary_TearDown_OK(t *testing.T) {}
func TestBackupRestoreCanary_TearDown_Noop(t *testing.T) {}
func TestBackupRestoreCanary_TearDown_Partial(t *testing.T) {}

func TestAppendSnapshotToValidationChain_NewChain(t *testing.T) {}
func TestAppendSnapshotToValidationChain_ExistingChain(t *testing.T) {}

func TestRestoreChainAndValidate_NewChain(t *testing.T) {}
func TestRestoreChainAndValidate_ExistingChain(t *testing.T) {}
func TestRestoreChainAndValidate_BadUuidFile_MissLine(t *testing.T) {}
func TestRestoreChainAndValidate_BadUuidFile_BadLine(t *testing.T) {}
func TestRestoreChainAndValidate_BadDelDir_MissFile(t *testing.T) {}
func TestRestoreChainAndValidate_BadDelDir_BadContent(t *testing.T) {}
func TestRestoreChainAndValidate_BadNewDir_MissFile(t *testing.T) {}
func TestRestoreChainAndValidate_BadNewDir_BadContent(t *testing.T) {}

