package backup_restore_canary

import (
  "context"
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

type Mocks struct {
  Btrfs       *mocks.Btrfsutil
  Lnxutil     *mocks.Linuxutil
  BackupMgr   *mocks.BackupManager
  RestoreMgr  *mocks.RestoreManager
  InitBackup  func(*mocks.BackupManager) error
}

func (self *Mocks) NewBackupManager(ctx context.Context,
    conf *pb.Config, src_name string) (types.BackupManagerAdmin, error) {
  self.BackupMgr.InitFromConfSource(conf.Sources[0])
  if self.InitBackup != nil {
    err := self.InitBackup(self.BackupMgr)
    if err != nil { return nil, err }
  }
  return self.BackupMgr, nil
}

func (self *Mocks) NewRestoreManager(ctx context.Context,
    conf *pb.Config, dst_name string) (types.RestoreManager, error) {
  self.RestoreMgr.InitFromConfRestore(conf.Restores[0])
  return self.RestoreMgr, nil
}

func (self *Mocks) CleanDirs() {
  for _,m := range self.Lnxutil.Mounts {
    util.RemoveAll(m.MountedPath)
  }
}

func buildBackupRestoreCanary(hist_len int) (*BackupRestoreCanary, *Mocks) {
  bkp := mocks.NewBackupManager()
  mock := &Mocks{
    Btrfs: &mocks.Btrfsutil{},
    Lnxutil: mocks.NewLinuxutil(),
    BackupMgr: bkp,
    RestoreMgr: mocks.NewRestoreManager(bkp),
  }
  mock.Btrfs.CreateDirs = true
  mock.InitBackup = func(bkp *mocks.BackupManager) error {
    for range make([]int, hist_len) {
      _, err := bkp.BackupAllToCurrentSequences(context.Background())
      if err != nil { return err }
    }
    return nil 
  }
  conf := util.LoadTestConf()
  canary,err := NewBackupRestoreCanary(conf, mock.Btrfs, mock.Lnxutil, mock)
  if err != nil { util.Fatalf("NewBackupRestoreCanary: %v", err) }
  return canary.(*BackupRestoreCanary), mock
}

func TestBackupRestoreCanary_Setup_OK(t *testing.T) {
  const hist_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()

  err := canary.Setup(ctx)
  if err != nil { t.Fatalf("Setup: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 1, 1, })
  util.EqualsOrFailTest(t, "No subvols expected", mock.Btrfs.VolCount(), 0)
  util.EqualsOrFailTest(t, "State is not new", canary.State.New, false)
  expect_volroot := canary.State.FakeConf.Sources[0].Paths[0].VolPath
  expect_snaproot := canary.State.FakeConf.Sources[0].Paths[0].SnapPath
  expect_dstroot := canary.State.FakeConf.Restores[0].RootRestorePath
  util.EqualsOrFailTest(t, "State bad volroot", canary.State.VolRoot, expect_volroot)
  util.EqualsOrFailTest(t, "State bad snaproot", canary.State.SnapRoot, expect_snaproot)
  util.EqualsOrFailTest(t, "State bad dstroot", canary.State.RestoreRoot, expect_dstroot)
}

func TestBackupRestoreCanary_Setup_Noop(t *testing.T) {
  const hist_len = 3
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()

  err := canary.Setup(ctx)
  if err != nil { t.Fatalf("Setup: %v", err) }
  err = canary.Setup(ctx)
  if err != nil { t.Fatalf("Setup: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 1, 1, })
}

func TestBackupRestoreCanary_Setup_NewChain(t *testing.T) {
  const hist_len = 0
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()

  err := canary.Setup(ctx)
  if err != nil { t.Fatalf("Setup: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 1, 1, })
  util.EqualsOrFailTest(t, "New subvols expected", mock.Btrfs.VolCount(), 1)
  util.EqualsOrFailTest(t, "State is not new", canary.State.New, true)
}

func TestBackupRestoreCanary_TearDown_Noop(t *testing.T) {
  const hist_len = 0
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()
  mock.Lnxutil.ForAllErrMsg("Should not get called")
  mock.Btrfs.Err = fmt.Errorf("Should not get called")

  err := canary.TearDown(ctx)
  if err != nil { t.Fatalf("TearDown: %v", err) }
}

func TestBackupRestoreCanary_TearDown_OK(t *testing.T) {
  const hist_len = 0
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()

  err := canary.Setup(ctx)
  if err != nil { t.Fatalf("Setup: %v", err) }
  err = canary.TearDown(ctx)
  if err != nil { t.Fatalf("TearDown: %v", err) }

  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 0, 0, })
}

func TestBackupRestoreCanary_TearDown_Partial(t *testing.T) {
  const hist_len = 0
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  canary, mock := buildBackupRestoreCanary(hist_len)
  defer mock.CleanDirs()
  mock.Lnxutil.ForMethodErrMsg(mock.Lnxutil.CreateBtrfsFilesystem, "injected_err")

  err := canary.Setup(ctx)
  if err == nil { t.Fatalf("expected error") }
  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 0, 1, })

  err = canary.TearDown(ctx)
  if err != nil { t.Fatalf("TearDown: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs state", mock.Lnxutil.ObjCounts(),
                                           []int{ 1, 0, 0, })
}

func TestAppendSnapshotToValidationChain_NewChain(t *testing.T) {}
func TestAppendSnapshotToValidationChain_ExistingChain(t *testing.T) {}
func TestAppendSnapshotToValidationChain_CallBeforeRestore(t *testing.T) {}
func TestAppendSnapshotToValidationChain_SeveralCalls(t *testing.T) {}

func TestRestoreChainAndValidate_NewChain(t *testing.T) {}
func TestRestoreChainAndValidate_ExistingChain(t *testing.T) {}
func TestRestoreChainAndValidate_BadUuidFile_MissLine(t *testing.T) {}
func TestRestoreChainAndValidate_BadUuidFile_BadLine(t *testing.T) {}
func TestRestoreChainAndValidate_BadDelDir_MissFile(t *testing.T) {}
func TestRestoreChainAndValidate_BadDelDir_BadContent(t *testing.T) {}
func TestRestoreChainAndValidate_BadNewDir_MissFile(t *testing.T) {}
func TestRestoreChainAndValidate_BadNewDir_BadContent(t *testing.T) {}
func TestRestoreChainAndValidate_CannotCallTwice(t *testing.T) {}

