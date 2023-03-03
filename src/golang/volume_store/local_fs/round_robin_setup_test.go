package local_fs

import (
  "context"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

func buildTestRoundRobinSetupWithState(
    t *testing.T, state *pb.AllMetadata) (*RoundRobinSetup, *mocks.Linuxutil, func()) {
  local_fs, clean_f := util.LoadTestMultiSinkBackupConf(1, 3, state != nil)
  conf := util.LoadTestConfWithLocalFs(local_fs)

  if state != nil {
    for _,p := range local_fs.Sinks[0].Partitions {
      writer := SimpleDirRw{p}
      if err := writer.PutState(state); err != nil {  t.Fatalf("%v", err) }
    }
  }
  lu := mocks.NewLinuxutil()
  setup, err := NewRoundRobinSetup(conf, lu, conf.Backups[0].Name)
  if err != nil { t.Fatalf("NewRoundRobinSetup %v", err) }
  return setup, lu, clean_f
}

func TestSetupMountAllSinkPartitions(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,lu,clean_f := buildTestRoundRobinSetupWithState(t, &pb.AllMetadata{})
  defer clean_f()

  err := setup.MountAllSinkPartitions(ctx)
  if err != nil { t.Fatalf("MountAllSinkPartitions err: %v", err) }
  expect_mounts := len(setup.Sink.Partitions)
  util.EqualsOrFailTest(t, "Bad mount count", lu.ObjCounts().Mounts, expect_mounts)
}

func TestSetupMountAllSinkPartitions_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  setup,lu,clean_f := buildTestRoundRobinSetupWithState(t, &pb.AllMetadata{})
  defer clean_f()

  err := setup.MountAllSinkPartitions(ctx)
  if err != nil { t.Fatalf("MountAllSinkPartitions err: %v", err) }
  err = setup.MountAllSinkPartitions(ctx)
  if err != nil { t.Fatalf("MountAllSinkPartitions err: %v", err) }
  err = setup.MountAllSinkPartitions(ctx)
  if err != nil { t.Fatalf("MountAllSinkPartitions err: %v", err) }
  expect_mounts := len(setup.Sink.Partitions)
  util.EqualsOrFailTest(t, "Bad mount count", lu.ObjCounts().Mounts, expect_mounts)
}

