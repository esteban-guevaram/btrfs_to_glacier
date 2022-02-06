package local_fs_metadata

import (
  "context"
  "fmt"
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"
)

func buildTestRoundRobinMetadataWithState(
    t *testing.T, state *pb.AllMetadata) (*RoundRobinMetadata, func()) {
  local_fs, clean_f := util.TestMultiSinkLocalFs(1, 3, state != nil)
  conf := util.LoadTestConfWithLocalFs(local_fs)

  if state != nil {
    for _,p := range local_fs.Sinks[0].Partitions {
      writer := SimpleDirRw{p}
      if err := writer.PutState(state); err != nil {  t.Fatalf("%v", err) }
    }
  }
  meta := &RoundRobinMetadata{
    SimpleDirMetadata: nil,
    Sink: local_fs.Sinks[0],
    Conf: conf,
    Linuxutil: &mocks.Linuxutil{},
  }
  return meta, clean_f
}

func callSetupMetadataSync(t *testing.T, meta *RoundRobinMetadata) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  done := meta.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Fatalf("SetupMetadata err: %v", err) }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func checkStateAfterSetup(t *testing.T, meta *RoundRobinMetadata, expect_state *pb.AllMetadata) {
  sink := meta.Conf.LocalFs.Sinks[0]
  lu := meta.Linuxutil.(*mocks.Linuxutil)
  found_part := false
  if meta.SimpleDirMetadata == nil { t.Fatalf("meta.SimpleDirMetadata == nil") }

  mount_map := make(map[string]bool)
  for _,m := range lu.Mounts { mount_map[m.Device.FsUuid] = true }

  for _,p := range sink.Partitions {
    if !util.IsDir(MetaDir(p)) { t.Errorf("!IsDir '%s'", MetaDir(p)) }
    if !mount_map[p.FsUuid] { t.Errorf("%s was not mounted", p.String()) }
    if p.FsUuid == meta.DirInfo.FsUuid {
      found_part = true
      client := SimpleDirRw{ p }
      persisted_state := client.GetState()
      if persisted_state == nil { persisted_state = &pb.AllMetadata{} }
      mem_only.CompareStates(t, "Bad persisted state", persisted_state, expect_state)
    }
  }
  if !found_part { t.Errorf("!found_part") }
  mem_only.CompareStates(t, "Bad state", meta.State, expect_state)
}

func TestSetupRoundRobinMetadata_AllPartitionsNew(t *testing.T) {
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, nil)
  defer clean_f()
  sink := meta.Conf.LocalFs.Sinks[0]
  for _,p := range sink.Partitions {
    if util.IsDir(MetaDir(p)) { t.Errorf("IsDir '%s'", MetaDir(p)) }
  }

  callSetupMetadataSync(t, meta)
  checkStateAfterSetup(t, meta, &pb.AllMetadata{})
}

func TestSetupRoundRobinMetadata_OneNewPartition(t *testing.T) {
  const part_idx = 1
  _, state := util.DummyAllMetadata()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, state)
  defer clean_f()
  sink := meta.Conf.LocalFs.Sinks[0]
  client := SimpleDirRw{ sink.Partitions[part_idx] }
  client.DeleteState(/*del_dir*/true)

  callSetupMetadataSync(t, meta)
  checkStateAfterSetup(t, meta, &pb.AllMetadata{})
  util.EqualsOrFailTest(t, "Bad partition index", meta.DirInfo.FsUuid, sink.Partitions[part_idx].FsUuid)
}

func TestSetupRoundRobinMetadata_ExistingPartitions(t *testing.T) {
  _, expect_state := util.DummyAllMetadata()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, expect_state)
  defer clean_f()

  callSetupMetadataSync(t, meta)
  checkStateAfterSetup(t, meta, expect_state)
}

func TestSetupRoundRobinMetadata_ExistingPartitionsWithoutState(t *testing.T) {
  _, state := util.DummyAllMetadata()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, state)
  defer clean_f()
  sink := meta.Conf.LocalFs.Sinks[0]
  for _,p := range sink.Partitions {
    client := SimpleDirRw{ p }
    client.DeleteState(/*del_dir*/false)
  }

  callSetupMetadataSync(t, meta)
  checkStateAfterSetup(t, meta, &pb.AllMetadata{})
}

func TestSetupRoundRobinMetadata_MountFail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, nil)
  defer clean_f()
  meta.Linuxutil.(*mocks.Linuxutil).Err = fmt.Errorf("mount_err")

  done := meta.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err == nil { t.Errorf("expected error") }
    case <-ctx.Done(): t.Fatalf("timedout")
  }
}

func TestSetupRoundRobinMetadata_Idempotent(t *testing.T) {
  _, expect_state := util.DummyAllMetadata()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, expect_state)
  defer clean_f()

  callSetupMetadataSync(t, meta)
  expect_fs := meta.DirInfo.FsUuid
  callSetupMetadataSync(t, meta)
  util.EqualsOrFailTest(t, "Should to same fs", meta.DirInfo.FsUuid, expect_fs)
}

func TestUMountAllSinkPartitions(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()

  callSetupMetadataSync(t, meta)
  err := meta.UMountAllSinkPartitions(ctx)
  if err != nil { t.Errorf("TestUMountAllSinkPartitions: %v", err) }

  lu := meta.Linuxutil.(*mocks.Linuxutil)
  util.EqualsOrFailTest(t, "Still mounted partitions", len(lu.Mounts), 0)
}

func TestReadWriteSaveCycle(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), util.TestTimeout)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  meta,clean_f := buildTestRoundRobinMetadataWithState(t, expect_state)
  defer clean_f()

  callSetupMetadataSync(t, meta)
  head, err := meta.RecordSnapshotSeqHead(ctx, util.DummySnapshotSequence(vol_uuid, "seq2"))
  if err != nil { t.Errorf("RecordSnapshotSeqHead: %v", err) }
  expect_state.Heads[0] = head
  _, err = meta.PersistCurrentMetadataState(ctx)
  if err != nil { t.Errorf("PersistCurrentMetadataState: %v", err) }

  mem_only.CompareStates(t, "Bad state", meta.State, expect_state)
  client := SimpleDirRw{ meta.DirInfo }
  persisted_state := client.GetState()
  mem_only.CompareStates(t, "Bad persisted state", persisted_state, expect_state)
}

