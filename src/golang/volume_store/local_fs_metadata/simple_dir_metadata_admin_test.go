package local_fs_metadata

import (
  "context"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"

  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

func buildTestAdminMetadata(t *testing.T) (*SimpleDirMetadataAdmin, *SimpleDirRw, func()) {
  local_fs, clean_f := util.TestSimpleDirLocalFs()
  conf := util.LoadTestConfWithLocalFs(local_fs)
  metadata,client := buildTestMetadataWithConf(t, conf)
  admin := &SimpleDirMetadataAdmin{ SimpleDirMetadata:metadata, }
  return admin, client, clean_f
}

func buildTestAdminMetadataWithState(
    t *testing.T, state *pb.AllMetadata) (*SimpleDirMetadataAdmin, *SimpleDirRw, func()) {
  metadata,client,clean_f := buildTestMetadataWithState(t, state)
  admin := &SimpleDirMetadataAdmin{ SimpleDirMetadata:metadata, }
  return admin, client, clean_f
}

func TestSetupMetadata(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,_,clean_f := buildTestAdminMetadata(t)
  defer clean_f()
  done := meta_admin.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestSetupmeta_admin timeout")
  }
}

func TestSetupMetadata_Fail(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,_,clean_f := buildTestAdminMetadata(t)
  defer clean_f()
  meta_admin.DirInfo.MetadataDir = uuid.NewString() // this dir should not exist
  done := meta_admin.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err == nil { t.Errorf("Expected error in SetupMetadata") }
    case <-ctx.Done():
      t.Fatalf("TestSetupmeta_admin timeout")
  }
}

func TestSetupMetadata_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,_,clean_f := buildTestAdminMetadata(t)
  defer clean_f()
  for i:=0; i<2; i+=1 {
    done := meta_admin.SetupMetadata(ctx)
    select {
      case err := <-done:
        if err != nil { t.Errorf("Returned error: %v", err) }
      case <-ctx.Done():
        t.Fatalf("TestSetupMetadata_Idempotent timeout")
    }
  }
}

func TestDeleteMetadataUuids(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  ini_state := proto.Clone(expect_state).(*pb.AllMetadata)
  ini_state.Sequences = append(ini_state.Sequences,
                                util.DummySnapshotSequence(vol_uuid, uuid.NewString()))
  ini_state.Snapshots = append(ini_state.Snapshots,
                                 util.DummySnapshot(uuid.NewString(), vol_uuid))
  meta_admin,_,clean_f := buildTestAdminMetadataWithState(t, ini_state)
  defer clean_f()

  done := meta_admin.DeleteMetadataUuids(ctx,
                                         []string{ini_state.Sequences[1].Uuid},
                                         []string{ini_state.Snapshots[1].Uuid})

  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      util.EqualsOrFailTest(t, "Bad state", meta_admin.State, expect_state)
    case <-ctx.Done():
      t.Fatalf("TestDeleteMetadataUuids timeout")
  }
}

func TestDeleteMetadataUuids_Empty(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin,_,clean_f := buildTestAdminMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()

  done := meta_admin.DeleteMetadataUuids(ctx,
                                         []string{"not_exists_seq"},
                                         []string{"not_exists_snap"})

  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      util.EqualsOrFailTest(t, "Bad state", meta_admin.State, &pb.AllMetadata{})
    case <-ctx.Done():
      t.Fatalf("TestDeleteMetadataUuids_Empty timeout")
  }
}

func TestDeleteMetadataUuids_UuidNotFound(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  ini_state := proto.Clone(expect_state).(*pb.AllMetadata)
  meta_admin,_,clean_f := buildTestAdminMetadataWithState(t, ini_state)
  defer clean_f()

  done := meta_admin.DeleteMetadataUuids(ctx,
                                         []string{"not_exists_seq"},
                                         []string{"not_exists_snap"})

  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
      util.EqualsOrFailTest(t, "Bad state", meta_admin.State, expect_state)
    case <-ctx.Done():
      t.Fatalf("TestDeleteMetadataUuids_UuidNotFound timeout")
  }
}

func TestReplaceSnapshotSeqHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, expect_state := util.DummyAllMetadata()
  ini_state := proto.Clone(expect_state).(*pb.AllMetadata)
  meta_admin,_,clean_f := buildTestAdminMetadataWithState(t, ini_state)
  defer clean_f()

  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence(vol_uuid, "seq_new"))
  old_head := proto.Clone(expect_state.Heads[0]).(*pb.SnapshotSeqHead)
  expect_state.Heads[0] = proto.Clone(new_head).(*pb.SnapshotSeqHead)


  got_old_head, err := meta_admin.ReplaceSnapshotSeqHead(ctx, new_head)
  if err != nil { t.Errorf("Returned error: %v", err) }

  compareStates(t, "bad head state", meta_admin.State, expect_state)
  util.EqualsOrFailTest(t, "OldSnapshotSeqHead", got_old_head, old_head)
}

func TestReplaceSnapshotSeqHead_NoOldHead(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  meta_admin, _,clean_f := buildTestAdminMetadataWithState(t, &pb.AllMetadata{})
  defer clean_f()
  new_head := util.DummySnapshotSeqHead(util.DummySnapshotSequence("vol", "seq_new"))
  _, err := meta_admin.ReplaceSnapshotSeqHead(ctx, new_head)
  if err == nil { t.Errorf("expected error.") }
}

