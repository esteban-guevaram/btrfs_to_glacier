package local_fs_metadata

import (
  "context"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"

  "btrfs_to_glacier/util"

  "github.com/google/uuid"
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

