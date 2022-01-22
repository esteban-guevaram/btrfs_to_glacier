package local_fs_metadata

import (
  "context"
  "fmt"
  fpmod "path/filepath"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "google.golang.org/protobuf/proto"
)

type SimpleDirMetadataAdmin struct {
  *SimpleDirMetadata
}

func NewMetadataAdmin(
    ctx context.Context, conf *pb.Config, part_uuid string) (types.AdminMetadata, error) {
  metadata, err := NewMetadata(ctx, conf, part_uuid)
  if err != nil { return nil, err }

  admin := &SimpleDirMetadataAdmin{ SimpleDirMetadata: metadata.(*SimpleDirMetadata), }
  return admin, nil
}

// Do not create anything, just check
func (self *SimpleDirMetadataAdmin) SetupMetadata(ctx context.Context) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    p := self.DirInfo
    if !util.IsDir(MetaDir(p)) {
      done <- fmt.Errorf("'%s' is not a directory", MetaDir(p))
      return
    }
    if !util.Exists(SymLink(p)) { done <- nil ; return }
    if !util.IsSymLink(SymLink(p)) {
      done <- fmt.Errorf("'%s' is not a symlink", SymLink(p))
      return
    }
    target,err := fpmod.EvalSymlinks(SymLink(p))
    if err != nil { done <- err; return }
    if !fpmod.HasPrefix(target, MetaDir(p)) {
      done <- fmt.Errorf("'%s' points outside of '%s'", SymLink(p), MetaDir(p))
      return
    }
    done <- nil
  }()
  return done
}

func TestOnlySetInnerState(metadata types.Metadata, state *pb.AllMetadata) {
  if metadata == nil { util.Fatalf("metadata == nil") }
  impl,ok := metadata.(*SimpleDirMetadataAdmin)
  if !ok { util.Fatalf("called with the wrong impl") }
  impl.State = proto.Clone(state).(*pb.AllMetadata)
}

