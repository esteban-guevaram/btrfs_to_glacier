package local_fs_metadata

import (
  "context"
  "fmt"
  fpmod "path/filepath"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"

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

func (self *SimpleDirMetadataAdmin) DeleteMetadataUuids(
    ctx context.Context, seq_uuids []string, snap_uuids []string) (<-chan error) {
  seq_set := make(map[string]bool)
  for _,uuid := range seq_uuids { seq_set[uuid] = true }
  snap_set := make(map[string]bool)
  for _,uuid := range snap_uuids { snap_set[uuid] = true }

  new_seqs := make([]*pb.SnapshotSequence, 0, len(self.State.Sequences))
  new_snaps := make([]*pb.SubVolume, 0, len(self.State.Snapshots))

  for _,seq := range self.State.Sequences {
    if seq_set[seq.Uuid] { continue }
    new_seqs = append(new_seqs, seq)
  }
  for _,snap := range self.State.Snapshots {
    if snap_set[snap.Uuid] { continue }
    new_snaps = append(new_snaps, snap)
  }

  self.State.Sequences = new_seqs
  self.State.Snapshots = new_snaps
  return util.WrapInChan(nil)
}

func (self *SimpleDirMetadataAdmin) ReplaceSnapshotSeqHead(
    ctx context.Context, head *pb.SnapshotSeqHead) (*pb.SnapshotSeqHead, error) {
  err := store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  idx, prev_head := self.findHead(head.Uuid)
  if prev_head == nil { return nil, fmt.Errorf("%w uuid=%v", types.ErrNotFound, head.Uuid) }

  self.State.Heads[idx] = proto.Clone(head).(*pb.SnapshotSeqHead)
  return prev_head, nil
}

func TestOnlySetInnerState(metadata types.Metadata, state *pb.AllMetadata) {
  if metadata == nil { util.Fatalf("metadata == nil") }
  impl,ok := metadata.(*SimpleDirMetadataAdmin)
  if !ok { util.Fatalf("called with the wrong impl") }
  impl.State = proto.Clone(state).(*pb.AllMetadata)
}

