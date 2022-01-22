package local_fs_metadata

import (
  "context"
  "errors"
  "fmt"
  "io/fs"
  fpmod "path/filepath"
  "os"
  "sort"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
  store "btrfs_to_glacier/volume_store"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

const (
  KeepLast = 3
)

type SimpleDirMetadata struct {
  Conf       *pb.Config
  DirInfo    *pb.LocalFs_Partition
  SymLink    string
  State      *pb.AllMetadata
  KeepLast   int
}

func MetaDir(dir_info *pb.LocalFs_Partition) string {
  return fpmod.Join(dir_info.MountRoot, dir_info.MetadataDir)
}
func SymLink(dir_info *pb.LocalFs_Partition) string {
  return fpmod.Join(dir_info.MountRoot, dir_info.MetadataDir, "metadata.pb.gz")
}

func NewMetadata(ctx context.Context, conf *pb.Config, part_uuid string) (types.Metadata, error) {
  var part *pb.LocalFs_Partition
  for _,p := range conf.LocalFs.Partitions {
    if p.PartitionUuid != part_uuid { continue }
    part = p
  }
  if part == nil { return nil, fmt.Errorf("Partition '%s' not found", part_uuid) }

  metadata := &SimpleDirMetadata{
    Conf: conf,
    DirInfo: part,
    SymLink: SymLink(part),
    KeepLast: KeepLast,
  }
  err := metadata.LoadPreviousStateFromDir(ctx)
  return metadata, err
}

func (self *SimpleDirMetadata) LoadPreviousStateFromDir(ctx context.Context) error {
  if self.State != nil { util.Fatalf("Cannot load state twice") }
  self.State = &pb.AllMetadata{
    CreatedTs: uint64(time.Now().Unix()),
  }
  err := util.UnmarshalGzProto(self.SymLink, self.State)
  if err != nil && !errors.Is(err, os.ErrNotExist) { return err }
  self.State.Uuid = self.SymLink
  return nil
}

func (self *SimpleDirMetadata) MetaVer(ts uint64) string {
  name := fmt.Sprintf("metadata_%d.pb.gz", ts)
  return fpmod.Join(self.DirInfo.MountRoot, self.DirInfo.MetadataDir, name)
}

func (self *SimpleDirMetadata) CleanOldVersions(ctx context.Context) ([]string, error) {
  var cleaned_files []string
  var filter_entries []fs.DirEntry

  meta_dir := MetaDir(self.DirInfo)
  fs_dir := os.DirFS(meta_dir)
  entries, err := fs.ReadDir(fs_dir, ".")
  if err != nil { return cleaned_files, err }

  for _,e := range entries {
    info, err := e.Info()
    if err != nil { return cleaned_files, err }
    if e.IsDir() || info.Mode() & fs.ModeSymlink != 0 { continue }
    filter_entries = append(filter_entries, e)
  }
  sort.Slice(filter_entries, func(i,j int) bool {
    info_i, err := filter_entries[i].Info()
    if err != nil { util.Fatalf("failed to sort entries: %v", err) }
    info_j, err := filter_entries[j].Info()
    if err != nil { util.Fatalf("failed to sort entries: %v", err) }
    return info_j.ModTime().Before(info_i.ModTime())
  })

  if len(filter_entries) <= self.KeepLast { return cleaned_files, nil }
  for _,e := range filter_entries[self.KeepLast:] {
    path := fpmod.Join(meta_dir, e.Name())
    err := os.Remove(path)
    if err != nil { return cleaned_files, err }
    cleaned_files = append(cleaned_files, path)
  }
  return cleaned_files, nil
}

func (self *SimpleDirMetadata) SaveCurrentStateToDir(ctx context.Context) (string, error) {
  if self.State == nil { util.Fatalf("Cannot store nil state") }
  self.State.CreatedTs = uint64(time.Now().Unix())
  version_id := fmt.Sprintf("%d_%s", self.State.CreatedTs, uuid.NewString())
  prev_path,_ := fpmod.EvalSymlinks(self.SymLink)
  store_path := self.MetaVer(self.State.CreatedTs)

  err := util.MarshalGzProto(store_path, self.State)
  if err != nil { return "", err }

  if util.Exists(self.SymLink) {
    if err := os.Remove(self.SymLink); err != nil { return "", err }
  }
  err = os.Symlink(store_path, self.SymLink)
  if err != nil {
    // Attempt to add back the old path
    if len(prev_path) > 0 { os.Symlink(prev_path, self.SymLink) }
    return "", err
  }

  util.Infof("Saved metadata version: '%s'", version_id)
  _, err = self.CleanOldVersions(ctx)
  return version_id, err
}

func (self *SimpleDirMetadata) findHead(uuid string) (int,*pb.SnapshotSeqHead) {
  if self.State == nil { util.Fatalf("state not loaded") }
  for idx,head := range self.State.Heads {
    if head.Uuid == uuid { return idx,head }
  }
  return 0, nil
}
func (self *SimpleDirMetadata) findOrAppendHead(uuid string) *pb.SnapshotSeqHead {
  if _,head := self.findHead(uuid); head != nil { return head }
  head := &pb.SnapshotSeqHead{ Uuid: uuid, }
  self.State.Heads = append(self.State.Heads, head)
  return head
}

func (self *SimpleDirMetadata) findSeq(uuid string) *pb.SnapshotSequence {
  if self.State == nil { util.Fatalf("state not loaded") }
  for _,meta_seq := range self.State.Sequences {
    if meta_seq.Uuid == uuid { return meta_seq }
  }
  return nil
}
func (self *SimpleDirMetadata) findOrCloneSeq(seq *pb.SnapshotSequence) *pb.SnapshotSequence {
  if meta_seq := self.findSeq(seq.Uuid); meta_seq != nil { return meta_seq }
  meta_seq := proto.Clone(seq).(*pb.SnapshotSequence)
  self.State.Sequences = append(self.State.Sequences, meta_seq)
  return meta_seq
}

func (self *SimpleDirMetadata) findSnap(uuid string) *pb.SubVolume {
  if self.State == nil { util.Fatalf("state not loaded") }
  for _,meta_snap := range self.State.Snapshots {
    if meta_snap.Uuid == uuid { return meta_snap }
  }
  return nil
}
func (self *SimpleDirMetadata) findOrCloneSnap(snap *pb.SubVolume) *pb.SubVolume {
  if meta_snap := self.findSnap(snap.Uuid); meta_snap != nil { return meta_snap }
  meta_snap := proto.Clone(snap).(*pb.SubVolume)
  self.State.Snapshots = append(self.State.Snapshots, meta_snap)
  return meta_snap
}

func (self *SimpleDirMetadata) RecordSnapshotSeqHead(
    ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error) {
  err := store.ValidateSnapshotSequence(new_seq)
  if err != nil { return nil, err }

  uuid := new_seq.Volume.Uuid
  head := self.findOrAppendHead(uuid)

  if head.CurSeqUuid == new_seq.Uuid {
    util.PbInfof("Noop already current seq in head: %v", head)
    return proto.Clone(head).(*pb.SnapshotSeqHead), nil
  }

  if len(head.CurSeqUuid) > 0 { head.PrevSeqUuid = append(head.PrevSeqUuid, head.CurSeqUuid) }
  head.CurSeqUuid = new_seq.Uuid

  err = store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  util.PbInfof("Wrote head: %v", head)
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *SimpleDirMetadata) AppendSnapshotToSeq(
    ctx context.Context, seq *pb.SnapshotSequence, snap *pb.SubVolume) (*pb.SnapshotSequence, error) {
  err := store.ValidateSubVolume(store.CheckSnapNoContent, snap)
  if err != nil { return nil, err }

  new_seq := self.findOrCloneSeq(seq)
  if len(seq.SnapUuids) > 0 {
    last := seq.SnapUuids[len(seq.SnapUuids) - 1]
    if last == snap.Uuid {
      util.PbInfof("Noop already last snap in seq: %v", seq)
      return proto.Clone(new_seq).(*pb.SnapshotSequence), nil
    }
  }

  new_seq.SnapUuids = append(new_seq.SnapUuids, snap.Uuid)

  err = store.ValidateSnapshotSequence(new_seq)
  if err != nil { return nil, err }
  if new_seq.Volume.Uuid != snap.ParentUuid {
    return nil, util.PbErrorf("Sequence volume and snap parent do not match: %v, %v", new_seq, snap)
  }
  if new_seq.Volume.CreatedTs >= snap.CreatedTs {
    return nil, util.PbErrorf("Sequence volume created after snap: %v, %v", new_seq, snap)
  }

  util.PbInfof("Wrote sequence: %v", new_seq)
  return proto.Clone(new_seq).(*pb.SnapshotSequence), nil
}

func (self *SimpleDirMetadata) AppendChunkToSnapshot(
    ctx context.Context, snap *pb.SubVolume, data *pb.SnapshotChunks) (*pb.SubVolume, error) {
  err := store.ValidateSnapshotChunks(store.CheckChunkNotFirst, data)
  if err != nil { return nil, err }

  new_snap := self.findOrCloneSnap(snap)
  if new_snap.Data != nil && new_snap.Data.KeyFingerprint != data.KeyFingerprint {
    return nil, util.PbErrorf("Snapshot chunk key mismatch: %v, %v", new_snap, data)
  }
  if store.IsFullyContainedInSubvolume(new_snap, data) {
    util.PbInfof("Noop already last chunk in snap: %v", new_snap)
    return proto.Clone(new_snap).(*pb.SubVolume), nil
  }

  data_len := store.SubVolumeDataLen(snap)
  if data.Chunks[0].Start != data_len {
    return nil, util.PbErrorf("Snapshot chunk not contiguous: %v, %v", snap, data)
  }

  if new_snap.Data == nil {
    new_snap.Data = &pb.SnapshotChunks { KeyFingerprint: data.KeyFingerprint }
  }
  new_snap.Data.Chunks = append(new_snap.Data.Chunks, data.Chunks...)

  err = store.ValidateSubVolume(store.CheckSnapWithContent, new_snap)
  if err != nil { return nil, err }

  util.PbInfof("Wrote snapshot: %v", new_snap)
  return proto.Clone(new_snap).(*pb.SubVolume), nil
}

func (self *SimpleDirMetadata) ReadSnapshotSeqHead(
    ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshotSeqHead: uuid is nil") }

  _,head := self.findHead(uuid)
  if head == nil { return nil, types.ErrNotFound }

  err := store.ValidateSnapshotSeqHead(head)
  if err != nil { return nil, err }

  util.PbInfof("Read head: %v", head)
  return proto.Clone(head).(*pb.SnapshotSeqHead), nil
}

func (self *SimpleDirMetadata) ReadSnapshotSeq(
    ctx context.Context, uuid string) (*pb.SnapshotSequence, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshotSeq: uuid is nil") }

  seq := self.findSeq(uuid)
  if seq == nil { return nil, types.ErrNotFound }

  err := store.ValidateSnapshotSequence(seq)
  if err != nil { return nil, err }

  util.PbInfof("Read sequence: %v", seq)
  return proto.Clone(seq).(*pb.SnapshotSequence), nil
}

func (self *SimpleDirMetadata) ReadSnapshot(
    ctx context.Context, uuid string) (*pb.SubVolume, error) {
  if len(uuid) < 1 { return nil, fmt.Errorf("ReadSnapshot: uuid is nil") }

  snap := self.findSnap(uuid)
  if snap == nil { return nil, types.ErrNotFound }

  err := store.ValidateSubVolume(store.CheckSnapWithContent, snap)
  if err != nil { return nil, err }

  util.PbInfof("Read subvolume: %v", snap)
  return proto.Clone(snap).(*pb.SubVolume), nil
}

type SnapshotSeqHeadIterator struct { List []*pb.SnapshotSeqHead; Idx int }
func (self *SnapshotSeqHeadIterator) Next(ctx context.Context, o *pb.SnapshotSeqHead) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotSeqHeadIterator) Err() error { return nil }

type SnapshotSequenceIterator struct { List []*pb.SnapshotSequence; Idx int }
func (self *SnapshotSequenceIterator) Next(ctx context.Context, o *pb.SnapshotSequence) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotSequenceIterator) Err() error { return nil }

type SnapshotIterator struct { List []*pb.SubVolume; Idx int }
func (self *SnapshotIterator) Next(ctx context.Context, o *pb.SubVolume) bool {
  if self.Idx < len(self.List) {
    *o = *self.List[self.Idx]
    self.Idx += 1
    return true
  }
  return false
}
func (self *SnapshotIterator) Err() error { return nil }

func (self *SimpleDirMetadata) ListAllSnapshotSeqHeads(
    ctx context.Context) (types.SnapshotSeqHeadIterator, error) {
  return &SnapshotSeqHeadIterator{ List: self.State.Heads, }, nil
}

func (self *SimpleDirMetadata) ListAllSnapshotSeqs(
    ctx context.Context) (types.SnapshotSequenceIterator, error) {
  return &SnapshotSequenceIterator{ List: self.State.Sequences, }, nil
}

func (self *SimpleDirMetadata) ListAllSnapshots(
    ctx context.Context) (types.SnapshotIterator, error) {
  return &SnapshotIterator{ List: self.State.Snapshots, }, nil
}

func (self *SimpleDirMetadata) PersistCurrentMetadataState(ctx context.Context) (string, error) {
  return self.SaveCurrentStateToDir(ctx)
}

