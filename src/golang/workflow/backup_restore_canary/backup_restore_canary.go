package backup_restore_canary

import (
  "context"
  "errors"
  "fmt"
  "os"
  fpmod "path/filepath"
  "strings"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

const (
  LoopDevSizeMb = 32
  FakeSource = "FakeSource"
  FakeDestination = "FakeDestination"
)

var ErrCannotCallTwice = errors.New("validate_cannot_be_called_twice")
var ErrMustRestoreBefore = errors.New("before_add_snap_need_restore")
var ErrCannotCallOnEmptyChain = errors.New("cannot_call_this_method_on_empty_restore_chain")
var ErrValidateUuidFile = errors.New("validate_error_uuid_file")
var ErrValidateNewDir = errors.New("validate_error_new_dir")
var ErrValidateDelDir = errors.New("validate_error_del_dir")

// Used to lazy initialize specific object to the fake configs created for the fake filesystem loop device.
type MgrFactoryIf interface {
  NewBackupManager(ctx context.Context,
    conf *pb.Config) (types.BackupManagerAdmin, error)
  NewRestoreManager(ctx context.Context,
    conf *pb.Config, dst_name string) (types.RestoreManager, error)
}

// Fields that may change value during the execution.
type State struct {
  FakeConf            *pb.Config
  Fs                  *types.Filesystem
  New                 bool
  Uuid                string
  VolRoot             string
  SnapRoot            string
  RestoreRoot         string
  TopDstRestoredPath  string
  TopSrcRestoredSnap  *pb.SubVolume
  RestoredSrcSnaps    []*pb.SubVolume // does not contain TopSrcRestoredSnap
  BackupMgr           types.BackupManagerAdmin
  RestoreMgr          types.RestoreManager
}

// Note: this type cannot be abstracted away from btrfs.
// It needs to perform some operations that are not available in types.VolumeManager.
type BackupRestoreCanary struct {
  Conf     *pb.Config
  Btrfs    types.Btrfsutil
  Lnxutil  types.Linuxutil
  Factory  MgrFactoryIf
  State    *State
}

func NewBackupRestoreCanary(
    conf *pb.Config, btrfs types.Btrfsutil, lnxutil types.Linuxutil, factory MgrFactoryIf) (types.BackupRestoreCanary, error) {
  canary := &BackupRestoreCanary{
    Conf: conf,
    Btrfs: btrfs,
    Lnxutil: lnxutil,
    Factory: factory,
    State: nil,
  }
  return canary, nil
}

// Creates empty btrfs filesystem on loop device.
// Prepares State to point to the newly created filesystem.
// Creates a new volume if this is the first time the canary is run.
func (self *BackupRestoreCanary) Setup(ctx context.Context) error {
  if self.State != nil {
    util.Infof("Setup twice is a noop: %s", self.State.Uuid)
    return nil
  }
  dev, err := self.Lnxutil.CreateLoopDevice(ctx, LoopDevSizeMb)
  if err != nil { return err }

  // Set the state to indicate there is something to tear down.
  self.State = &State{
    Fs: &types.Filesystem{ Devices: []*types.Device{dev,}, },
  }

  fs, err := self.Lnxutil.CreateBtrfsFilesystem(ctx, dev, uuid.NewString(), "--mixed")
  if err != nil { return err }
  self.State.Fs = fs

  target_mnt, err := os.MkdirTemp("", "canary-root-fs-mount-")
  if err != nil { return err }
  mnt, err := self.Lnxutil.Mount(ctx, fs.Uuid, target_mnt)
  if err != nil { return err }

  //util.Debugf("Mounted filesystem '%s' root subvol (id:%d) in '%s'",
  //            fs.Uuid, mnt.BtrfsVolId, target_mnt)
  fs.Mounts = append(fs.Mounts, mnt)

  err = self.PrepareState(ctx)
  return err
}

func (self *BackupRestoreCanary) PrepareState(ctx context.Context) error {
  err := self.SetupPathsInNewFs()
  if err != nil { return err }

  self.State.FakeConf = self.BuildFakeConf()

  self.State.BackupMgr, err = self.Factory.NewBackupManager(ctx, self.State.FakeConf)
  if err != nil { return err }

  self.State.RestoreMgr, err = self.Factory.NewRestoreManager(ctx, self.State.FakeConf, FakeDestination)
  if err != nil { return err }

  self.State.Uuid, err = self.DetermineVolUuid(ctx)
  if err != nil { return err }

  if len(self.State.Uuid) < 1 {
    drop_f := self.Lnxutil.GetRootOrDie()
    defer drop_f()
    self.State.New = true
    err = self.Btrfs.CreateSubvolume(self.State.VolRoot)
    if err != nil { return err }
    sv, err := self.Btrfs.SubVolumeInfo(self.State.VolRoot)
    if err != nil { return err }
    err = self.CreateFirstValidationChainItem()
    if err != nil { return err }
    _, err = self.State.BackupMgr.BackupAllToCurrentSequences(ctx, []*pb.SubVolume{sv,})
    if err != nil { return err }
    self.State.Uuid, err = self.DetermineVolUuid(ctx)
    if err != nil { return err }
  }
  return nil
}

func (self *BackupRestoreCanary) DetermineVolUuid(ctx context.Context) (string, error) {
  heads, err := self.State.RestoreMgr.ReadHeadAndSequenceMap(ctx)
  if err != nil { return "", err }
  if len(heads) > 1 {
    return "", fmt.Errorf("Metadata contains more than 1 volume: %v", len(heads))
  }
  for k,_ := range heads {
    return k, nil
  }
  return "", nil
}

func (self *BackupRestoreCanary) BuildFakeConf() *pb.Config {
  fake_conf := proto.Clone(self.Conf).(*pb.Config)
  fake_conf.Workflows = nil
  fake_conf.Tools = nil
  fake_conf.Backups = nil

  volsnap := &pb.Source_VolSnapPathPair{
    VolPath: self.State.VolRoot,
    SnapPath: self.State.SnapRoot,
  }
  source := &pb.Source{
    Type: pb.Source_BTRFS,
    Name: FakeSource,
    Paths: []*pb.Source_VolSnapPathPair{volsnap,},
  }
  fake_conf.Sources = []*pb.Source{source,}
  restore := &pb.Restore{
    Type: pb.Restore_BTRFS,
    Name: FakeDestination,
    RootRestorePath: self.State.RestoreRoot,
  }
  fake_conf.Restores = []*pb.Restore{restore,}
  return fake_conf
}

// Structure of filesystem to validate:
// * restores/       # restored snapshots will go here
// * subvol/         # writable clone of most recent snapshot, used to continue the snap sequence to validate.
//                   # for first snapshot this will simply be a brand new subvolume
// * snapshots/      # new snapshots in the sequence go here
// * subvol/uuids    # contains all snapshot uuids in history, one per line
//                   # empty for the first snapshot
// * subvol/deleted/ # contains a single file named after the most recently deleted snapshot
//                   # content is hash(prev_hash, backup_sv.Uuid, backup_sv.Data.Chunks.Uuid)
//                   # no files for the first snapshot
// * subvol/new/     # contains one file per snapshot, named as its uuid
//                   # content is hash(backup_sv.Uuid, backup_sv.Data.Chunks.Uuid)
//                   # no files for the first snapshot
func (self *BackupRestoreCanary) SetupPathsInNewFs() error {
  root_vol := self.State.Fs.Mounts[0].MountedPath
  self.State.VolRoot = fpmod.Join(root_vol, "subvol")
  if util.Exists(self.State.VolRoot) { return fmt.Errorf("Filesystem is not new: %s", root_vol) }

  self.State.RestoreRoot = fpmod.Join(root_vol, "restores")
  if err := os.Mkdir(self.State.RestoreRoot, 0775); err != nil { return err }

  self.State.SnapRoot = fpmod.Join(root_vol, "snapshots")
  if err := os.Mkdir(self.State.SnapRoot, 0775); err != nil { return err }
  return nil
}

func (self *BackupRestoreCanary) DelDir() string {
  return fpmod.Join(self.State.VolRoot, types.KCanaryDelDir)
}
func (self *BackupRestoreCanary) NewDir() string {
  return fpmod.Join(self.State.VolRoot, types.KCanaryNewDir)
}
func (self *BackupRestoreCanary) UuidFile() string {
  return fpmod.Join(self.State.VolRoot, types.KCanaryUuidFile)
}
func (self *BackupRestoreCanary) RestoredDelDir() string {
  return fpmod.Join(self.State.TopDstRestoredPath, types.KCanaryDelDir)
}
func (self *BackupRestoreCanary) RestoredNewDir() string {
  return fpmod.Join(self.State.TopDstRestoredPath, types.KCanaryNewDir)
}
func (self *BackupRestoreCanary) RestoredUuidFile() string {
  return fpmod.Join(self.State.TopDstRestoredPath, types.KCanaryUuidFile)
}

func (self *BackupRestoreCanary) CreateFirstValidationChainItem() error {
  if err := os.Mkdir(self.DelDir(), 0775); err != nil { return err }
  if err := os.Mkdir(self.NewDir(), 0775); err != nil { return err }
  f, err := os.Create(self.UuidFile())
  return util.Coalesce(err, f.Close())
}

// Destroys the loop device and backing file.
// In case of a partial `Setup()`, attempts to delete any dangling infrastructure.
func (self *BackupRestoreCanary) TearDown(ctx context.Context) error {
  if self.State == nil {
    util.Infof("Teardown before calling setup is a noop")
    return nil
  }
  var umount_err, deldev_err error
  if len(self.State.Fs.Mounts) > 0 {
    umount_err = self.Lnxutil.UMount(ctx, self.State.Fs.Uuid)
  }
  deldev_err = self.Lnxutil.DeleteLoopDevice(ctx, self.State.Fs.Devices[0])
  return util.Coalesce(umount_err, deldev_err)
}

// btrfs subvolume snap restores/asubvol.snap.2 clones/asubvol.clone.2
// # ... add a couple new files ...
// btrfs subvolume snap -r clones/asubvol.clone.2 restores/asubvol.snap.3
// btrfs send -p restores/asubvol.snap.2 restores/asubvol.snap.3 | btrfs receive restore_dir2
// comm -3 <(find restore_dir2/asubvol.snap.3 -printf "./%P\n") <(find clones/asubvol.clone.2 -printf "./%P\n")
// # both subvolumes contain the same files
func (self *BackupRestoreCanary) AppendSnapshotToValidationChain(
    ctx context.Context) (types.BackupPair, error) {
  result := types.BackupPair{}
  if self.State.TopSrcRestoredSnap == nil { return result, ErrMustRestoreBefore }
  drop_f := self.Lnxutil.GetRootOrDie()
  defer drop_f()
  if self.State.New {
    err := self.AppendDataToSubVolume()
    if err != nil { return result, err }
    sv, err := self.Btrfs.SubVolumeInfo(self.State.VolRoot)
    if err != nil { return result, err }
    bkp_pair, err := self.State.BackupMgr.BackupAllToCurrentSequences(ctx, []*pb.SubVolume{sv,})
    if err != nil { return result, err }
    if len(bkp_pair) != 1 {
      return bkp_pair[0], fmt.Errorf("canary should use just 1 subvolume")
    }
    return bkp_pair[0], err
  }

  // Create the clone only once in case of multiple appends
  clone, err := self.Btrfs.SubVolumeInfo(self.State.VolRoot)
  if err != nil {
    err := self.Btrfs.CreateClone(self.State.TopSrcRestoredSnap.MountedPath, self.State.VolRoot)
    if err != nil { return result, err }
    result.Sv, err = self.Btrfs.SubVolumeInfo(self.State.VolRoot)
    if err != nil { return result, err }
  }
  /*else*/ if err == nil { result.Sv = clone }

  err = self.AppendDataToSubVolume()
  if err != nil { return result, err }

  result.Snap, err = self.State.BackupMgr.BackupToCurrentSequenceUnrelatedVol(ctx, result.Sv, self.State.Uuid)
  return result, err
}

func ReadFileIntoString(dir string, file string) (string, error) {
  path := fpmod.Join(dir, file)
  content, err := os.ReadFile(path)
  if err != nil { return "", err }
  return string(content), nil
}

// Prerequisite the subvolume must have been created before
// and should contain the file and directories listed in `SetupPathsInNewFs`.
func (self *BackupRestoreCanary) AppendDataToSubVolume() error {
  top_snap := self.State.TopSrcRestoredSnap
  f, err := os.OpenFile(self.UuidFile(), os.O_WRONLY|os.O_APPEND, 0666)
  if err != nil { return err }
  _, err_w1 := f.WriteString(fmt.Sprintln(top_snap.Uuid))
  err_cl := f.Close()
  if err = util.Coalesce(err_w1, err_cl); err != nil { return err }

  newpath := fpmod.Join(self.NewDir(), top_snap.Uuid)
  f, err = os.Create(newpath)
  if err != nil { return err }
  _, err_w1 = f.WriteString(util.HashFromSv(top_snap, ""))
  err_cl = f.Close()
  if err = util.Coalesce(err_w1, err_cl); err != nil { return err }

  var prev_content string
  entries, err := os.ReadDir(self.DelDir())
  if err != nil { return err }
  for _,e := range entries {
    if prev_content,err = ReadFileIntoString(self.DelDir(), e.Name()); err != nil { return nil }
    path := fpmod.Join(self.DelDir(), e.Name())
    if err = util.RemoveAll(path); err != nil { return err }
  }
  delpath := fpmod.Join(self.DelDir(), top_snap.Uuid)
  f, err = os.Create(delpath)
  if err != nil { return err }
  _, err_w1 = f.WriteString(util.HashFromSv(top_snap, prev_content))
  err_cl = f.Close()
  return util.Coalesce(err_w1, err_cl)
}

func (self *BackupRestoreCanary) ValidateEmptyChain() error {
  content, err := os.ReadFile(self.RestoredUuidFile())
  if err != nil { return err }
  if len(content) > 0 {
    return fmt.Errorf("%w: %s should be empty", ErrValidateUuidFile, self.RestoredUuidFile())
  }

  entries, err := os.ReadDir(self.RestoredDelDir())
  if err != nil { return err }
  if len(entries) > 0 {
    return fmt.Errorf("%w: %s should be empty", ErrValidateDelDir, self.RestoredDelDir())
  }

  entries, err = os.ReadDir(self.RestoredNewDir())
  if err != nil { return err }
  if len(entries) > 0 {
    return fmt.Errorf("%w: %s should be empty", ErrValidateNewDir, self.RestoredNewDir())
  }
  return nil
}

func (self *BackupRestoreCanary) ValidateUuidFile() error {
  len_snaps_in_top := len(self.State.RestoredSrcSnaps)
  if len_snaps_in_top == 0 { return ErrCannotCallOnEmptyChain }

  content, err := os.ReadFile(self.RestoredUuidFile())
  lines := strings.Split(strings.TrimSpace(string(content)), "\n")
  if err != nil { return err }

  if len(lines) != len_snaps_in_top {
    return fmt.Errorf("%w: Volume does not contain a list of all of its ancestors: %d / %d",
                      ErrValidateUuidFile, len(lines), len_snaps_in_top)
  }
  for i,l := range lines {
    if l != self.State.RestoredSrcSnaps[i].Uuid {
      return fmt.Errorf("%w: Snapshot history mismatch: %s / %s",
                        ErrValidateUuidFile, l, self.State.RestoredSrcSnaps[i].Uuid)
    }
  }
  return nil
}

func (self *BackupRestoreCanary) ValidateDelDir() error {
  if len(self.State.RestoredSrcSnaps) == 0 { return ErrCannotCallOnEmptyChain }

  entries, err := os.ReadDir(self.RestoredDelDir())
  if err != nil { return err }
  if len(entries) != 1 {
    return fmt.Errorf("%w: should contain only 1 file, got: %d",
                      ErrValidateDelDir, len(entries))
  }

  expect_delname := self.State.TopSrcRestoredSnap.Uuid
  if expect_delname != entries[0].Name() {
    return fmt.Errorf("%w: should contain a file named after State.RestoredSrcSnaps[-1]: '%s'",
                      ErrValidateDelDir, entries[0].Name())
  }

  got_hash, err := ReadFileIntoString(self.RestoredDelDir(), entries[0].Name())
  if err != nil { return err }

  prev_hash, expect_hash := "", ""
  for _,sv := range self.State.RestoredSrcSnaps {
    expect_hash = util.HashFromSv(sv, prev_hash)
    prev_hash = expect_hash
  }
  if strings.Compare(got_hash, expect_hash) != 0 {
    return fmt.Errorf("%w: file, bad content: %x != %x", ErrValidateDelDir, got_hash, expect_hash)
  }
  return nil
}

func (self *BackupRestoreCanary) ValidateNewDir() error {
  len_snaps_in_top := len(self.State.RestoredSrcSnaps)
  if len_snaps_in_top == 0 { return ErrCannotCallOnEmptyChain }

  sv_to_hash := make(map[string]string)
  for _,sv := range self.State.RestoredSrcSnaps { sv_to_hash[sv.Uuid] = util.HashFromSv(sv, "") }

  entries, err := os.ReadDir(self.RestoredNewDir())
  if err != nil { return err }
  if len(entries) != len_snaps_in_top {
    return fmt.Errorf("%w: should contain 1 file per snapshot in history: %d / %d",
                      ErrValidateNewDir, len(entries), len_snaps_in_top)
  }
  for _,entry := range entries {
    if entry.IsDir() {
      return fmt.Errorf("%w: should not contain directories, got: %s",
                        ErrValidateNewDir, entry.Name())
    }
    got_hash, err := ReadFileIntoString(self.RestoredNewDir(), entry.Name())
    if err != nil { return err }
    expect_hash := sv_to_hash[entry.Name()]
    if strings.Compare(got_hash, expect_hash) != 0 {
      return fmt.Errorf("%w: file, bad content: %x != %x",
                        ErrValidateNewDir, got_hash, expect_hash)
    }
  }
  return nil
}

// Suppose `State.TopDstRestoredPath` contains the `n` snapshot in the sequence.
// Then we expected it contains all data from [0, n-1] as follows:
// `RestoredUuidFile` = [ snap_uuid(0) ... snap_uuid(n-1) ]
//    where snap_uuid is the id used in the backup metadata.
// `RestoredDelDir` = { Hash(snap(n-1), Hash(snap(n-2), ... Hash(snap(0))...)) }
//    where snap has the chunk ids in the backup metadata and the name of the file is snap_uuid(n).
// `RestoredNewDir` = [ {Hash(snap(0))} ... {Hash(snap(n-1)} ]
//    where snap has the chunk ids in the backup metadata and file are named after snap_uuid(i).
// Note that for the "zero" snapshot: Hash(snap(0))="", snap_uuid(0)=""
//
// `State.RestoredSrcSnaps` = [ snap(1) ... snap(n-1) ]
// `State.TopSrcRestoredSnap` = snap(n)
func (self *BackupRestoreCanary) RestoreChainAndValidate(
    ctx context.Context) ([]types.RestorePair, error) {
  pairs, err := self.State.RestoreMgr.RestoreCurrentSequence(ctx, self.State.Uuid)
  if err != nil { return nil, err }
  if self.State.New {
    if len(pairs) != 1 { util.Fatalf("expected only the initial snapshot, got: %v", pairs) }
    self.State.RestoredSrcSnaps = nil
    self.State.TopSrcRestoredSnap = pairs[0].Src
    self.State.TopDstRestoredPath = pairs[0].Dst.MountedPath
    return pairs, self.ValidateEmptyChain()
  }
  if len(pairs) < 2 { util.Fatalf("An existing chain should contain at least 2 snaps") }
  if self.State.TopSrcRestoredSnap != nil { return pairs, ErrCannotCallTwice }

  self.State.RestoredSrcSnaps = make([]*pb.SubVolume, 0, len(pairs))
  for i,pair := range pairs {
    self.State.TopSrcRestoredSnap = pair.Src
    self.State.TopDstRestoredPath = pair.Dst.MountedPath
    if i == len(pairs) - 1 { break }
    self.State.RestoredSrcSnaps = append(self.State.RestoredSrcSnaps, pair.Src)
  }

  err_uuid := self.ValidateUuidFile()
  err_newf := self.ValidateNewDir()
  err_deld := self.ValidateDelDir()

  util.Infof("Validated chain of %d items for vol '%s'",
             len(self.State.RestoredSrcSnaps), self.State.Uuid)
  return pairs, util.Coalesce(err_uuid, err_newf, err_deld)
}

