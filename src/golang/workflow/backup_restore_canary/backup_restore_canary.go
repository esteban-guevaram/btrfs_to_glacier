package backup_restore_canary

import (
  "bytes"
  "context"
  "crypto/md5"
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

type FactoryIf interface {
  //Meta     types.Metadata
  //Store    types.Storage
  NewBackupManager(ctx context.Context,
    conf *pb.Config, src_name string) (types.BackupManagerAdmin, error)
  NewRestoreManager(ctx context.Context,
    conf *pb.Config, dst_name string) (types.RestoreManager, error)
}

// Fields that may change value during the execution.
type State struct {
  FakeConf    *pb.Config
  Fs          *types.Filesystem
  New         bool
  Uuid        string
  VolRoot     string
  SnapRoot    string
  RestoreRoot string
  BackupMgr   types.BackupManagerAdmin
  RestoreMgr  types.RestoreManager
}

// Note: this type cannot be abstracted away from btrfs.
// It needs to perform some operations that are not available in types.VolumeManager.
type BackupRestoreCanary struct {
  Conf     *pb.Config
  Btrfs    types.Btrfsutil
  Lnxutil  types.Linuxutil
  Factory  FactoryIf
  State    *State
}

// Creates empty btrfs filesystem on loop device.
// Prepares State to point to the newly created filesystem.
// Creates a new volume if this is the first time the canary is run.
// Prerequisite: Storage and Metadata have previously been setup.
func (self *BackupRestoreCanary) Setup(ctx context.Context) error {
  if self.State != nil {
    util.Infof("Setup twice is a noop: %s", self.State.Uuid)
    return nil
  }
  dev, err := self.Lnxutil.CreateLoopDevice(ctx, LoopDevSizeMb)
  if err != nil { return err }

  // Set an empty state to indicate there is something to tear down.
  self.State = &State{
    Fs: &types.Filesystem{ Devices: []*types.Device{dev,}, },
  }

  label := uuid.NewString()
  fs, err := self.Lnxutil.CreateBtrfsFilesystem(ctx, dev, label, "--mixed")
  if err != nil { return err }
  self.State.Fs = fs

  target_mnt, err := os.MkdirTemp("", label)
  if err != nil { return err }
  mnt, err := self.Lnxutil.Mount(ctx, fs.Uuid, target_mnt)
  err = self.SetupPathsInNewFs()
  if err != nil { return err }

  util.Infof("Mounted %s subvol %d", fs.Uuid, mnt.BtrfsVolId)
  fs.Mounts = append(fs.Mounts, mnt)

  err = self.PrepareState(ctx)
  return err
}

func (self *BackupRestoreCanary) PrepareState(ctx context.Context) error {
  err := self.SetupPathsInNewFs()
  if err != nil { return err }

  self.State.FakeConf = self.BuildFakeConf()

  self.State.BackupMgr, err = self.Factory.NewBackupManager(ctx, self.State.FakeConf, FakeSource)
  if err != nil { return err }

  self.State.RestoreMgr, err = self.Factory.NewRestoreManager(ctx, self.State.FakeConf, FakeSource)
  if err != nil { return err }

  self.State.Uuid, err = self.DetermineVolUuid(ctx)
  if err != nil { return err }

  if len(self.State.Uuid) < 1 {
    self.State.New = true
    err = self.Btrfs.CreateSubvolume(self.State.VolRoot)
    if err != nil { return err }
    err = self.CreateFirstValidationChainItem()
    if err != nil { return err }
    _, err = self.State.BackupMgr.BackupAllToCurrentSequences(ctx)
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
  fake_conf.Stores = nil

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
//                   # for first snapshot this will simply be a band new subvolume
// * snapshots/      # new snapshots in the sequence go here
// * subvol/uuids    # contains all snapshot uuids in history, one per line
//                   # empty for the first snapshot
// * subvol/deleted/ # contains a single file named after the most recent snapshot, contains the snapshot uuid hash
//                   # no files for the first snapshot
// * subvol/new/     # contains one file per snapshot, named as its uuid, contains the snapshot uuid hash
//                   # no files for the first snapshot
func (self *BackupRestoreCanary) SetupPathsInNewFs() error {
  root_vol := self.State.Fs.Mounts[0].MountedPath
  self.State.VolRoot = fpmod.Join(root_vol, "subvol")
  if err := os.Mkdir(self.State.VolRoot, 0775); err != nil { return err }

  self.State.RestoreRoot = fpmod.Join(root_vol, "restores")
  if err := os.Mkdir(self.State.RestoreRoot, 0775); err != nil { return err }

  self.State.SnapRoot = fpmod.Join(root_vol, "snapshots")
  if err := os.Mkdir(self.State.SnapRoot, 0775); err != nil { return err }
  return nil
}

func (self *BackupRestoreCanary) DelDir() string {
  return fpmod.Join(self.State.VolRoot, "deleted")
}
func (self *BackupRestoreCanary) NewDir() string {
  return fpmod.Join(self.State.VolRoot, "new")
}
func (self *BackupRestoreCanary) UuidFile() string {
  return fpmod.Join(self.State.VolRoot, "uuids")
}

func (self *BackupRestoreCanary) CreateFirstValidationChainItem() error {
  if err := os.Mkdir(self.DelDir(), 0775); err != nil { return err }
  if err := os.Mkdir(self.NewDir(), 0775); err != nil { return err }
  f, err := os.Create(self.UuidFile())
  return util.Coalesce(err, f.Close())
}

// Destroys the loop device and backing file.
// In case of a partial `Setup()`, attempts to delete any dangling infracstructure.
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
// btrfs send -p restores/asubvol.snap.2 restores/asubvol.snap.3 | btrfs receive restore2
// comm -3 <(find restore2/asubvol.snap.3 -printf "./%P\n") <(find clones/asubvol.clone.2 -printf "./%P\n")
// # both subvolumes contain the same files
func (self *BackupRestoreCanary) AppendSnapshotToValidationChain(ctx context.Context, last_snap *pb.SubVolume) error {
  if !self.State.New {
    err := self.Btrfs.CreateClone(last_snap.MountedPath, self.State.VolRoot)
    if err != nil { return err }
  }

  err := self.AppendDataToSnapshot(last_snap)
  if err != nil { return err }

  src, err := self.Btrfs.SubVolumeInfo(self.State.VolRoot)
  if err != nil { return err }
  // if self.State.New then `src` and self.State.Uuid do share the same parent
  _, err = self.State.BackupMgr.BackupToCurrentSequenceUnrelatedVol(ctx, src, self.State.Uuid)
  return err
}

func Hash(content []byte) []byte {
  hash := md5.Sum(content)
  return hash[:]
}
func HashStr(content string) []byte { return Hash([]byte(content)) }

func (self *BackupRestoreCanary) AppendDataToSnapshot(last_snap *pb.SubVolume) error {
  f, err := os.OpenFile(self.UuidFile(), os.O_APPEND, 0666)
  if err != nil { return err }
  _, err_w1 := f.Write([]byte(last_snap.Uuid))
  _, err_w2 := f.Write([]byte("\n"))
  err_cl := f.Close()
  if err = util.Coalesce(err_w1, err_w2, err_cl); err != nil { return err }

  content := HashStr(last_snap.Uuid)
  newpath := fpmod.Join(self.NewDir(), last_snap.Uuid)
  f, err = os.Create(newpath)
  if err != nil { return err }
  _, err_w1 = f.Write([]byte(fmt.Sprintf("%d", content)))
  err_cl = f.Close()
  if err = util.Coalesce(err_w1, err_cl); err != nil { return err }

  entries, err := os.ReadDir(self.DelDir())
  if err != nil { return err }
  for _,e := range entries {
    path := fpmod.Join(self.DelDir(), e.Name())
    if err = util.RemoveAll(path); err != nil { return err }
  }
  newpath = fpmod.Join(self.DelDir(), last_snap.Uuid)
  f, err = os.Create(newpath)
  if err != nil { return err }
  _, err_w1 = f.Write([]byte(fmt.Sprintf("%d", content)))
  err_cl = f.Close()
  return util.Coalesce(err_w1, err_cl)
}

func (self *BackupRestoreCanary) ValidateEmptyChain() error {
  content, err := os.ReadFile(self.UuidFile())
  if err != nil { return err }
  if len(content) > 0 { return fmt.Errorf("%s should be empty", self.UuidFile()) }

  entries, err := os.ReadDir(self.DelDir())
  if err != nil { return err }
  if len(entries) > 0 { return fmt.Errorf("%s should be empty", self.DelDir()) }

  entries, err = os.ReadDir(self.NewDir())
  if err != nil { return err }
  if len(entries) > 0 { return fmt.Errorf("%s should be empty", self.NewDir()) }
  return nil
}

func (self *BackupRestoreCanary) RestoreChainAndValidate(ctx context.Context) error {
  subvols, err := self.State.RestoreMgr.RestoreCurrentSequence(ctx, self.State.Uuid)
  if err != nil { return err }
  if self.State.New {
    if len(subvols) != 1 { util.Fatalf("expected only the initial snapshot, got: %v", subvols) }
    return self.ValidateEmptyChain()
  }

  sv_to_validate := subvols[0:len(subvols)-1]
  last_snap := sv_to_validate[len(sv_to_validate)-1]
  sv_to_hash := make(map[string][]byte)
  for _,sv := range sv_to_validate {
    sv_to_hash[sv.Uuid] = HashStr(sv.Uuid)
  }

  content, err := os.ReadFile(self.UuidFile())
  if err != nil { return err }
  lines := strings.Split(strings.TrimSpace(string(content)), "\n")
  if len(lines) != len(sv_to_validate) {
    return fmt.Errorf("Volume does not contain a list of all of its ancestors: %d / %d",
                      len(lines), len(sv_to_validate))
  }
  for i,l := range lines {
    if l != sv_to_validate[i].Uuid {
      return fmt.Errorf("Snapshot history mismatch: %s / %s", l, sv_to_validate[i].Uuid)
    }
  }

  entries, err := os.ReadDir(self.DelDir())
  if err != nil { return err }
  if len(entries) != 1 {
    return fmt.Errorf("DelDir should contain only 1 file, got: %d", len(entries))
  }
  if last_snap.Uuid != entries[0].Name() {
    return fmt.Errorf("DelDir should contain a file named after the most recent snapshot: '%s'",
                      entries[0].Name())
  }
  content, err = os.ReadFile(fpmod.Join(self.DelDir(), entries[0].Name()))
  if err != nil { return err }
  got_hash := Hash(content)
  expect_hash := sv_to_hash[last_snap.Uuid]
  if bytes.Compare(got_hash, expect_hash) != 0 {
    return fmt.Errorf("DelDir file, bad content: %x != %x", got_hash, expect_hash)
  }

  entries, err = os.ReadDir(self.NewDir())
  if err != nil { return err }
  if len(entries) != len(sv_to_validate) {
    return fmt.Errorf("NewDir should contain 1 file per snapshot in history: %d / %d",
                      len(entries), len(sv_to_validate))
  }
  for _,entry := range entries {
    if entry.IsDir() { return fmt.Errorf("NewDir should not contain directories, got: %s", entry.Name()) }
    content, err := os.ReadFile(fpmod.Join(self.NewDir(), entry.Name()))
    if err != nil { return err }
    got_hash := Hash(content)
    expect_hash := sv_to_hash[entry.Name()]
    if bytes.Compare(got_hash, expect_hash) != 0 {
      return fmt.Errorf("NewDir file, bad content: %x != %x", got_hash, expect_hash)
    }
  }

  util.Infof("Validated chain of %d items for vol '%s'", len(sv_to_validate), self.State.Uuid)
  return nil
}

