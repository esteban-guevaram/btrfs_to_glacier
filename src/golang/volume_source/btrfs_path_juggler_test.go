package volume_source

import (
  "errors"
  "strings"
  "testing"
  fpmod "path/filepath"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_source/shim"

  "github.com/google/uuid"
)

func buildSimpleFsList() []*types.Filesystem {
  fs1_mnts := []*types.MountEntry{
    util.DummyMountEntry(shim.BTRFS_FS_TREE_OBJECTID, "/tmp/fs1_root", ""),
    util.DummyMountEntry(256, "/tmp/fs1_subvol_root", "subvol1"),
  }
  fs2_mnts := []*types.MountEntry{
    util.DummyMountEntry(257, "/tmp/fs2_snaps", "snaps"),
    // for tests, it is important we have the same id in different fs
    util.DummyMountEntry(256, "/tmp/fs2_subvol_root", "subvol2"),
  }
  fs1 := util.DummyFilesystem(fs1_mnts)
  fs2 := util.DummyFilesystem(fs2_mnts)
  fs_list := []*types.Filesystem{ fs1, fs2, }
  //util.Debugf("fs_list: %s", util.AsJson(fs_list))
  return fs_list
}

func buildTestJuggler(fs_list []*types.Filesystem) (*BtrfsPathJuggler, *mocks.Btrfsutil, *mocks.Linuxutil) {
  conf := util.LoadTestConf()
  btrfsutil := &mocks.Btrfsutil{}
  linuxutil := &mocks.Linuxutil{}
  juggler := &BtrfsPathJuggler{
    Btrfsutil: btrfsutil,
    Linuxutil: linuxutil,
    Conf: conf,
    Filesystems: fs_list,
    IsDir: func(string) bool { return true },
  }
  for _,fs := range fs_list {
    for _,mnt := range fs.Mounts {
      btrfsutil.Subvols = append(btrfsutil.Subvols, util.DummySubVolumeFromMount(mnt))
    }
  }
  return juggler, btrfsutil, linuxutil
}

// Also adds it to the list of mocked subvolumes
func createSvForQuery(btrfsutil *mocks.Btrfsutil, mnt_path string, tree_path string, vol_id uint64) *pb.SubVolume {
  sv := util.DummySubVolume(uuid.NewString())
  sv.MountedPath = ""
  sv.TreePath = tree_path
  if vol_id > 0 { sv.VolId = vol_id }
  var copy_sv pb.SubVolume = *sv
  copy_sv.MountedPath = mnt_path
  btrfsutil.Subvols = append(btrfsutil.Subvols, &copy_sv)
  return sv
}

func TestFindFsAndTighterMountOwningPath_SvMountExists(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  expect_fs := fs_list[0]
  expect_mnt := expect_fs.Mounts[1]
  fs,mnt,_,err := juggler.FindFsAndTighterMountOwningPath(expect_mnt.MountedPath)
  if err != nil { t.Fatalf("FindFsAndTighterMountOwningPath err: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs found", expect_fs, fs)
  util.EqualsOrFailTest(t, "Bad mnt found", expect_mnt, mnt)
}

func TestFindFsAndTighterMountOwningPath_InnerPath(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  expect_fs := fs_list[0]
  expect_mnt := expect_fs.Mounts[0]
  path := fpmod.Join(expect_mnt.MountedPath, "inner_dir/bla/bla")
  inner_sv := util.DummySubVolume(uuid.NewString())
  inner_sv.MountedPath = fpmod.Dir(path)
  btrfsutil.Subvols = append(btrfsutil.Subvols, inner_sv)

  fs,mnt,id,err := juggler.FindFsAndTighterMountOwningPath(path)
  if err != nil { t.Fatalf("FindFsAndTighterMountOwningPath err: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs found", fs, expect_fs)
  util.EqualsOrFailTest(t, "Bad mnt found", mnt, expect_mnt)
  util.EqualsOrFailTest(t, "Bad vol id", id, inner_sv.VolId)
}

func TestFindFsAndTighterMountOwningPath_NoMount(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  path := "/this/is/not/a/btrfs/fs"
  _,_,_,err := juggler.FindFsAndTighterMountOwningPath(path)
  if err == nil { t.Errorf("should not have found any filesystem") }
}

func TestFindFsAndTighterMountOwningPath_NestedMounts(t *testing.T) {
  fs_list := buildSimpleFsList()
  path := fpmod.Join(fs_list[0].Mounts[0].MountedPath, "nested_mnt/bla")
  fs3_mnts := []*types.MountEntry{
    util.DummyMountEntry(257, fpmod.Dir(path), "surprise"),
  }
  fs3 := util.DummyFilesystem(fs3_mnts)
  fs_list = append(fs_list, fs3)

  juggler,_,_ := buildTestJuggler(fs_list)
  expect_fs := fs_list[2]
  expect_mnt := expect_fs.Mounts[0]
  fs,mnt,_,err := juggler.FindFsAndTighterMountOwningPath(path)
  if err != nil { t.Fatalf("FindFsAndTighterMountOwningPath err: %v", err) }
  util.EqualsOrFailTest(t, "Bad fs found", expect_fs, fs)
  util.EqualsOrFailTest(t, "Bad mnt found", expect_mnt, mnt)
}

func TestFindFsAndTighterMountOwningPath_NotBtrfs(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  path := fpmod.Join(fs_list[0].Mounts[0].MountedPath, "not/btrfs")
  btrfsutil.Subvols = nil
  _,_,_,err := juggler.FindFsAndTighterMountOwningPath(path)
  if err == nil { t.Errorf("should not have found any filesystem") }
}

// FindTighterMountForSubVolume

func TestFindTighterMountForSubVolume_SvMountExists(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  expect_fs := fs_list[0]
  expect_mnt := expect_fs.Mounts[1]
  var sv pb.SubVolume = *btrfsutil.Subvols[1]
  sv.MountedPath = ""
  got_fs,mnt,got_path,err := juggler.FindTighterMountForSubVolume(fs_list, &sv)
  if err != nil { t.Fatalf("FindTighterMountForSubVolume err: %v", err) }
  util.EqualsOrFailTest(t, "Bad mnt found", expect_mnt, mnt)
  util.EqualsOrFailTest(t, "Bad fs found", expect_fs, got_fs)
  util.EqualsOrFailTest(t, "Bad path found", expect_mnt.MountedPath, got_path)
}

func TestFindTighterMountForSubVolume_WrongFs(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  var sv pb.SubVolume = *btrfsutil.Subvols[1]
  sv.MountedPath = ""
  _,mnt,_,err := juggler.FindTighterMountForSubVolume(fs_list[1:], &sv)
  if err == nil { t.Errorf("FindTighterMountForSubVolume should have failed: %s", util.AsJson(&sv)) }
  util.Debugf("MountEntry: %s", util.AsJson(mnt))
}

func TestFindTighterMountForSubVolume_PathNotExist(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  sv := createSvForQuery(btrfsutil, "", "does/not/exist", 0)
  juggler.IsDir = func (p string) bool { return !strings.HasSuffix(p, sv.TreePath) }

  _,mnt,_,err := juggler.FindTighterMountForSubVolume(fs_list, sv)
  if !errors.Is(err, types.ErrNotMounted) {
    t.Errorf("FindTighterMountForSubVolume should return not mounted: %v", err)
  }
  util.Debugf("MountEntry: %s\nsv: %s", util.AsJson(mnt), util.AsJson(sv))
}

func TestFindTighterMountForSubVolume_InnerPath(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  expect_fs := fs_list[1]
  expect_mnt := expect_fs.Mounts[0]
  expect_path := fpmod.Join(btrfsutil.Subvols[2].MountedPath, "asnap")
  sv := createSvForQuery(btrfsutil, expect_path,
                         fpmod.Join(btrfsutil.Subvols[2].TreePath, "asnap"), 0)

  got_fs,mnt,got_path,err := juggler.FindTighterMountForSubVolume(fs_list, sv)
  if err != nil { t.Fatalf("FindTighterMountForSubVolume err: %v", err) }
  util.EqualsOrFailTest(t, "Bad mnt found", expect_mnt, mnt)
  util.EqualsOrFailTest(t, "Bad fs found", expect_fs, got_fs)
  util.EqualsOrFailTest(t, "Bad path found", expect_path, got_path)
}

func TestFindTighterMountForSubVolume_TreeRootNotMounted(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  sv := createSvForQuery(btrfsutil, "", "", shim.BTRFS_FS_TREE_OBJECTID)

  _,mnt,_,err := juggler.FindTighterMountForSubVolume(fs_list, sv)
  if err == nil { t.Errorf("FindTighterMountForSubVolume should have failed: %s", util.AsJson(sv)) }
  if !errors.Is(err, types.ErrNotMounted) { t.Errorf("expected ErrNotMounted") }
  util.Debugf("MountEntry: %s\nsv: %s", util.AsJson(mnt), util.AsJson(sv))
}

func TestFindTighterMountForSubVolume_NestedFs(t *testing.T) {
  fs_list := buildSimpleFsList()
  mnt_path := fpmod.Join(fs_list[1].Mounts[0].MountedPath, "another_vol")
  tree_path := fpmod.Join(fs_list[1].Mounts[0].TreePath, "another_vol")
  fs3 := util.DummyFilesystem([]*types.MountEntry{
    util.DummyMountEntry(258, mnt_path, tree_path),
  })
  fs_list = append(fs_list, fs3)
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  sv := createSvForQuery(btrfsutil, fs3.Mounts[0].MountedPath,
                         fs3.Mounts[0].TreePath, fs3.Mounts[0].BtrfsVolId)

  _,mnt,_,err := juggler.FindTighterMountForSubVolume(fs_list, sv)
  if err == nil { t.Errorf("FindTighterMountForSubVolume should have failed: %s", util.AsJson(sv)) }
  util.Debugf("MountEntry: %s\nsv: %s", util.AsJson(mnt), util.AsJson(sv))
}

func TestFindTighterMountForSubVolume_DupeTreePathAndVolId(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  dupe_mnt := fs_list[0].Mounts[1]
  sv := createSvForQuery(btrfsutil, "",
                         dupe_mnt.TreePath, dupe_mnt.BtrfsVolId)

  _,mnt,_,err := juggler.FindTighterMountForSubVolume(fs_list, sv)
  if !errors.Is(err, types.ErrNotMounted) { t.Errorf("FindTighterMountForSubVolume should have failed: %v", err) }
  util.Debugf("MountEntry: %s\nsv: %s\ndupe: %s",
              util.AsJson(mnt), util.AsJson(sv), util.AsJson(btrfsutil.Subvols[1]))
}

// CheckSourcesAndReturnCorrespondingFs

func TestCheckSourcesAndReturnCorrespondingFs_Ok(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  src_list := []*pb.Source{
    util.DummyBtrfsSrc([]string{fs_list[0].Mounts[0].MountedPath,},
                       []string{fs_list[0].Mounts[0].MountedPath,}),
  }
  fs,err := juggler.CheckSourcesAndReturnCorrespondingFs(src_list)
  if err != nil { t.Errorf("CheckSourcesAndReturnCorrespondingFs fail: %v", err) }
  if len(fs) != 1 { t.Errorf("wrong fs len ?") }
  if fs[0].Uuid != fs_list[0].Uuid { t.Errorf("Selected wrong fs") }
}

func TestCheckSourcesAndReturnCorrespondingFs_NotSingleFs(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  src_list := []*pb.Source{
    util.DummyBtrfsSrc([]string{fs_list[0].Mounts[1].MountedPath,
                                fs_list[0].Mounts[0].MountedPath,},
                       []string{fs_list[1].Mounts[0].MountedPath,}),
  }
  _,err := juggler.CheckSourcesAndReturnCorrespondingFs(src_list)
  if err == nil { t.Errorf("CheckSourcesAndReturnCorrespondingFs did not fail: %s", util.AsJson(src_list)) }
}

func TestCheckSourcesAndReturnCorrespondingFs_NotSvRoot(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  inner_path := fpmod.Join(fs_list[0].Mounts[0].MountedPath, "coucou")
  src_list := []*pb.Source{
    util.DummyBtrfsSrc([]string{inner_path,},
                       []string{fs_list[0].Mounts[0].MountedPath,}),
  }
  _,err := juggler.CheckSourcesAndReturnCorrespondingFs(src_list)
  if err == nil { t.Errorf("CheckSourcesAndReturnCorrespondingFs did not fail: %s", util.AsJson(src_list)) }
}

func TestCheckSourcesAndReturnCorrespondingFs_DupeSv(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,btrfsutil,_ := buildTestJuggler(fs_list)
  dupe_path := fpmod.Join(fs_list[0].Mounts[0].MountedPath, btrfsutil.Subvols[1].TreePath)
  dupe_sv := util.DummySubVolume(uuid.NewString())
  dupe_sv.TreePath = btrfsutil.Subvols[0].TreePath
  dupe_sv.MountedPath = dupe_path
  dupe_sv.VolId = btrfsutil.Subvols[1].VolId
  btrfsutil.Subvols = append(btrfsutil.Subvols, dupe_sv)
  src_list := []*pb.Source{
    util.DummyBtrfsSrc([]string{fs_list[0].Mounts[1].MountedPath,
                                dupe_path,},
                       []string{fs_list[0].Mounts[0].MountedPath,}),
  }
  _,err := juggler.CheckSourcesAndReturnCorrespondingFs(src_list)
  if err == nil { t.Errorf("CheckSourcesAndReturnCorrespondingFs did not fail: %s", util.AsJson(src_list)) }
  util.Debugf("Dummy subvols: %s", util.AsJson(btrfsutil.Subvols))
}

func TestCheckSourcesAndReturnCorrespondingFs_BadSnapPath(t *testing.T) {
  fs_list := buildSimpleFsList()
  juggler,_,_ := buildTestJuggler(fs_list)
  src_list := []*pb.Source{
    util.DummyBtrfsSrc([]string{fs_list[0].Mounts[0].MountedPath,},
                       []string{fs_list[1].Mounts[0].MountedPath,}),
  }
  _,err := juggler.CheckSourcesAndReturnCorrespondingFs(src_list)
  if err == nil { t.Errorf("CheckSourcesAndReturnCorrespondingFs did not fail: %s", util.AsJson(src_list)) }
}

