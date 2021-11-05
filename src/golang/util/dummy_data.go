package util

import (
  "fmt"
  "math/rand"
  "hash/adler32"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"

  "github.com/google/uuid"
)

func DummyBtrfsSrc(sv_list []string, snap_list []string) *pb.Source {
  src := &pb.Source{
    Type: pb.Source_BTRFS,
    Name: uuid.NewString(),
  }
  def_snap_path := snap_list[0]
  for idx,sv := range sv_list {
    pair := &pb.Source_VolSnapPathPair{
      VolPath: sv,
      SnapPath: def_snap_path,
    }
    if len(snap_list) > idx { pair.SnapPath = snap_list[idx] }
    src.Paths = append(src.Paths, pair)
  }
  return src
}

func DummyMountEntry(btrfs_id uint64, mnt_path string, tree_path string) *types.MountEntry {
  mnt := &types.MountEntry{
    Id: int(adler32.Checksum([]byte(mnt_path)) + adler32.Checksum([]byte(tree_path))),
    TreePath: tree_path,
    MountedPath: mnt_path,
    DevPath: "",
    Options: nil,
    BtrfsVolId: btrfs_id,
  }
  mnt.Minor = mnt.Id
  mnt.Major = mnt.Id
  var bind types.MountEntry = *mnt
  bind.Id = rand.Int()
  bind.MountedPath = fmt.Sprintf("/binds%s", mnt.MountedPath)
  mnt.Binds = []*types.MountEntry{ &bind, }
  return mnt
}

func DummyFilesystem(mnts []*types.MountEntry) *types.Filesystem {
  return &types.Filesystem{
    Uuid: uuid.NewString(),
    Label: uuid.NewString(),
    Devices: nil,
    Mounts: mnts,
  }
}

func DummyChunks(chunk_uuid string) *pb.SnapshotChunks {
  chunk := &pb.SnapshotChunks_Chunk {
    Uuid: chunk_uuid,
    Start: 0,
    Size: 3,
  }
  return &pb.SnapshotChunks{
    KeyFingerprint: "fp",
    Chunks: []*pb.SnapshotChunks_Chunk{chunk},
  }
}

func DummySubVolumeFromMount(mnt *types.MountEntry) *pb.SubVolume {
  sv := DummySubVolume(uuid.NewString())
  sv.MountedPath = mnt.MountedPath
  sv.TreePath = mnt.TreePath
  sv.VolId = mnt.BtrfsVolId
  return sv
}

func DummySubVolume(vol_uuid string) *pb.SubVolume {
 return &pb.SubVolume{
    Uuid: vol_uuid,
    VolId: (uint64)(adler32.Checksum([]byte(vol_uuid))),
    TreePath: vol_uuid,
    MountedPath: fmt.Sprintf("/banana_stashes/%s", vol_uuid),
    CreatedTs: 666,
    OriginSys: &pb.SystemInfo{
      KernMajor: 1,
      BtrfsUsrMajor: 1,
      ToolGitCommit: "commit_hash",
    },
  }
}

func DummySnapshot(snap_uuid string, vol_uuid string) *pb.SubVolume {
  vol := DummySubVolume(snap_uuid)
  vol.ParentUuid = vol_uuid
  vol.TreePath = fmt.Sprintf("snaps/%s", snap_uuid)
  vol.MountedPath = ""
  vol.ReadOnly = true
  vol.CreatedTs += 111
  vol.GenAtCreation = 777
  return vol
}

func DummySnapshotSequence(vol_uuid string, seq_uuid string) *pb.SnapshotSequence {
  vol := DummySubVolume(vol_uuid)
  snap := fmt.Sprintf("%s_snap", vol_uuid)
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    SnapUuids: []string{snap},
  }
}

func DummySnapshotSeqHead(seq *pb.SnapshotSequence, prev ...string) *pb.SnapshotSeqHead {
  return &pb.SnapshotSeqHead{
    Uuid: seq.Volume.Uuid,
    CurSeqUuid: seq.Uuid,
    PrevSeqUuid: prev,
  }
}

