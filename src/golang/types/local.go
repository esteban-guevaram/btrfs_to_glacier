package types

import (
  "context"
  "io"
  pb "btrfs_to_glacier/messages"
)

type SnapshotChangesOrError struct {
  Val *pb.SnapshotChanges
  Err error
}

type SubVolumeOrError struct {
  Val *pb.SubVolume
  Err error
}

type VolumeManager interface {
  // `path` must be the root of the volume.
  // If `path` does not point to a snapshot the corresponding fields will be empty.
  GetVolume(path string) (*pb.SubVolume, error)
  // Returns the first subvolume under `fs_root` that matches.
  // May return nil if nothing was found.
  FindVolume(fs_root string, matcher func(*pb.SubVolume) bool) (*pb.SubVolume, error)
  // Returns all snapshots whose parent is `subvol`.
  // Returned snaps are sorted by creation generation (oldest first).
  // `received_uuid` will only be set if the snapshot was effectibely received.
  GetSnapshotSeqForVolume(subvol *pb.SubVolume) ([]*pb.SubVolume, error)
  // Returns the changes between 2 snapshots of the same subvolume.
  // Both snaps must come from the same parent and `from` must be from a previous gen than `to`.
  GetChangesBetweenSnaps(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (<-chan SnapshotChangesOrError, error)
}

type VolumeSource interface {
  VolumeManager
  // Creates a read-only snapshot of `subvol`.
  // The path for the new snapshot will be determined by configuration.
  CreateSnapshot(subvol *pb.SubVolume) (*pb.SubVolume, error)
  // Create a pipe with the data from the delta between `from` and `to` snapshots.
  // `from` can be nil to get the full snapshot content.
  GetSnapshotStream(ctx context.Context, from *pb.SubVolume, to *pb.SubVolume) (io.ReadCloser, error)
}

type VolumeDestination interface {
  VolumeManager
  // Deletes a snapshot. Returns an error if attempting to delete a write snapshot or subvolume.
  DeleteSnapshot(snap *pb.SubVolume) error
  // Reads subvolume data from the pipe and creates a subvolume using `btrfs receive`.
  // The path for the new subvolume will be determined by configuration.
  // Takes ownership of `read_pipe` and will close it once done.
  ReceiveSendStream(ctx context.Context, src_subvol *pb.SubVolume, read_pipe io.ReadCloser) (<-chan SubVolumeOrError, error)
}

func ByUuid(uuid string) func(*pb.SubVolume) bool {
  return func(sv *pb.SubVolume) bool { return sv.Uuid == uuid }
}
func ByReceivedUuid(uuid string) func(*pb.SubVolume) bool {
  return func(sv *pb.SubVolume) bool { return sv.ReceivedUuid == uuid }
}


