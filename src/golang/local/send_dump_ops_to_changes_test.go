package local

import (
  "testing"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

func TestGetChangesBetweenSnaps_NewTree(t *testing.T) {
  dump_ops := &types.SendDumpOperations{
    Written: map[string]bool{
      "adir/asubdir/coucou3": true,
      "adir/asubdir2/coucou4": true,
      "adir/coucou2": true,
    },
    New: map[string]bool{
      "o260-8-0": true,
      "o261-8-0": true,
      "o263-9-0": true,
    },
    NewDir: map[string]bool{
      "o258-8-0": true,
      "o259-8-0": true,
      "o262-9-0": true,
      "o264-9-0": true,
    },
    Deleted: map[string]bool{},
    DelDir: map[string]bool{},
    FromTo: map[string]string{
      "o258-8-0": "adir",
      "o259-8-0": "adir/asubdir",
      "o260-8-0": "adir/coucou2",
      "o261-8-0": "adir/asubdir/coucou3",
      "o262-9-0": "adir/asubdir2",
      "o263-9-0": "adir/asubdir2/coucou4",
      "o264-9-0": "adir/asubdir_empty",
    },
    ToUuid: "72124d274e1e67428b7eed6909b00537",
    FromUuid: "5196e8fc2aa495499e24d9df818ba8c7",
  }
  expect_changes := &pb.SnapshotChanges{
    FromUuid: dump_ops.FromUuid,
    ToUuid: dump_ops.ToUuid,
    Changes: []*pb.SnapshotChanges_Change{
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW_DIR,
        Path: "adir",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW_DIR,
        Path: "adir/asubdir",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW,
        Path: "adir/asubdir/coucou3",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW_DIR,
        Path: "adir/asubdir2",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW,
        Path: "adir/asubdir2/coucou4",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW_DIR,
        Path: "adir/asubdir_empty",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW,
        Path: "adir/coucou2",
      },
    },
  }
  changes := sendDumpOpsToSnapChanges(dump_ops)
  util.EqualsOrFailTest(t, "Bad changes", changes, expect_changes)
}

func TestGetChangesBetweenSnaps_DelTree(t *testing.T) {
  dump_ops := &types.SendDumpOperations{
    Written: map[string]bool{},
    New: map[string]bool{},
    NewDir: map[string]bool{},
    Deleted: map[string]bool{
      "o258-8-0/coucou2": true,
      "o259-8-0/coucou3": true,
      "o262-9-0/coucou4": true,
    },
    DelDir: map[string]bool{
      "o258-8-0": true,
      "o258-8-0/asubdir_empty": true,
      "o259-8-0": true,
      "o262-9-0": true,
    },
    FromTo: map[string]string{
      "adir": "o258-8-0",
      "o258-8-0/asubdir": "o259-8-0",
      "o258-8-0/asubdir2": "o262-9-0",
    },
    ToUuid: "b370a17d42dc5444a8e688d0962d85b2",
    FromUuid: "72124d274e1e67428b7eed6909b00537",
  }
  expect_changes := &pb.SnapshotChanges{
    FromUuid: dump_ops.FromUuid,
    ToUuid: dump_ops.ToUuid,
    Changes: []*pb.SnapshotChanges_Change{
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DEL_DIR,
        Path: "adir",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DEL_DIR,
        Path: "adir/asubdir",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DELETE,
        Path: "adir/asubdir/coucou3",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DEL_DIR,
        Path: "adir/asubdir2",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DELETE,
        Path: "adir/asubdir2/coucou4",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DEL_DIR,
        Path: "adir/asubdir_empty",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_DELETE,
        Path: "adir/coucou2",
      },
    },
  }
  changes := sendDumpOpsToSnapChanges(dump_ops)
  util.EqualsOrFailTest(t, "Bad changes", changes, expect_changes)
}

func TestGetChangesBetweenSnaps_MovTree(t *testing.T) {
  dump_ops := &types.SendDumpOperations{
    Written: map[string]bool{
      "mvdir/coucou2": true,
      "mvdir/coucou4": true,
    },
    New: map[string]bool{
      "o262-10-0": true,
    },
    NewDir: map[string]bool{},
    Deleted: map[string]bool{
      "mvdir/coucou3": true,
    },
    DelDir: map[string]bool{},
    FromTo: map[string]string{
      "adir": "mvdir",
      "mvdir/coucou3": "mvdir/mv_coucou",
      "o262-10-0": "mvdir/coucou4",
    },
    ToUuid: "7d26d0a22c93e5438787b0a675a4db92",
    FromUuid: "e0366499f02a9d48a8e0e2355f9c43a6",
  }
  expect_changes := &pb.SnapshotChanges{
    FromUuid: dump_ops.FromUuid,
    ToUuid: dump_ops.ToUuid,
    Changes: []*pb.SnapshotChanges_Change{
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_MOVE,
        Path: "mvdir",
        From: "adir",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_WRITE,
        Path: "mvdir/coucou2",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_NEW,
        Path: "mvdir/coucou4",
      },
      &pb.SnapshotChanges_Change{
        Type: pb.SnapshotChanges_MOVE,
        Path: "mvdir/mv_coucou",
        From: "adir/coucou3",
      },
    },
  }
  changes := sendDumpOpsToSnapChanges(dump_ops)
  util.EqualsOrFailTest(t, "Bad changes", changes, expect_changes)
}

