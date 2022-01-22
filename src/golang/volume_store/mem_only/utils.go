package mem_only

import (
  "testing"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
)


func CompareStates(t *testing.T, msg string, left *pb.AllMetadata, right *pb.AllMetadata) {
  util.EqualsOrFailTest(t, msg, left.Heads, right.Heads)
  util.EqualsOrFailTest(t, msg, left.Sequences, right.Sequences)
  util.EqualsOrFailTest(t, msg, left.Snapshots, right.Snapshots)
}

