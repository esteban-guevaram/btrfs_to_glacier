package garbage_collector

import (
  "testing"

  "btrfs_to_glacier/types/mocks"
  "btrfs_to_glacier/util"
)

func TestDummyDataProperties(t *testing.T) {
  meta, store := mocks.DummyMetaAndStorage(1,2,3,4)
  util.EqualsOrFailTest(t, "Bad chunk count", len(store.Chunks), 24)
  util.EqualsOrFailTest(t, "Bad snap count", len(meta.Snaps), 6)
  for _,snap := range meta.Snaps {
    for _,chunk := range snap.Data.Chunks {
      _,found := store.Chunks[chunk.Uuid]
      util.EqualsOrFailTest(t, "Chunk not found in storage", found, true)
    }
  }
  meta, store = mocks.DummyMetaAndStorage(4,3,2,1)
  util.EqualsOrFailTest(t, "Bad chunk count", len(store.Chunks), 24)
  util.EqualsOrFailTest(t, "Bad snap count", len(meta.Snaps), 24)
  for _,snap := range meta.Snaps {
    for _,chunk := range snap.Data.Chunks {
      _,found := store.Chunks[chunk.Uuid]
      util.EqualsOrFailTest(t, "Chunk not found in storage", found, true)
    }
  }
  meta, store = mocks.DummyMetaAndStorage(1,1,1,1)
  util.EqualsOrFailTest(t, "Bad chunk count", len(store.Chunks), 1)
  util.EqualsOrFailTest(t, "Bad snap count", len(meta.Snaps), 1)
}

func TestCleanUnreachableChunks_NoneFound(t *testing.T) {
}

func TestCleanUnreachableChunks_DryRun(t *testing.T) {
}

