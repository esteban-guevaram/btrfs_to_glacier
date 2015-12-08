#ifndef __BTRFS_EXTENT_IO_H__
#define __BTRFS_EXTENT_IO_H__

#include "list.h"
#include "rbtree.h"

#define EXTENT_DIRTY 1
#define EXTENT_WRITEBACK (1 << 1)
#define EXTENT_UPTODATE (1 << 2)
#define EXTENT_LOCKED (1 << 3)
#define EXTENT_NEW (1 << 4)
#define EXTENT_DELALLOC (1 << 5)
#define EXTENT_DEFRAG (1 << 6)
#define EXTENT_DEFRAG_DONE (1 << 7)
#define EXTENT_BUFFER_FILLED (1 << 8)
#define EXTENT_CSUM (1 << 9)
#define EXTENT_BAD_TRANSID (1 << 10)
#define EXTENT_BUFFER_DUMMY (1 << 11)
#define EXTENT_IOBITS (EXTENT_LOCKED | EXTENT_WRITEBACK)

#define BLOCK_GROUP_DATA     EXTENT_WRITEBACK
#define BLOCK_GROUP_METADATA EXTENT_UPTODATE
#define BLOCK_GROUP_SYSTEM   EXTENT_NEW

#define BLOCK_GROUP_DIRTY EXTENT_DIRTY

struct btrfs_fs_info;

struct cache_tree {
  struct rb_root root;
};

struct cache_extent {
  struct rb_node rb_node;
  u64 objectid;
  u64 start;
  u64 size;
};

struct extent_io_tree {
  struct cache_tree state;
  struct cache_tree cache;
  struct list_head lru;
  u64 cache_size;
};

struct extent_state {
  struct cache_extent cache_node;
  u64 start;
  u64 end;
  int refs;
  unsigned long state;
  u64 xprivate;
};

struct extent_buffer {
  struct cache_extent cache_node;
  u64 start;
  u64 dev_bytenr;
  u32 len;
  struct extent_io_tree *tree;
  struct list_head lru;
  struct list_head recow;
  int refs;
  int flags;
  int fd;
  char data[];
};

static inline void extent_buffer_get(struct extent_buffer *eb)
{
  eb->refs++;
}

static inline int set_extent_buffer_uptodate(struct extent_buffer *eb)
{
  eb->flags |= EXTENT_UPTODATE;
  return 0;
}

static inline int clear_extent_buffer_uptodate(struct extent_io_tree *tree,
        struct extent_buffer *eb)
{
  eb->flags &= ~EXTENT_UPTODATE;
  return 0;
}

static inline int extent_buffer_uptodate(struct extent_buffer *eb)
{
  if (!eb || IS_ERR(eb))
    return 0;
  if (eb->flags & EXTENT_UPTODATE)
    return 1;
  return 0;
}

#endif

