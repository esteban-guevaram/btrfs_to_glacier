#include <stdio.h>
#include <time.h>
#include <assert.h>
#include "btrfs_lib.h"

static void print_subvols(struct root_lookup *root_lookup);
static void test_subvols(struct root_lookup *root_lookup);

int main (int argc, char** argv) {
  struct root_lookup root_lookup;

  char *subvol = "/media/Belenus";
  if (argc > 1)
    subvol = argv[1];

  build_btrfs_subvols_from_path(subvol, &root_lookup);

  print_subvols(&root_lookup);
  test_subvols(&root_lookup);

  free_subvol_rb_tree(&root_lookup);
  printf("ALL OK\n");
}

static void print_subvols(struct root_lookup *root_lookup)
{
  char buffer[256];
  struct rb_node *n;
  n = rb_first(&root_lookup->root);

  while (n) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);
    printf("\nname:%s\n - full_path:%s\n - uuid:%s\n - par_uuid:%s\n"
           " - offset:%llu\n - top:%llu\n - root:%llu\n - time:%s\n",
      entry->name, entry->full_path,
      uuid_to_str(entry->uuid, buffer), uuid_to_str(entry->puuid, buffer+BTRFS_UUID_SIZE*2+1),
      entry->root_offset, entry->top_id, entry->root_id,
      ctime(&entry->otime)
    );  
    n = rb_next(n);
  }
}

static void test_subvols(struct root_lookup *root_lookup)
{
  int subvol_count = 0;
  struct rb_node *n;
  n = rb_first(&root_lookup->root);

  for(; n; subvol_count++) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);
    assert(entry->uuid[0]);
    n = rb_next(n);
  }

  assert(subvol_count > 0);
}

