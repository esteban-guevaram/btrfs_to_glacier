#include <stdio.h>
#include <assert.h>
#include "btrfs_lib.h"

static void print_subvols(struct root_lookup *root_lookup);
static void test_subvols(struct root_lookup *root_lookup);

int main (int argc, char** argv) {
  int fd = -1;
  DIR *dirstream = NULL;
  struct root_lookup root_lookup;

  char *subvol = "/media/Belenus";
  if (argc > 1)
    subvol = argv[1];
  printf("Start building tree for %s\n", subvol);

  fd = btrfs_open_dir(subvol, &dirstream, 1);
  btrfs_list_subvols(fd, &root_lookup);

  print_subvols(&root_lookup);
  test_subvols(&root_lookup);

  close_file_or_dir(fd, dirstream);
  free_subvol_rb_tree(&root_lookup);
  printf("ALL OK\n");
}

static void print_subvols(struct root_lookup *root_lookup)
{
  struct rb_node *n;
  n = rb_first(&root_lookup->root);

  while (n) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);
    printf("name:%s, full_path:%s\n",
      entry->name, entry->full_path
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
    //struct root_info *entry;
    //entry = rb_entry(n, struct root_info, rb_node);
    n = rb_next(n);
  }

  assert(subvol_count > 0);
}

