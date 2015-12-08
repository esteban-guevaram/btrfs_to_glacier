#ifndef __BTRFS_LIB_H__
#define __BTRFS_LIB_H__

#include "rbtree.h"
#include "btrfs_list.h"
#include <dirent.h>

int btrfs_list_subvols(int fd, struct root_lookup *root_lookup);
int list_subvol_search(int fd, struct root_lookup *root_lookup);
int list_subvol_fill_paths(int fd, struct root_lookup *root_lookup);
int list_subvol_resolve_root(int fd, struct root_lookup *root_lookup);
void free_subvol_rb_tree(struct root_lookup *root_tree);

int btrfs_open_dir(const char *path, DIR **dirstream, int verbose);
void close_file_or_dir(int fd, DIR *dirstream);

#endif // __BTRFS_LIB_H__
