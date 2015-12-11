#ifndef __BTRFS_LIB_H__
#define __BTRFS_LIB_H__

#include "rbtree.h"
#include "btrfs_list.h"
#include <dirent.h>

#define TRACE(msg, ...) printf("[TRACE]" msg "\n", ## __VA_ARGS__)

#define TRY_OR_DIE(fn_call) TRY_OR_DIE_HELPER1(fn_call, __COUNTER__ )
#define TRY_OR_DIE_HELPER1(fn_call, num) TRY_OR_DIE_HELPER2(fn_call, num)
#define TRY_OR_DIE_HELPER2(fn_call, num) TRY_OR_DIE_HELPER3(fn_call, __try_ ## num)

#define TRY_OR_DIE_HELPER3(fn_call, var_id)                           \
  int var_id = ( fn_call );                                           \
  if ( var_id ) {                                                     \
    fprintf(stderr, "ERROR: " #fn_call " : %s\n", strerror(errno));   \
    exit( var_id );                                                   \
  }  

int btrfs_list_subvols(const char* dirpath, int fd, struct root_lookup *root_lookup);
int list_subvol_search(int fd, struct root_lookup *root_lookup);
int list_subvol_fill_paths(int fd, struct root_lookup *root_lookup);
int list_subvol_resolve_root(int fd, struct root_lookup *root_lookup);
int list_subvol_complete_path(const char* dirpath, struct root_lookup *root_lookup);
void free_subvol_rb_tree(struct root_lookup *root_tree);

int btrfs_open_dir(const char *path, DIR **dirstream, int verbose);
void close_file_or_dir(int fd, DIR *dirstream);

char* uuid_to_str(u8* uuid, char* result) ;
int complete_subvol_path(char* partial_path, const char* root_path, char* result);

int btrfs_list_get_path_rootid(int fd, u64 *treeid);
int resolve_root(struct root_lookup *rl, struct root_info *ri, u64 top_id);

#endif // __BTRFS_LIB_H__
