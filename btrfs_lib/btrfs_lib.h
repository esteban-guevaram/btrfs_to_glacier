#ifndef __BTRFS_LIB_H__
#define __BTRFS_LIB_H__

#include "rbtree.h"
#include "btrfs_list.h"
#include <dirent.h>

#ifdef TRACING
  #define TRACE(msg, ...) printf("[TRACE_%d_%s] " msg "\n", __LINE__, __func__, ## __VA_ARGS__)
#else
  #define TRACE(msg, ...) 
#endif  

#define ERR_TRACE(msg, ...) \
  fprintf(stderr, "[ERROR] %s:%d " msg "\n", __func__, __LINE__, ## __VA_ARGS__)

#define TRY_OR_DIE(fn_call) TRY_OR_DIE_HELPER1(fn_call, __COUNTER__ )
#define TRY_OR_DIE_HELPER1(fn_call, num) TRY_OR_DIE_HELPER2(fn_call, num)
#define TRY_OR_DIE_HELPER2(fn_call, num) TRY_OR_DIE_HELPER3(fn_call, __try_ ## num)

#define TRY_OR_DIE_HELPER3(fn_call, var_id)                           \
  int var_id = ( fn_call );                                           \
  if ( var_id ) {                                                     \
    fprintf(stderr, "[ERROR] %s:%d " #fn_call " : %s\n",              \
      __func__, __LINE__, strerror(errno));                           \
    return var_id;                                                    \
  }  

typedef int (subvol_visitor)(struct root_info* subvol, void* state);

int build_btrfs_subvols_from_path (const char* subvol, struct root_lookup* result);
int visit_subvols_in_tree (struct root_lookup* tree, subvol_visitor visitor, void* state);
int visit_copy_only_valid_subvol (struct root_info* subvol, void* state);

int btrfs_list_subvols(const char* dirpath, int fd, struct root_lookup *root_lookup);
int list_subvol_search(int fd, struct root_lookup *root_lookup);
int clean_deleted_subvols(struct root_lookup *root_lookup);
int list_subvol_fill_paths(int fd, struct root_lookup *root_lookup);
int list_subvol_resolve_root(int fd, struct root_lookup *root_lookup);
int list_subvol_complete_path(const char* dirpath, struct root_lookup *root_lookup);

int root_tree_insert(struct root_lookup *root_tree, struct root_info *ins);
void free_subvol_rb_tree(struct root_lookup *root_tree);
void clear_data_subvol(struct root_info *subvol);
int clone_subvol(struct root_info *source, struct root_info* dest);

int btrfs_open_dir(const char *path, DIR **dirstream, int verbose);
void close_file_or_dir(int fd, DIR *dirstream);

char* uuid_to_str(u8* uuid, char* result) ;
int complete_subvol_path(char* partial_path, const char* root_path, char* result);

int btrfs_list_get_path_rootid(int fd, u64 *treeid);
int resolve_root(struct root_lookup *rl, struct root_info *ri, u64 top_id);

#endif // __BTRFS_LIB_H__
