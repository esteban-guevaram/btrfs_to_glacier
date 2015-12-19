#include <libgen.h>
#include "kerncompat.h"
#include "btrfs_lib.h"
#include "ctree.h"

static void copy_into_and_discard(char** dest, const char *src);
static int reached_fs_root(const char* dir, const char* last);

int list_subvol_resolve_root(int fd, struct root_lookup *root_lookup) 
{
  u64 root_id;
  struct rb_node *n = rb_first(&root_lookup->root);

  TRY_OR_DIE( btrfs_list_get_path_rootid(fd, &root_id) );

  while (n) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);
    TRY_OR_DIE( resolve_root(root_lookup, entry, root_id) );
    n = rb_next(n);
  }
  return 0;
}

char* uuid_to_str(u8* uuid, char *result) 
{
  if (uuid == NULL || *uuid == 0) {
    *result = '\0';
  }
  else {
    for(u64 i=0; i<BTRFS_UUID_SIZE; ++i)
      snprintf(result + 2*i, 3, "%.2X", uuid[i]);
    result[2 * BTRFS_UUID_SIZE] = '\0';  
  }
  return result;
}

int list_subvol_complete_path(const char* dirpath, struct root_lookup *root_lookup) 
{
  char buffer[256];
  struct rb_node *n = rb_first(&root_lookup->root);

  while (n) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);

    TRY_OR_DIE( complete_subvol_path(entry->full_path, dirpath, buffer) );

    if (buffer[0])
      copy_into_and_discard(&entry->full_path, buffer);
    n = rb_next(n);
  }
  return 0;
}

int complete_subvol_path(char* partial_path, const char* root_path, char* result)
{
  DIR *dirstream = NULL;
  char buffer[256], tmp_dir[256], last_dir[256];

  result[0] = '\0';
  strncpy(tmp_dir, root_path, 256);
  basename(tmp_dir); // We rely on basename side-effect when path ends with '/'

  for(; !reached_fs_root(tmp_dir, last_dir);
      strncpy(last_dir, tmp_dir, 256), dirname(tmp_dir))
  {
    if (tmp_dir[0] != '/' || tmp_dir[1] != '\0')
      snprintf(buffer, 256, "%s/%s", tmp_dir, partial_path);
    else  
      snprintf(buffer, 256, "/%s", partial_path);

    int fd = btrfs_open_dir(buffer, &dirstream, false);
    if (fd > 0) {
      strncpy(result, buffer, 256);
      close_file_or_dir(fd, dirstream);
    }  
    TRACE("Checking '%s' => %d", buffer, fd);
  }

  return result[0] == '\0';
}

static int reached_fs_root(const char* dir, const char* last) {
  assert(dir[0] != '\0');
  return strncmp(dir, last, 256) == 0;
}

static void copy_into_and_discard(char** dest, const char *src) {
  free(*dest);
  int len = strnlen(src, 255) + 1;
  *dest = calloc(sizeof(char), len);
  memcpy(*dest, src, len);
}

