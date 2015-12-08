#include "kerncompat.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/statfs.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <linux/magic.h>
#include <btrfs/ioctl.h>

#include "btrfs_lib.h"
#include "ctree.h"

static int btrfs_list_get_path_rootid(int fd, u64 *treeid);
static int open_file_or_dir(const char *fname, DIR **dirstream, int open_flags);
static int add_root(struct root_lookup *root_lookup,
        u64 root_id, u64 ref_tree, u64 root_offset, u64 flags,
        u64 dir_id, char *name, int name_len, u64 ogen, u64 gen,
        time_t ot, void *uuid, void *puuid, void *ruuid);
static int update_root(struct root_lookup *root_lookup,
           u64 root_id, u64 ref_tree, u64 root_offset, u64 flags,
           u64 dir_id, char *name, int name_len, u64 ogen, u64 gen,
           time_t ot, void *uuid, void *puuid, void *ruuid);
static struct root_info *root_tree_search(struct root_lookup *root_tree, u64 root_id);
static int comp_entry_with_rootid(struct root_info *entry1,
          struct root_info *entry2, int is_descending);
static int root_tree_insert(struct root_lookup *root_tree, struct root_info *ins);
static void root_lookup_init(struct root_lookup *tree);
static int lookup_ino_path(int fd, struct root_info *ri);
static int resolve_root(struct root_lookup *rl, struct root_info *ri, u64 top_id);

typedef void (*rb_free_node)(struct rb_node *node);
static void rb_free_nodes(struct rb_root *root, rb_free_node free_node);
static void __free_root_info(struct rb_node *node);

int list_subvol_resolve_root(int fd, struct root_lookup *root_lookup) 
{
  int ret;
  u64 root_id;
  struct rb_node *n;
  n = rb_first(&root_lookup->root);

  btrfs_list_get_path_rootid(fd, &root_id);

  while (n) {
    struct root_info *entry;
    entry = rb_entry(n, struct root_info, rb_node);
    ret = resolve_root(root_lookup, entry, root_id);
    if (ret) return ret;
    n = rb_next(n);
  }

  return ret;
}

/*
 * Do the following checks before calling open_file_or_dir():
 * 1: path is in a btrfs filesystem
 * 2: path is a directory
 */
int btrfs_open_dir(const char *path, DIR **dirstream, int verbose)
{
  struct statfs stfs;
  struct stat st;
  int ret;

  if (statfs(path, &stfs) != 0) {
    if (verbose)
      fprintf(stderr,
        "ERROR: can't access '%s': %s\n",
        path, strerror(errno));
    return -1;
  }

  if (stfs.f_type != BTRFS_SUPER_MAGIC) {
    if (verbose)
      fprintf(stderr,
        "ERROR: not a btrfs filesystem: %s\n",
        path);
    return -2;
  }

  if (stat(path, &st) != 0) {
    if (verbose)
      fprintf(stderr,
        "ERROR: can't access '%s': %s\n",
        path, strerror(errno));
    return -1;
  }

  if (!S_ISDIR(st.st_mode)) {
    if (verbose)
      fprintf(stderr,
        "ERROR: not a directory: %s\n",
        path);
    return -3;
  }

  ret = open_file_or_dir(path, dirstream, O_RDWR);
  if (ret < 0) {
    if (verbose)
      fprintf(stderr,
        "ERROR: can't access '%s': %s\n",
        path, strerror(errno));
  }

  return ret;
}

int open_file_or_dir(const char *fname, DIR **dirstream, int open_flags)
{
  int ret;
  struct stat st;
  int fd;

  ret = stat(fname, &st);
  if (ret < 0) {
    return -1;
  }
  if (S_ISDIR(st.st_mode)) {
    *dirstream = opendir(fname);
    if (!*dirstream)
      return -1;
    fd = dirfd(*dirstream);
  } else if (S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) {
    fd = open(fname, open_flags);
  } else {
    /*
     * we set this on purpose, in case the caller output
     * strerror(errno) as success
     */
    errno = EINVAL;
    return -1;
  }
  if (fd < 0) {
    fd = -1;
    if (*dirstream) {
      closedir(*dirstream);
      *dirstream = NULL;
    }
  }
  return fd;
}

void close_file_or_dir(int fd, DIR *dirstream)
{
  if (dirstream)
    closedir(dirstream);
  else if (fd >= 0)
    close(fd);
}

int btrfs_list_subvols(int fd, struct root_lookup *root_lookup)
{
  int ret;

  ret = list_subvol_search(fd, root_lookup);
  if (ret) {
    fprintf(stderr, "ERROR: can't perform the search - %s\n",
        strerror(errno));
    return ret;
  }

  /*
   * now we have an rbtree full of root_info objects, but we need to fill
   * in their path names within the subvol that is referencing each one.
   */
  ret = list_subvol_fill_paths(fd, root_lookup);
  if (ret) {
    fprintf(stderr, "ERROR: can't fill paths - %s\n",
        strerror(errno));
    return ret;
  }

  ret = list_subvol_resolve_root(fd, root_lookup);
  if (ret) {
    fprintf(stderr, "ERROR: can't resolve full paths - %s\n",
        strerror(errno));
    return ret;
  }
  return ret;
}

int list_subvol_fill_paths(int fd, struct root_lookup *root_lookup)
{
  struct rb_node *n;

  n = rb_first(&root_lookup->root);
  while (n) {
    struct root_info *entry;
    int ret;
    entry = rb_entry(n, struct root_info, rb_node);
    ret = lookup_ino_path(fd, entry);
    if (ret && ret != -ENOENT)
      return ret;
    n = rb_next(n);
  }

  return 0;
}

int list_subvol_search(int fd, struct root_lookup *root_lookup)
{
  int ret;
  struct btrfs_ioctl_search_args args;
  struct btrfs_ioctl_search_key *sk = &args.key;
  struct btrfs_ioctl_search_header sh;
  struct btrfs_root_ref *ref;
  struct btrfs_root_item *ri;
  unsigned long off = 0;
  int name_len;
  char *name;
  u64 dir_id;
  u64 gen = 0;
  u64 ogen;
  u64 flags;
  int i;
  time_t t;
  u8 uuid[BTRFS_UUID_SIZE];
  u8 puuid[BTRFS_UUID_SIZE];
  u8 ruuid[BTRFS_UUID_SIZE];

  root_lookup_init(root_lookup);
  memset(&args, 0, sizeof(args));

  /* search in the tree of tree roots */
  sk->tree_id = 1;

  /*
   * set the min and max to backref keys.  The search will
   * only send back this type of key now.
   */
  sk->max_type = BTRFS_ROOT_BACKREF_KEY;
  sk->min_type = BTRFS_ROOT_ITEM_KEY;

  sk->min_objectid = BTRFS_FIRST_FREE_OBJECTID;

  /*
   * set all the other params to the max, we'll take any objectid
   * and any trans
   */
  sk->max_objectid = BTRFS_LAST_FREE_OBJECTID;
  sk->max_offset = (u64)-1;
  sk->max_transid = (u64)-1;

  /* just a big number, doesn't matter much */
  sk->nr_items = 4096;

  while(1) {
    ret = ioctl(fd, BTRFS_IOC_TREE_SEARCH, &args);
    if (ret < 0)
      return ret;
    /* the ioctl returns the number of item it found in nr_items */
    if (sk->nr_items == 0)
      break;

    off = 0;

    /*
     * for each item, pull the key out of the header and then
     * read the root_ref item it contains
     */
    for (i = 0; i < sk->nr_items; i++) {
      memcpy(&sh, args.buf + off, sizeof(sh));
      off += sizeof(sh);
      if (sh.type == BTRFS_ROOT_BACKREF_KEY) {
        ref = (struct btrfs_root_ref *)(args.buf + off);
        name_len = btrfs_stack_root_ref_name_len(ref);
        name = (char *)(ref + 1);
        dir_id = btrfs_stack_root_ref_dirid(ref);

        add_root(root_lookup, sh.objectid, sh.offset,
           0, 0, dir_id, name, name_len, 0, 0, 0,
           NULL, NULL, NULL);
      } else if (sh.type == BTRFS_ROOT_ITEM_KEY) {
        ri = (struct btrfs_root_item *)(args.buf + off);
        gen = btrfs_root_generation(ri);
        flags = btrfs_root_flags(ri);
        if(sh.len >
           sizeof(struct btrfs_root_item_v0)) {
          t = btrfs_stack_timespec_sec(&ri->otime);
          ogen = btrfs_root_otransid(ri);
          memcpy(uuid, ri->uuid, BTRFS_UUID_SIZE);
          memcpy(puuid, ri->parent_uuid, BTRFS_UUID_SIZE);
          memcpy(ruuid, ri->received_uuid, BTRFS_UUID_SIZE);
        } else {
          t = 0;
          ogen = 0;
          memset(uuid, 0, BTRFS_UUID_SIZE);
          memset(puuid, 0, BTRFS_UUID_SIZE);
          memset(ruuid, 0, BTRFS_UUID_SIZE);
        }

        add_root(root_lookup, sh.objectid, 0,
           sh.offset, flags, 0, NULL, 0, ogen,
           gen, t, uuid, puuid, ruuid);
      }

      off += sh.len;

      /*
       * record the mins in sk so we can make sure the
       * next search doesn't repeat this root
       */
      sk->min_objectid = sh.objectid;
      sk->min_type = sh.type;
      sk->min_offset = sh.offset;
    }
    sk->nr_items = 4096;
    sk->min_offset++;
    if (!sk->min_offset)	/* overflow */
      sk->min_type++;
    else
      continue;

    if (sk->min_type > BTRFS_ROOT_BACKREF_KEY) {
      sk->min_type = BTRFS_ROOT_ITEM_KEY;
      sk->min_objectid++;
    } else
      continue;

    if (sk->min_objectid > sk->max_objectid)
      break;
  }

  return 0;
}

/*
 * add_root - update the existed root, or allocate a new root and insert it
 *	      into the lookup tree.
 * root_id: object id of the root
 * ref_tree: object id of the referring root.
 * root_offset: offset value of the root'key
 * dir_id: inode id of the directory in ref_tree where this root can be found.
 * name: the name of root_id in that directory
 * name_len: the length of name
 * ogen: the original generation of the root
 * gen: the current generation of the root
 * ot: the original time(create time) of the root
 * uuid: uuid of the root
 * puuid: uuid of the root parent if any
 * ruuid: uuid of the received subvol, if any
 */
static int add_root(struct root_lookup *root_lookup,
        u64 root_id, u64 ref_tree, u64 root_offset, u64 flags,
        u64 dir_id, char *name, int name_len, u64 ogen, u64 gen,
        time_t ot, void *uuid, void *puuid, void *ruuid)
{
  struct root_info *ri;
  int ret;

  ret = update_root(root_lookup, root_id, ref_tree, root_offset, flags,
        dir_id, name, name_len, ogen, gen, ot,
        uuid, puuid, ruuid);
  if (!ret)
    return 0;

  ri = calloc(1, sizeof(*ri));
  if (!ri) {
    printf("memory allocation failed\n");
    exit(1);
  }
  ri->root_id = root_id;

  if (name && name_len > 0) {
    ri->name = malloc(name_len + 1);
    if (!ri->name) {
      fprintf(stderr, "memory allocation failed\n");
      exit(1);
    }
    strncpy(ri->name, name, name_len);
    ri->name[name_len] = 0;
  }
  if (ref_tree)
    ri->ref_tree = ref_tree;
  if (dir_id)
    ri->dir_id = dir_id;
  if (root_offset)
    ri->root_offset = root_offset;
  if (flags)
    ri->flags = flags;
  if (gen)
    ri->gen = gen;
  if (ogen)
    ri->ogen = ogen;
  if (!ri->ogen && root_offset)
    ri->ogen = root_offset;
  if (ot)
    ri->otime = ot;

  if (uuid)
    memcpy(&ri->uuid, uuid, BTRFS_UUID_SIZE);

  if (puuid)
    memcpy(&ri->puuid, puuid, BTRFS_UUID_SIZE);

  if (ruuid)
    memcpy(&ri->ruuid, ruuid, BTRFS_UUID_SIZE);

  ret = root_tree_insert(root_lookup, ri);
  if (ret) {
    printf("failed to insert tree %llu\n", (unsigned long long)root_id);
    exit(1);
  }
  return 0;
}

static int update_root(struct root_lookup *root_lookup,
           u64 root_id, u64 ref_tree, u64 root_offset, u64 flags,
           u64 dir_id, char *name, int name_len, u64 ogen, u64 gen,
           time_t ot, void *uuid, void *puuid, void *ruuid)
{
  struct root_info *ri;

  ri = root_tree_search(root_lookup, root_id);
  if (!ri || ri->root_id != root_id)
    return -ENOENT;
  if (name && name_len > 0) {
    free(ri->name);

    ri->name = malloc(name_len + 1);
    if (!ri->name) {
      fprintf(stderr, "memory allocation failed\n");
      exit(1);
    }
    strncpy(ri->name, name, name_len);
    ri->name[name_len] = 0;
  }
  if (ref_tree)
    ri->ref_tree = ref_tree;
  if (root_offset)
    ri->root_offset = root_offset;
  if (flags)
    ri->flags = flags;
  if (dir_id)
    ri->dir_id = dir_id;
  if (gen)
    ri->gen = gen;
  if (ogen)
    ri->ogen = ogen;
  if (!ri->ogen && root_offset)
    ri->ogen = root_offset;
  if (ot)
    ri->otime = ot;
  if (uuid)
    memcpy(&ri->uuid, uuid, BTRFS_UUID_SIZE);
  if (puuid)
    memcpy(&ri->puuid, puuid, BTRFS_UUID_SIZE);
  if (ruuid)
    memcpy(&ri->ruuid, ruuid, BTRFS_UUID_SIZE);

  return 0;
}

/*
 * find a given root id in the tree.  We return the smallest one,
 * rb_next can be used to move forward looking for more if required
 */
static struct root_info *root_tree_search(struct root_lookup *root_tree, u64 root_id)
{
  struct rb_node *n = root_tree->root.rb_node;
  struct root_info *entry;
  struct root_info tmp;
  int ret;

  tmp.root_id = root_id;

  while(n) {
    entry = rb_entry(n, struct root_info, rb_node);

    ret = comp_entry_with_rootid(&tmp, entry, 0);
    if (ret < 0)
      n = n->rb_left;
    else if (ret > 0)
      n = n->rb_right;
    else
      return entry;
  }
  return NULL;
}

static int comp_entry_with_rootid(struct root_info *entry1,
          struct root_info *entry2, int is_descending)
{
  int ret;

  if (entry1->root_id > entry2->root_id)
    ret = 1;
  else if (entry1->root_id < entry2->root_id)
    ret = -1;
  else
    ret = 0;

  return is_descending ? -ret : ret;
}

/*
 * insert a new root into the tree.  returns the existing root entry
 * if one is already there.  Both root_id and ref_tree are used
 * as the key
 */
static int root_tree_insert(struct root_lookup *root_tree, struct root_info *ins)
{
  struct rb_node **p = &root_tree->root.rb_node;
  struct rb_node * parent = NULL;
  struct root_info *curr;
  int ret;

  while(*p) {
    parent = *p;
    curr = rb_entry(parent, struct root_info, rb_node);

    ret = comp_entry_with_rootid(ins, curr, 0);
    if (ret < 0)
      p = &(*p)->rb_left;
    else if (ret > 0)
      p = &(*p)->rb_right;
    else
      return -EEXIST;
  }

  rb_link_node(&ins->rb_node, parent, p);
  rb_insert_color(&ins->rb_node, &root_tree->root);
  return 0;
}

static void root_lookup_init(struct root_lookup *tree)
{
  tree->root.rb_node = NULL;
}

/*
 * for a single root_info, ask the kernel to give us a path name
 * inside it's ref_root for the dir_id where it lives.
 *
 * This fills in root_info->path with the path to the directory and and
 * appends this root's name.
 */
static int lookup_ino_path(int fd, struct root_info *ri)
{
  struct btrfs_ioctl_ino_lookup_args args;
  int ret, e;

  if (ri->path)
    return 0;

  if (!ri->ref_tree)
    return -ENOENT;

  memset(&args, 0, sizeof(args));
  args.treeid = ri->ref_tree;
  args.objectid = ri->dir_id;

  ret = ioctl(fd, BTRFS_IOC_INO_LOOKUP, &args);
  e = errno;
  if (ret) {
    if (e == ENOENT) {
      ri->ref_tree = 0;
      return -ENOENT;
    }
    fprintf(stderr, "ERROR: Failed to lookup path for root %llu - %s\n",
      (unsigned long long)ri->ref_tree,
      strerror(e));
    return ret;
  }

  if (args.name[0]) {
    /*
     * we're in a subdirectory of ref_tree, the kernel ioctl
     * puts a / in there for us
     */
    ri->path = malloc(strlen(ri->name) + strlen(args.name) + 1);
    if (!ri->path) {
      perror("malloc failed");
      exit(1);
    }
    strcpy(ri->path, args.name);
    strcat(ri->path, ri->name);
  } else {
    /* we're at the root of ref_tree */
    ri->path = strdup(ri->name);
    if (!ri->path) {
      perror("strdup failed");
      exit(1);
    }
  }
  return 0;
}

/*
 * for a given root_info, search through the root_lookup tree to construct
 * the full path name to it.
 *
 * This can't be called until all the root_info->path fields are filled
 * in by lookup_ino_path
 */
static int resolve_root(struct root_lookup *rl, struct root_info *ri, u64 top_id)
{
  char *full_path = NULL;
  int len = 0;
  struct root_info *found;

  /*
   * we go backwards from the root_info object and add pathnames
   * from parent directories as we go.
   */
  found = ri;
  while (1) {
    char *tmp;
    u64 next;
    int add_len;

    /*
     * ref_tree = 0 indicates the subvolumes
     * has been deleted.
     */
    if (!found->ref_tree) {
      free(full_path);
      return -ENOENT;
    }

    add_len = strlen(found->path);

    if (full_path) {
      /* room for / and for null */
      tmp = malloc(add_len + 2 + len);
      if (!tmp) {
        perror("malloc failed");
        exit(1);
      }
      memcpy(tmp + add_len + 1, full_path, len);
      tmp[add_len] = '/';
      memcpy(tmp, found->path, add_len);
      tmp [add_len + len + 1] = '\0';
      free(full_path);
      full_path = tmp;
      len += add_len + 1;
    } else {
      full_path = strdup(found->path);
      len = add_len;
    }
    if (!ri->top_id)
      ri->top_id = found->ref_tree;

    next = found->ref_tree;
    if (next == top_id)
      break;
    /*
    * if the ref_tree = BTRFS_FS_TREE_OBJECTID,
    * we are at the top
    */
    if (next == BTRFS_FS_TREE_OBJECTID)
      break;
    /*
    * if the ref_tree wasn't in our tree of roots, the
    * subvolume was deleted.
    */
    found = root_tree_search(rl, next);
    if (!found) {
      free(full_path);
      return -ENOENT;
    }
  }

  ri->full_path = full_path;

  return 0;
}

static int btrfs_list_get_path_rootid(int fd, u64 *treeid)
{
  int  ret;
  struct btrfs_ioctl_ino_lookup_args args;

  memset(&args, 0, sizeof(args));
  args.objectid = BTRFS_FIRST_FREE_OBJECTID;

  ret = ioctl(fd, BTRFS_IOC_INO_LOOKUP, &args);
  if (ret < 0) {
    fprintf(stderr,
      "ERROR: can't perform the search - %s\n",
      strerror(errno));
    return ret;
  }
  *treeid = args.treeid;
  return 0;
}

static void __free_root_info(struct rb_node *node)
{
  struct root_info *ri;

  ri = rb_entry(node, struct root_info, rb_node);
  free(ri->name);
  free(ri->path);
  free(ri->full_path);
  free(ri);
}

void free_subvol_rb_tree(struct root_lookup *root_tree)
{
  rb_free_nodes(&root_tree->root, __free_root_info);
}

void rb_free_nodes(struct rb_root *root, rb_free_node free_node)
{
  struct rb_node *node;

  while ((node = rb_first(root))) {
    rb_erase(node, root);
    free_node(node);
  }
}

