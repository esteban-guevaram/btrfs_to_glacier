#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <common.h>
#include <logger.h>

#include <btrfsutil.h>

int is_directory(const char* path) {
  const size_t buf_len = 256;
  char buf[buf_len];
  int len = strnlen(path, buf_len-2);
  if (len < 1) return 0;

  strncpy(buf, path, buf_len);
  if (buf[len-1] != '/') strncat(buf, "/", buf_len);
  buf[buf_len-1] = '\0';
  // Another option to using `stat`
  //return !access(buf, F_OK);
  struct stat s;
  if(stat(buf, &s) == -1) {
    LOG_WARN("stat failed (path=%s) [%d: %s]", buf, errno, strerror(errno));
    exit(1);
  }
  return S_ISDIR(s.st_mode);
}

int main(int argc, char** argv) {
  LOG_INFO("Testing btrfs-prog linking");
  TEST_ASSERT_MSG(argc == 2, "USAGE: test_btrfs_prog_integration SUBVOL_PATH");
  const char* subvol_path = argv[1];
  TEST_ASSERT_MSG(is_directory(subvol_path), "SUBVOL_PATH does not exist");

  struct btrfs_util_subvolume_info subvol;
  enum btrfs_util_error stx = btrfs_util_subvolume_info(subvol_path, 0, &subvol);
  TEST_ASSERT_MSG(stx == BTRFS_UTIL_OK, "Failed to get subvolume information");
  char buf[256];
  LOG_INFO("path='%s', UUID='%s'", subvol_path, uuid_to_str(subvol.uuid, buf, sizeof(buf)));
  LOG_INFO("All done !!");
  return 0;
}

