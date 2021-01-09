#include <common.h>

#include <sys/resource.h>

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <dlfcn.h>

#include <logger.h>

#include <btrfsutil.h>

IGNORE_WARNING_PUSH("-Wunused-function")
// Use this to set breakpoints to trap assertion violations
void __my_assert__(int condition) {
  abort();
}
IGNORE_WARNING_POP

char* uuid_to_str(uint8_t* uuid, char *result, size_t buf_len) {
  const struct btrfs_util_subvolume_info subvol;
  if (uuid == NULL || sizeof(subvol.uuid) > 2*buf_len + 1) {
    *result = '\0';
  }
  else {
    for(int i=0; i<sizeof(subvol.uuid); ++i)
      snprintf(result + 2*i, 3, "%.2x", uuid[i]);
    result[2 * sizeof(subvol.uuid)] = '\0';  
  }
  return result;
}

