#include <sys/capability.h>
#include <sys/types.h>

#include <common.h>
#include <logger.h>

#include <linux_utils.h>

int main(int argc, char** argv) {
  LOG_INFO("Testing linux_utils functions");
  is_cap_sys_admin();
  struct MajorMinor kver = {0, 0};
  linux_kernel_version(&kver);
  TEST_ASSERT(kver.major != 0);
  TEST_ASSERT(kver.minor != 0);
  return 0;
}

