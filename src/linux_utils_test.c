#include <sys/capability.h>
#include <sys/types.h>

#include <common.h>
#include <logger.h>

#include <linux_utils.h>

int main(int argc, char** argv) {
  LOG_INFO("Testing linux_utils functions");
  is_cap_sys_admin();
  return 0;
}

