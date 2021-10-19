#pragma once
#include <stdint.h>

struct MajorMinor {
  int major;
  int minor;
};

void set_euid_or_die(int new_euid);
int is_cap_sys_admin();
void linux_kernel_version(struct MajorMinor* result);

