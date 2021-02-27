#pragma once

struct MajorMinor {
  int major;
  int minor;
};

int is_cap_sys_admin();
void linux_kernel_version(struct MajorMinor* result);

