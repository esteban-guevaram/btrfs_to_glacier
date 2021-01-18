#include <sys/capability.h>
#include <sys/types.h>

#include <common.h>

int is_cap_sys_admin() {
  CALL_POSIX_CHECK(cap_t caps = cap_get_proc(),
                   caps != NULL);
  //LOG_DEBUG("cap_t for process : %s", cap_to_text(caps, NULL));
  cap_flag_value_t enabled;
  CALL_POSIX_CHECK(int get_res = cap_get_flag(caps, CAP_SYS_ADMIN, CAP_EFFECTIVE, &enabled),
                   get_res == 0);
  CALL_POSIX_CHECK(int free_res = cap_free(caps),
                   free_res == 0);
  //LOG_DEBUG("is_cap_sys_admin : %d", enabled == CAP_SET);
  return enabled == CAP_SET;
}

