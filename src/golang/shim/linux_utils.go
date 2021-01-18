package shim

/*
#include <stdlib.h>
#include <stdio.h>
#include <linux_utils.h>
*/
import "C"

func IsCapSysAdmin() bool {
  // can only cast C.int to int
  res := int(C.is_cap_sys_admin())
  return (res != 0)
}

