#pragma once

#include <stddef.h>
#include <stdint.h>

#define __STRINGIFY__(arg) #arg
#define STRINGIFY(arg) __STRINGIFY__(arg)

#define __PASTIFY__(arg1, arg2) arg1 ## arg2
#define PASTIFY(arg1, arg2) __PASTIFY__(arg1, arg2)

#ifndef __LEVEL_ASSERT__
  #define __LEVEL_ASSERT__ 1
#endif

#if __LEVEL_ASSERT__ > 0
  #define ASSERT(exp, msg, ...) \
    if (!(exp)) { __LOG__("ASSERT", msg " : " #exp ,##__VA_ARGS__); __my_assert__(!!(exp)); }
#else
  #define ASSERT(exp,msg, ...)
#endif

#define TEST_ASSERT(exp, msg, ...) \
  if (!(exp)) { __LOG__("TEST_ASSERT", msg " : " #exp ,##__VA_ARGS__); exit(1); }

#define STATIC_ASSERT(COND,MSG) \
  typedef char __static_assert_##MSG[(COND)?1:-1]

void __my_assert__(int condition);

#define is_null_or_empty(str) (str) == NULL || *(str) == '\0'

#define DIE_OR_ASSIGN(result, expr) \
  errno = 0; int result = expr; if(result < 0) { LOG("[%d: %s] " #expr " < 0", errno, strerror(errno)); exit(1); }

/////////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __clang__
  #define __COMPILER__ clang
#elif defined(__GNUC__)
  #define __COMPILER__ GCC
#else
  #define __COMPILER__ CouldNotDetectCompiler
#endif

#define IGNORE_WARNING_PUSH(flag) \
  _Pragma(STRINGIFY(__COMPILER__ diagnostic push)) \
  _Pragma(STRINGIFY(__COMPILER__ diagnostic ignored flag))

#define IGNORE_WARNING_POP \
  _Pragma(STRINGIFY(__COMPILER__ diagnostic pop)) \

/////////////////////////////////////////////////////////////////////////////////////////////////

// Copies a string representation of `uuid` into `result` which is expected to be at least `buf_len` bytes.
char* uuid_to_str(uint8_t* uuid, char *result, size_t buf_len);

