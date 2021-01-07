#pragma once

#include <stdio.h>

#ifndef __LEVEL_LOG__
  #define __LEVEL_LOG__ 4
#endif

#define __LOG__(level, format, ...) \
  printf("[%s]%s:%d - " format "\n", level, __FILE__,__LINE__, ##__VA_ARGS__)
  //printf("[%s]%s:%d - " format "\n", level, __func__,__LINE__, ##__VA_ARGS__)

#ifdef __LOG_TRACE__
  #define LOG_TRACE(format, ...) __LOG__("TRCE",format,##__VA_ARGS__)
#else
  #define LOG_TRACE(format, ...)
#endif

#if __LEVEL_LOG__ > 3
  #define LOG_DEBUG(format, ...) __LOG__("DEBG",format,##__VA_ARGS__)
#else
  #define LOG_DEBUG(format, ...)
#endif

#if __LEVEL_LOG__ > 2
  #define LOG_INFO(format, ...) __LOG__("INFO",format,##__VA_ARGS__)
#else
  #define LOG_INFO(format, ...)
#endif

#if __LEVEL_LOG__ > 1
  #define LOG_WARN(format, ...) __LOG__("WARN",format,##__VA_ARGS__)
#else
  #define LOG_WARN(format, ...)
#endif

#if __LEVEL_LOG__ > 0
  #define LOG_ERROR(format, ...) __LOG__("FATL",format,##__VA_ARGS__)
#else
  #define LOG_ERROR(format, ...)
#endif


