#ifndef UTIL_ASAN_H_
#define UTIL_ASAN_H_

// Compatibility with non-clang compilers.
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(address_sanitizer)
#define ASAN_NO_INSTRUMENTATION __attribute__((no_sanitize("address")))
#else
#define ASAN_NO_INSTRUMENTATION
#endif

#if defined (ADDRESS_SANITIZER) && __clang_major__ >= 3 && __clang_minor__ >= 7
#include <sanitizer/lsan_interface.h>
#define IGNORE_LEAKING_OBJECT(p) __lsan_ignore_object(p)
#else
#define IGNORE_LEAKING_OBJECT(p)
#endif


#endif //UTIL_ASAN_H_
