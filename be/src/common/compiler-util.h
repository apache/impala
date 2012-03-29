// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_COMPILER_UTIL_H
#define IMPALA_COMMON_COMPILER_UTIL_H

// Compiler hint that this branch is likely or unlikely to 
// be taken. Take from the "What all programmers should know
// about memory" paper.
// example: if (LIKELY(size > 0)) { ... }
// example: if (UNLIKELY(!status.ok())) { ... }
#define LIKELY(expr) __builtin_expect(!!(expr), 0)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 1)

#define PREFETCH(addr) __builtin_prefetch(addr)

#endif

