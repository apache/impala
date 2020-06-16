// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_COMMON_COMPILER_UTIL_H
#define IMPALA_COMMON_COMPILER_UTIL_H

/// Compiler hint that this branch is likely or unlikely to
/// be taken. Take from the "What all programmers should know
/// about memory" paper.
/// example: if (LIKELY(size > 0)) { ... }
/// example: if (UNLIKELY(!status.ok())) { ... }
#ifdef LIKELY
#undef LIKELY
#endif

#ifdef UNLIKELY
#undef UNLIKELY
#endif

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define PREFETCH(addr) __builtin_prefetch(addr)

/// Force inlining. The 'inline' keyword is treated by most compilers as a hint,
/// not a command. This should be used sparingly for cases when either the function
/// needs to be inlined for a specific reason or the compiler's heuristics make a bad
/// decision, e.g. not inlining a small function on a hot path.
#define ALWAYS_INLINE __attribute__((always_inline))

/// Clang is pedantic about __restrict__ (e.g. never allows calling a non-__restrict__)
/// member function from a __restrict__-ed memory function and has some apparent bugs
/// (e.g. can't convert a __restrict__ reference to a const& __restrict__ reference).
/// Just disable it.
#ifdef __clang__
#define RESTRICT
#else
#define RESTRICT __restrict__
#endif

// Suppress warnings when ignoring the return value from a function annotated with
// WARN_UNUSED_RESULT. Based on ignore_result() in gutil/basictypes.h.
template<typename T>
inline void discard_result(const T&) {}

namespace impala {

/// The size of an L1 cache line in bytes on x86-64.
constexpr int CACHE_LINE_SIZE = 64;
}
#endif
