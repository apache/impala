// Copyright 2003 Google Inc.
//
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
//
// All Rights Reserved.
//
//
// Implementation of PauseCPU.  This file should not be included
// directly.  Clients should instead include "base/atomicops.h".

#ifndef GUTIL_YIELD_PROCESSOR_H_
#define GUTIL_YIELD_PROCESSOR_H_

namespace base {
namespace subtle {

inline void PauseCPU() {
#if defined(__x86_64__)
  // Issue the x86 "pause" instruction, which tells the CPU that we
  // are in a spinlock wait loop and should allow other hyperthreads
  // to run, not speculate memory access, etc.
  __asm__ __volatile__("pause" : : : "memory");
#elif defined(__aarch64__)
  // A "yield" instruction in aarch64 is essentially a nop, and does not cause
  // enough delay to help backoff. "isb" is a barrier that, especially inside a
  // loop, creates a small delay without consuming ALU resources. Experiments
  // show that adding the isb instruction improves stability and reduces result
  // jitter. Adding more delay than a single isb reduces performance.
  __asm__ __volatile__("isb" : : : "memory");
#else
  // PauseCPU is not defined for other architectures.
#endif
}

}  // namespace subtle
}  // namespace base

#endif  // GUTIL_YIELD_PROCESSOR_H_
