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

#include "util/hash-util.h"

// Define the hashing functions for llvm.  They are not used by anything that is
// cross compiled and without this, would get stripped by the clang optimizer.
#ifdef IR_COMPILE
using namespace impala;

extern "C"
uint32_t IrMurmurHash(const void* data, int32_t bytes, uint32_t hash) {
  return HashUtil::MurmurHash2_64(data, bytes, hash);
}

extern "C"
uint32_t IrCrcHash(const void* data, int32_t bytes, uint32_t hash) {
#if defined(__SSE4_2__) || defined(__aarch64__)
  return HashUtil::CrcHash(data, bytes, hash);
#else
  return HashUtil::FnvHash64to32(data, bytes, hash);
#endif
}
#else
#error "This file should only be compiled by clang."
#endif

