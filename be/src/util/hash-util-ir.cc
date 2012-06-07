// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/hash-util.h"

// Define the hashing functions for llvm.  They are not used by anything that is
// cross compiled and without this, would get stripped by the clang optimizer.
#ifdef IR_COMPILE
extern "C"
uint32_t IrFvnHash(const void* data, int32_t bytes, uint32_t hash) {
  return HashUtil::FvnHash(data, bytes, hash);
}

extern "C"
uint32_t IrCrcHash(const void* data, int32_t bytes, uint32_t hash) {
#ifdef __SSE4_2__
  return HashUtil::CrcHash(data, bytes, hash);
#else
  return HashUtil::FvnHash(data, bytes, hash);
#endif
}
#else
#error "This file should only be compiled by clang."
#endif

