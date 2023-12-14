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


#ifndef IMPALA_CODEGEN_MCJIT_MEM_MGR_H
#define IMPALA_CODEGEN_MCJIT_MEM_MGR_H

#include "thirdparty/llvm/SectionMemoryManager.h"

extern void *__dso_handle __attribute__ ((__visibility__ ("hidden")));

namespace impala {

/// Custom memory manager. It is needed for a couple of purposes.
///
/// We use it as a way to resolve references to __dso_handle in cross-compiled IR.
/// This uses the same approach as the legacy llvm JIT to handle __dso_handle. MCJIT
/// doesn't handle those for us: see LLVM issue 18062.
/// TODO: get rid of this by purging the cross-compiled IR of references to __dso_handle,
/// which come from global variables with destructors.
///
/// We also use it to track how much memory is allocated for compiled code.
class ImpalaMCJITMemoryManager : public SectionMemoryManager {
 public:
  ImpalaMCJITMemoryManager() : bytes_allocated_(0), bytes_tracked_(0){}

  virtual uint64_t getSymbolAddress(const std::string& name) override {
    if (name == "__dso_handle") return reinterpret_cast<uint64_t>(&__dso_handle);
    return SectionMemoryManager::getSymbolAddress(name);
  }

  virtual uint8_t* allocateCodeSection(uintptr_t size, unsigned alignment,
      unsigned section_id, llvm::StringRef section_name) override {
    bytes_allocated_ += size;
    return SectionMemoryManager::allocateCodeSection(
        size, alignment, section_id, section_name);
  }

  virtual uint8_t* allocateDataSection(uintptr_t size, unsigned alignment,
      unsigned section_id, llvm::StringRef section_name, bool is_read_only) override {
    bytes_allocated_ += size;
    return SectionMemoryManager::allocateDataSection(
        size, alignment, section_id, section_name, is_read_only);
  }

  int64_t bytes_allocated() const { return bytes_allocated_; }
  int64_t bytes_tracked() const { return bytes_tracked_; }
  void set_bytes_tracked(int64_t bytes_tracked) {
    DCHECK_LE(bytes_tracked, bytes_allocated_);
    bytes_tracked_ = bytes_tracked;
  }

 private:
  /// Total bytes allocated for the compiled code.
  int64_t bytes_allocated_;

  /// Total bytes already tracked by MemTrackers. <= 'bytes_allocated_'.
  /// Needed to release the correct amount from the MemTracker when done.
  int64_t bytes_tracked_;
};
}

#endif
