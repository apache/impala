// Copyright 2016 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_CODEGEN_MCJIT_MEM_MGR_H
#define IMPALA_CODEGEN_MCJIT_MEM_MGR_H

#include <llvm/ExecutionEngine/SectionMemoryManager.h>

extern void *__dso_handle __attribute__ ((__visibility__ ("hidden")));

namespace impala {

/// Custom memory manager to resolve references to __dso_handle in cross-compiled IR.
/// This uses the same approach as the legacy llvm JIT to handle __dso_handle. MCJIT
/// doesn't handle those for us: see LLVM issue 18062.
/// TODO: get rid of this by purging the cross-compiled IR of references to __dso_handle,
/// which come from global variables with destructors.
class ImpalaMCJITMemoryManager : public llvm::SectionMemoryManager {
 public:
  virtual uint64_t getSymbolAddress(const std::string& name) override {
    if (name == "__dso_handle") return reinterpret_cast<uint64_t>(&__dso_handle);
    return SectionMemoryManager::getSymbolAddress(name);
  }
};

}

#endif
