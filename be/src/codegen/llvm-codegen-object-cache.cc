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

#include "codegen/llvm-codegen-object-cache.h"

#include <llvm/IR/Module.h>

using std::string;
using std::stringstream;

namespace impala {

void CodeGenObjectCache::notifyObjectCompiled(
    const llvm::Module* M, llvm::MemoryBufferRef ObjBuffer) {
  DCHECK(M != nullptr);
  // Save the compiled object in the memory cache, with the module ID as the key.
  // In our scenario, we anticipate having only one module per execution engine.
  // Consequently, each CodeGenObjectCache should hold just one module.
  // If a module with a different ID is encountered, a warning is logged for
  // further investigation.
  const string& module_id = M->getModuleIdentifier();
  std::lock_guard<std::mutex> l(mutex_);
  if (key_.empty()) {
    LOG(INFO) << "Insert module id " << module_id << " in CodeGenObjectCache " << this
              << " with size " << ObjBuffer.getBuffer().size() << " bytes";
    key_ = module_id;
    // The Buffer Id doesn't seem to serve a purpose in our case, and skipping it
    // can conserve memory.
    cached_obj_ =
        llvm::MemoryBuffer::getMemBufferCopy(ObjBuffer.getBuffer(), "" /*Buffer Id*/);
  } else if (key_ != module_id) {
    stringstream ss;
    ss << "Inserting a different module in CodeGenObjectCache " << this
       << ". Old module id is " << module_id << ", and new module id is " << key_;
    DCHECK(false) << ss.str();
    LOG(WARNING) << ss.str();
  }
}

std::unique_ptr<llvm::MemoryBuffer> CodeGenObjectCache::getObject(const llvm::Module* M) {
  DCHECK(M != nullptr);
  // Return a copy of the cached object containing the compiled module with the specified
  // ID. If the ID doesn't match the one in the cached object, a warning is logged for
  // further investigation.
  const string& module_id = M->getModuleIdentifier();
  std::lock_guard<std::mutex> l(mutex_);
  if (module_id == key_) {
    LOG(INFO) << "Object for module id " << module_id << " loaded from cache with size "
              << cached_obj_->getMemBufferRef().getBufferSize() << " bytes";
    return llvm::MemoryBuffer::getMemBufferCopy(
        cached_obj_->getMemBufferRef().getBuffer());
  } else if (!key_.empty()) {
    stringstream ss;
    ss << "Object for module id " << module_id << " can't be loaded from cache."
       << " Existing module id is " << key_;
    DCHECK(false) << ss.str();
    LOG(WARNING) << ss.str();
  }
  return nullptr;
}

size_t CodeGenObjectCache::objSize() {
  size_t sz = sizeof(CodeGenObjectCache);
  if (cached_obj_ == nullptr) return sz;
  return cached_obj_->getMemBufferRef().getBufferSize() + sz;
}
} // namespace impala
