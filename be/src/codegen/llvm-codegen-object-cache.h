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

#pragma once

#include <memory>
#include <mutex>

#include <llvm/ExecutionEngine/ObjectCache.h>
#include <llvm/Support/MemoryBuffer.h>

#include "common/logging.h"

namespace impala {

class CodeGenObjectCache : public llvm::ObjectCache {
 public:
  CodeGenObjectCache() {}
  virtual ~CodeGenObjectCache() {}
  virtual void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj);
  virtual std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M);
  size_t objSize();

 private:
  std::mutex mutex_;
  std::string key_;
  std::unique_ptr<llvm::MemoryBuffer> cached_obj_;
};
} // namespace impala
