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

#include <atomic>

namespace impala {

class CodegenFnPtrBase {
 public:
   CodegenFnPtrBase() = default;
   virtual ~CodegenFnPtrBase() = 0;

   void* load() const {
     return ptr_.load(mem_order_load_);
   }

   void store(void* value) {
     ptr_.store(value, mem_order_store_);
   }

 private:
  static constexpr std::memory_order mem_order_load_ = std::memory_order_acquire;
  static constexpr std::memory_order mem_order_store_ = std::memory_order_release;
  std::atomic<void*> ptr_{nullptr};
};

inline CodegenFnPtrBase::~CodegenFnPtrBase() = default;

/// Class specialised to the type of the function pointer. Note that this is only a
/// convenience class to handle casts and to have the function type name in its type name
/// for documentation. It is not type-safe as any pointer can be stored through the base
/// class, which is necessary because LlvmCodegen needs to be able to set function
/// pointers of any type.
template <class FuncType>
class CodegenFnPtr : public CodegenFnPtrBase {
 public:
   FuncType load() const {
     return reinterpret_cast<FuncType>(CodegenFnPtrBase::load());
   }

   void store(FuncType value) {
     CodegenFnPtrBase::store(reinterpret_cast<void*>(value));
   }
};

} // namespace impala.
