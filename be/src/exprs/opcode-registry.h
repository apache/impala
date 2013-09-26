// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_EXPRS_OPCODE_REGISTRY_H
#define IMPALA_EXPRS_OPCODE_REGISTRY_H

#include <string>
#include <vector>
#include <boost/thread/mutex.hpp>
#include "common/logging.h"
#include "exprs/expr.h"   // For ComputeFn typedef
#include "gen-cpp/Opcodes_types.h"

namespace impala {

class Expr;
class TupleRow;

class OpcodeRegistry {
 public:
  // Returns the function ptr for this opcode.
  void* GetFunctionPtr(TExprOpcode::type opcode) {
    int index = static_cast<int>(opcode);
    DCHECK_GE(index, 0);
    DCHECK_LT(index, functions_.size());
    return functions_[index];
  }

  // Registry is a singleton
  static OpcodeRegistry* Instance() {
    if (instance_ == NULL) {
      boost::lock_guard<boost::mutex> l(instance_lock_);
      if (instance_ == NULL) {
        // Make sure not to assign instance_ (and make it visible to other threads)
        // until it is fully initialized.  Note the fast path does not lock.
        instance_ = new OpcodeRegistry();
      }
    }
    return instance_;
  }

 private:
  // Private constructor, singleton interface
  OpcodeRegistry() {
    int num_opcodes = static_cast<int>(TExprOpcode::LAST_OPCODE);
    functions_.resize(num_opcodes);
    Init();
  }

  // Populates all of the registered functions. Implemented in
  // opcode-registry-init.cc which is an auto-generated file
  void Init();

  void Add(TExprOpcode::type opcode, void* fn) {
    int index = static_cast<int>(opcode);
    DCHECK_LT(index, functions_.size());
    DCHECK_GE(index, 0);
    functions_[index] = fn;
  }

  static OpcodeRegistry* instance_;
  static boost::mutex instance_lock_;
  std::vector<void*> functions_;
};

}

#endif

