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
#include <boost/unordered_map.hpp>

#include "common/logging.h"
#include "exprs/expr.h"   // For ComputeFn typedef

#include "gen-cpp/Opcodes_types.h"
#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class Expr;
class TupleRow;

// TODO: use this as a function cache mechanism for UDFs/UDAs
class OpcodeRegistry {
 public:
  // Struct that contains function ptrs for each phase of the aggregation as
  // defined by the UDA interface.
  // Nulls indicates that the function does not implement that phase.
  // These are not typedefed, since the actual signature is variable.
  // TODO: for cross-compiled IR aggregate functions, we'll either add the
  // symbol name (string) or the llvm::Function*.
  struct AggFnDescriptor {
    void* init_fn;
    void* update_fn;
    void* merge_fn;
    void* serialize_fn;
    void* finalize_fn;

    AggFnDescriptor(void* init = NULL, void* update = NULL, void* merge = NULL,
        void* serialize = NULL, void* finalize = NULL)
      : init_fn(init), update_fn(update), merge_fn(merge), serialize_fn(serialize),
        finalize_fn(finalize) {
    }
  };

  // Returns the function ptr for this opcode.
  void* GetFunctionPtr(TExprOpcode::type opcode) {
    int index = static_cast<int>(opcode);
    DCHECK_GE(index, 0);
    DCHECK_LT(index, scalar_builtins_.size());
    return scalar_builtins_[index];
  }

  // Returns the builtin function ptrs for an aggregate function.
  const AggFnDescriptor* GetBuiltinAggFnDescriptor(
      const std::pair<TAggregationOp::type, PrimitiveType>& arg_type) const {
    AggregateBuiltins::const_iterator it = aggregate_builtins_.find(arg_type);
    if (it == aggregate_builtins_.end()) return NULL;
    return &it->second;
  }

  // Returns the function symbol for this opcode (used for loading IR functions).
  const std::string& GetFunctionSymbol(TExprOpcode::type opcode) {
    int index = static_cast<int>(opcode);
    DCHECK_GE(index, 0);
    DCHECK_LT(index, symbols_.size());
    return symbols_[index];
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

  // Mapping of builtin aggregate function op and input type to function desc.
  typedef boost::unordered_map<
      std::pair<TAggregationOp::type, PrimitiveType>, AggFnDescriptor> AggregateBuiltins;

 private:
  // Private ctor. Singleton interface.
  OpcodeRegistry();

  // Populates all of the registered functions. Implemented in
  // opcode-registry-init.cc which is an auto-generated file
  void Init();

  void Add(TExprOpcode::type opcode, void* fn, const char* symbol) {
    int index = static_cast<int>(opcode);
    DCHECK_LT(index, scalar_builtins_.size());
    DCHECK_GE(index, 0);
    scalar_builtins_[index] = fn;
    symbols_[index] = symbol;
  }

  static OpcodeRegistry* instance_;
  static boost::mutex instance_lock_;
  std::vector<void*> scalar_builtins_;
  std::vector<std::string> symbols_;
  AggregateBuiltins aggregate_builtins_;
};

}

#endif

