// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_OPCODE_REGISTRY_H
#define IMPALA_EXPRS_OPCODE_REGISTRY_H

#include <string>
#include <vector>
#include <glog/logging.h>
#include "exprs/expr.h"   // For ComputeFn typedef
#include "gen-cpp/Opcodes_types.h"

namespace impala {

class Expr;
class TupleRow;

class OpcodeRegistry {
 public:
  // Returns the function for this opcode.  If the opcdoe is not valid,
  // this function returns NULL
   Expr::ComputeFn GetFunction(TExprOpcode::type opcode) {
    int index = static_cast<int>(opcode);
    DCHECK_GE(index, 0);
    DCHECK_LT(index, functions_.size());
    return functions_[index];
  }

  // Registry is a singleton
  static OpcodeRegistry* Instance() {
    if (instance_ == NULL) {
      instance_ = new OpcodeRegistry();
      instance_->Init();
    }
    return instance_;
  }

 private:
  // Private constructor, singleton interface
  OpcodeRegistry() {
    int num_opcodes = static_cast<int>(TExprOpcode::LAST_OPCODE);
    functions_.resize(num_opcodes);
  }

  // Populates all of the registered functions. Implemented in
  // opcode-registry-init.cc which is an auto-generated file 
  void Init();

  // Add a function to the registry.
  void Add(TExprOpcode::type opcode, const Expr::ComputeFn& function) {
    int index = static_cast<int>(opcode);
    DCHECK_LT(index, functions_.size());
    DCHECK_GE(index, 0);
    functions_[index] = function;
  }

  static OpcodeRegistry* instance_;
  std::vector<Expr::ComputeFn> functions_;
};

}

#endif

