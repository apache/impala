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
#ifndef IMPALA_CODEGEN_INSTRUCTION_COUNTER_H_
#define IMPALA_CODEGEN_INSTRUCTION_COUNTER_H_

#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include <algorithm>

#include "common/logging.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Function.h"

namespace impala {

/// The InstructionCounter class handles visiting through the instructions in a block,
/// function or module. It holds all of the categories as well as the top level counters
/// such as the total number of instructions.
class InstructionCounter {
 public:
  /// String constants for instruction count names.
  static const char* TOTAL_INSTS;
  static const char* TOTAL_BLOCKS;
  static const char* TOTAL_FUNCTIONS;
  static const char* TERMINATOR_INSTS;
  static const char* BINARY_INSTS;
  static const char* MEMORY_INSTS;
  static const char* CAST_INSTS;
  static const char* OTHER_INSTS;

  InstructionCounter();

  /// Visits each Function in Module M.
  void visit(const llvm::Module& M);

  /// Increments "Total Functions" InstructionCounter and visits each BasicBlock in F.
  void visit(const llvm::Function &F);

  /// Increments "Total Blocks" InstructionCounter and visits each Instruction in BB.
  void visit(const llvm::BasicBlock &BB);

  /// Increments "Total Instructions" and whichever instruction count I is delegated to in
  /// this functions switch statement.
  void visit(const llvm::Instruction &I);

  /// Prints a single counter described by name and count
  void PrintCounter(const char* name, int count, int max_count_len,
      std::stringstream* stream) const;

  /// Prints all counters
  std::string PrintCounters() const;

  /// Return count of counter described by name
  int GetCount(const char* name);

  /// Set all counts to 0.
  void ResetCount();

 private:
  typedef std::map<std::string, int> CounterMap;

  /// Allows for easy visitation of iterators.
  template<class Iterator>
  void visit(Iterator start, Iterator end) {
    while (start != end) {
      visit(*start++);
    }
  }

  /// Increment InstructionCount with name_ equal to name argument.
  void IncrementCount(const char* name);

  /// This maps instruction names to their respective count.
  CounterMap counters_;
};

}  // namespace impala

#endif  // IMPALA_CODEGEN_INSTRUCTION_COUNTER_H_
