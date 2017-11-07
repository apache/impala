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

#include "codegen/instruction-counter.h"

#include "common/logging.h"

#include "common/names.h"

using namespace impala;
using std::make_pair;
using std::max;

const char* InstructionCounter::TOTAL_INSTS = "Total Instructions";
const char* InstructionCounter::TOTAL_BLOCKS = "Total Blocks";
const char* InstructionCounter::TOTAL_FUNCTIONS = "Total Functions";
const char* InstructionCounter::TERMINATOR_INSTS = "Terminator Instructions ";
const char* InstructionCounter::BINARY_INSTS = "Binary Instructions ";
const char* InstructionCounter::MEMORY_INSTS = "Memory Instructions ";
const char* InstructionCounter::CAST_INSTS = "Cast Instructions ";
const char* InstructionCounter::OTHER_INSTS = "Other Instructions ";

InstructionCounter::InstructionCounter() {
  // Create top level counters and put them into counters_
  counters_.insert(make_pair(TOTAL_INSTS, 0));
  counters_.insert(make_pair(TOTAL_BLOCKS, 0));
  counters_.insert(make_pair(TOTAL_FUNCTIONS, 0));

  // Create all instruction counter and put them into counters_. Any InstructionCount
  // that has instructions delegated to it in InstructionCounter::visit(const
  // Instruction &I) must be created and inserted into counters_ here.
  counters_.insert(make_pair(TERMINATOR_INSTS, 0));
  counters_.insert(make_pair(BINARY_INSTS, 0));
  counters_.insert(make_pair(MEMORY_INSTS, 0));
  counters_.insert(make_pair(CAST_INSTS, 0));
  counters_.insert(make_pair(OTHER_INSTS, 0));
}

void InstructionCounter::visit(const llvm::Module& M) {
  visit(M.begin(), M.end());
}

void InstructionCounter::visit(const llvm::Function& F) {
  IncrementCount(TOTAL_FUNCTIONS);
  visit(F.begin(), F.end());
}

void InstructionCounter::visit(const llvm::BasicBlock& BB) {
  IncrementCount(TOTAL_BLOCKS);
  visit(BB.begin(), BB.end());
}

int InstructionCounter::GetCount(const char* name) {
  CounterMap::const_iterator counter = counters_.find(name);
  DCHECK(counter != counters_.end());
  return counter->second;
}

void InstructionCounter::visit(const llvm::Instruction& I) {
  IncrementCount(TOTAL_INSTS);
  switch (I.getOpcode()) {
    case llvm::Instruction::Ret:
    case llvm::Instruction::Br:
    case llvm::Instruction::Switch:
    case llvm::Instruction::IndirectBr:
    case llvm::Instruction::Invoke:
    case llvm::Instruction::Resume:
    case llvm::Instruction::Unreachable:
      IncrementCount(TERMINATOR_INSTS);
      break;
    case llvm::Instruction::Add:
    case llvm::Instruction::FAdd:
    case llvm::Instruction::Sub:
    case llvm::Instruction::FSub:
    case llvm::Instruction::Mul:
    case llvm::Instruction::FMul:
    case llvm::Instruction::UDiv:
    case llvm::Instruction::SDiv:
    case llvm::Instruction::FDiv:
    case llvm::Instruction::URem:
    case llvm::Instruction::SRem:
    case llvm::Instruction::FRem:
    case llvm::Instruction::Shl:
    case llvm::Instruction::LShr:
    case llvm::Instruction::AShr:
    case llvm::Instruction::And:
    case llvm::Instruction::Or:
    case llvm::Instruction::Xor:
      IncrementCount(BINARY_INSTS);
      break;
    case llvm::Instruction::Alloca:
    case llvm::Instruction::Load:
    case llvm::Instruction::Store:
    case llvm::Instruction::Fence:
    case llvm::Instruction::AtomicCmpXchg:
    case llvm::Instruction::AtomicRMW:
      IncrementCount(MEMORY_INSTS);
      break;
    case llvm::Instruction::Trunc:
    case llvm::Instruction::ZExt:
    case llvm::Instruction::SExt:
    case llvm::Instruction::FPToUI:
    case llvm::Instruction::FPToSI:
    case llvm::Instruction::UIToFP:
    case llvm::Instruction::SIToFP:
    case llvm::Instruction::FPTrunc:
    case llvm::Instruction::FPExt:
    case llvm::Instruction::PtrToInt:
    case llvm::Instruction::IntToPtr:
    case llvm::Instruction::BitCast:
      IncrementCount(CAST_INSTS);
      break;
    case llvm::Instruction::ICmp:
    case llvm::Instruction::FCmp:
    case llvm::Instruction::PHI:
    case llvm::Instruction::Call:
    case llvm::Instruction::Select:
    case llvm::Instruction::UserOp1:
    case llvm::Instruction::UserOp2:
    case llvm::Instruction::VAArg:
    case llvm::Instruction::ExtractElement:
    case llvm::Instruction::InsertElement:
    case llvm::Instruction::ShuffleVector:
    case llvm::Instruction::ExtractValue:
    case llvm::Instruction::InsertValue:
    case llvm::Instruction::LandingPad:
    case llvm::Instruction::GetElementPtr:
      IncrementCount(OTHER_INSTS);
      break;
    default:
      DCHECK(false);
  }
}

void InstructionCounter::ResetCount() {
  for (CounterMap::value_type& counter: counters_) counter.second = 0;
}

void InstructionCounter::PrintCounter(const char* name, int count, int max_count_len,
    stringstream* stream) const {
  if (count > 0) {
    *stream << left << setw(max_count_len + 1) << setfill(' ') << count << setw(35)
            << setfill(' ') << name << left << setw(10 - max_count_len) << setfill(' ')
            << "Number of " << name << endl;
  }
}

string InstructionCounter::PrintCounters() const {
  // Find the longest length of all the InstructionCount count_ strings.
  int max_count_len = 0;
  stringstream count_stream;
  for (const CounterMap::value_type& counter: counters_) {
    count_stream << counter.second;
    max_count_len =
        max(max_count_len, static_cast<int>(strlen(count_stream.str().c_str())));
    count_stream.str("");
  }
  stringstream stream;
  // Print header
  stream << "\n===" << string(73, '-') << "===\n\n"
         << "                          ... Instruction Counts ...\n\n"
         << "===" << string(73, '-') << "===\n\n";

  for (const CounterMap::value_type& counter: counters_) {
    // Conditional is only used in order to print the top level counters
    // separate from the other counters.
    if (strcmp(counter.first.c_str(), TOTAL_BLOCKS) == 0) {
      stream << "\n===" << string(73, '-') << "===\n"
             << "                                ... Totals ...\n"
             << "===" << string(73, '-') << "===\n\n";
    }
    PrintCounter(counter.first.c_str(), counter.second, max_count_len, &stream);
  }
  stream << '\n';
  return stream.str();
}

void InstructionCounter::IncrementCount(const char* name) {
  CounterMap::iterator iter = counters_.find(name);
  DCHECK(iter != counters_.end());
  iter->second++;
}
