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

#include "codegen/subexpr-elimination.h"

#include <fstream>
#include <iostream>
#include <sstream>

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/InstructionSimplify.h>
#include "llvm/Transforms/IPO.h"
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/SSAUpdater.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>

#include "common/logging.h"
#include "codegen/subexpr-elimination.h"
#include "impala-ir/impala-ir-names.h"
#include "util/cpu-info.h"
#include "util/path-builder.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

SubExprElimination::SubExprElimination(LlvmCodeGen* codegen) :
    codegen_(codegen) {
}

// Before running the standard llvm optimization passes, first remove redudant calls
// to slotref expression.  SlotRefs are more heavyweight due to the null handling that
// is required and after they are inlined, llvm is unable to eliminate the redudant
// inlined code blocks.
// For example:
//   select colA + colA would generate an inner loop with 2 calls to the colA slot ref,
// rather than doing subexpression elimination.  To handle this, we will:
//   1. inline all call sites in the original function except calls to SlotRefs
//   2. for all call sites to SlotRefs except the first to that SlotRef, replace the
//      results from the secondary calls with the result from the first and remove
//      the call instruction.
//   3. Inline calls to the SlotRefs (there should only be one for each slot ref).
//
// In the above example, the input function would look something like:
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   int rhs_value = SlotRef(row, &rhs_is_null);
//   if (rhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + rhs_value;
// }
// During step 2, we'd substitute the second call to SlotRef with the results from
// the first call.
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   int rhs_value = lhs_value;
//   rhs_is_null = lhs_is_null;
//   if (rhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + rhs_value;
// }
// And then rely on llvm to finish the removing the redudant code, resulting in:
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + lhs_value;
// }
// Details on how to do this:
// http://llvm.org/docs/ProgrammersManual.html#replacing-an-instruction-with-another-value

// Step 2 requires more manipulation to ensure the resulting IR is still valid IR.
// This involves replacing the removed call instructions with PHI nodes to have
// unambiguous control flow.  The gist of the issue is that after replacing values
// llvm is unable to ascertain that the block where the src value (the first expr call)
// is set is guaranteed to execute in all call graphs which execute the block where the
// value was replaced.
// See this for more details:
// http://blog.llvm.org/2009/12/advanced-topics-in-redundant-load.html

struct CachedExprResult {
  // First function call result.  Subsequent calls will be replaced with this value
  CallInst* result;
  // First is null result.  Subsequent calls will be replaced with this value.
  LoadInst* is_null_load;
};

// Replaces all dst instructions with the result of src.  Propagates the changes
// through the call graph.  This is adapted from Scalar/LoopRotation.cpp
void ReplaceAllUses(Instruction* src, Instruction* dst, Type* type) {
  // Object to help rewrite the values while maintaining proper SSA form.
  SSAUpdater ssa_updater;
  ssa_updater.Initialize(type, src->getName());
  ssa_updater.AddAvailableValue(src->getParent(), src);
  Value::use_iterator ui = dst->use_begin();
  Value::use_iterator ue = dst->use_end();
  while (ui != ue) {
    // Grab the use before incrementing the iterator.
    Use& u = ui.getUse();
    ++ui;
    // Rewrite dst with src and add phi nodes as necessary through the call graph
    ssa_updater.RewriteUse(u);
  }
  dst->eraseFromParent();
}

bool SubExprElimination::Run(Function* fn) {
  // Step 1:
  int num_inlined;
  do {
    // This assumes that all redundant exprs have been registered.
    num_inlined = codegen_->InlineAllCallSites(fn, true);
  } while (num_inlined > 0);

  // Mapping of (expr eval function, its 'row' arg) to cached result.  We want to remove
  // redundant calls to the same function with the same argument.
  map<pair<Function*, Value*>, CachedExprResult> cached_slot_ref_results;

  // Step 2:
  Function::iterator block_iter = fn->begin();
  while (block_iter != fn->end()) {
    BasicBlock* block = block_iter++;
    // loop over instructions in the block
    BasicBlock::iterator instr_iter = block->begin();
    while (instr_iter != block->end()) {
      Instruction* instr = instr_iter++;
      // look for call instructions
      if (!CallInst::classof(instr)) continue;

      CallInst* call_instr = reinterpret_cast<CallInst*>(instr);
      Function* called_fn = call_instr->getCalledFunction();
      if (codegen_->registered_exprs_.find(called_fn) ==
          codegen_->registered_exprs_.end()) {
        continue;
      }

      // Found a registered expr function.  We generate the IR in a very specific way
      // when calling the expr.  The call instruction is always followed by loading the
      // resulting is_null result.  We need to update both.
      // TODO: we need to update this to do more analysis since we are relying on a very
      // specific code structure to do this.

      // Arguments are (row, scratch_buffer, is_null);
      DCHECK_EQ(call_instr->getNumArgOperands(), 3);
      Value* row_arg = call_instr->getArgOperand(0);

      DCHECK(BitCastInst::classof(row_arg));
      BitCastInst* row_cast = reinterpret_cast<BitCastInst*>(row_arg);
      // Get at the underlying row arg.  We need to differentiate between
      // call Fn(row1) and call Fn(row2). (identical fns but different input).
      row_arg = row_cast->getOperand(0);

      instr = instr_iter++;
      LoadInst* is_null_load = reinterpret_cast<LoadInst*>(instr);

      // Remove function calls that having matching Expr and input row.
      pair<Function*, Value*> call_desc = make_pair(called_fn, row_arg);

      if (cached_slot_ref_results.find(call_desc) == cached_slot_ref_results.end()) {
        // New function, save the result
        CachedExprResult cache_entry;
        cache_entry.result = call_instr;
        cache_entry.is_null_load = is_null_load;
        cached_slot_ref_results[call_desc] = cache_entry;
      } else {
        CachedExprResult& cache_entry = cached_slot_ref_results[call_desc];
        // Replace the call result
        // TODO; ReplaceInstWithValue doesn't just work. It has problems with
        // needing to insert PhiNodes. Investigate and understand this.
        ReplaceAllUses(cache_entry.result, call_instr,
            call_instr->getCalledFunction()->getReturnType());

        // Replace the stack allocated is_null result with the result from the
        // previous call.  We unfortunately load this value again later in the IR.
        // TODO: switch to multiple return values
        Instruction* cached_load =
            new LoadInst(cache_entry.is_null_load->getPointerOperand());
        BasicBlock::iterator is_null_iterator(is_null_load);
        llvm::ReplaceInstWithInst(
            is_null_load->getParent()->getInstList(), is_null_iterator, cached_load);
      }
    }
  }

  // Step 3:
  codegen_->InlineAllCallSites(fn, false);
  return true;
}

