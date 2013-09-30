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


#ifndef IMPALA_EXPRS_AGG_FN_EVALUATOR_H
#define IMPALA_EXPRS_AGG_FN_EVALUATOR_H

#include <string>

#include "exprs/opcode-registry.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

class AggregationNode;
class TExprNode;

// This class evaluates aggregate functions. Aggregate funtions can either be
// builtins or external UDAs. For both of types types, they can either use codegen
// or not.
// This class provides an interface that's 1:1 with the UDA interface and serves
// as glue code between the TupleRow/Tuple signature used by the AggregationNode
// and the AnyVal signature of the UDA interface. It handles evaluating input
// slots from TupleRows and aggregating the result to the result tuple.
class AggFnEvaluator {
 public:
  // Creates an AggFnEvaluator object from desc. The object is added to 'pool'
  // and returned in *result. This constructs the input Expr trees for
  // this aggregate function as specified in desc. The result is returned in
  // *result.
  static Status Create(ObjectPool* pool, const TAggregateFunction& desc,
      AggFnEvaluator** result);

  // Initializes the agg expr. 'desc' must be the row descriptor for the input TupleRow.
  // It is used to get the input values in the Update() and Merge() functions.
  // 'output_slot_desc' is the slot that this aggregator should write to.
  // The underlying aggregate function allocates memory from the 'pool'. This is
  // either string data for intemerdiate results or whatever memory the UDA might
  // need.
  // TODO: should we give them their own pool?
  Status Prepare(RuntimeState* state, const RowDescriptor& desc,
      MemPool* pool, const SlotDescriptor* output_slot_desc);

  //PrimitiveType type() const { return type_.type; }
  TAggregationOp::type agg_op() const { return agg_op_; }
  const std::vector<Expr*>& input_exprs() const { return input_exprs_; }
  bool is_count_star() const {
    return agg_op_ == TAggregationOp::COUNT && input_exprs_.empty();
  }

  static std::string DebugString(const std::vector<AggFnEvaluator*>& exprs);
  std::string DebugString() const;

  // Functions for different phases of the aggregation.
  void Init(Tuple* dst);
  void Update(TupleRow* src, Tuple* dst);
  void Merge(TupleRow* src, Tuple* dst);
  void Serialize(Tuple* dst);
  void Finalize(Tuple* dst);

  // TODO: implement codegen path. These functions would return IR functions with
  // the same signature as the interpreted ones above.
  // Function* GetIrInitFn();
  // Function* GetIrUpdateFn();
  // Function* GetIrMergeFn();
  // Function* GetIrSerializeFn();
  // Function* GetIrFinalizeFn();

 private:
  const ColumnType return_type_;
  const ColumnType intermediate_type_;
  std::vector<Expr*> input_exprs_;

  const TAggregationOp::type agg_op_;

  // Unowned
  const SlotDescriptor* output_slot_desc_;

  // Context to run the aggregate functions.
  // TODO: this and pool_ make this not thread safe but they are easy to duplicate
  // per thread.
  boost::scoped_ptr<impala_udf::FunctionContext> ctx_;

  // Created to a subclass of AnyVal for type(). We use this to convert values
  // from the UDA interface to the Expr interface.
  // These objects are allocated in the runtime state's object pool.
  // TODO: this is awful, remove this when exprs are updated.
  std::vector<impala_udf::AnyVal*> staging_input_vals_;
  impala_udf::AnyVal* staging_output_val_;

  // Function ptrs to the aggregate function. This is either populated from the
  // opcode registry for builtins or from the external binary for native UDAs.
  OpcodeRegistry::AggFnDescriptor fn_ptrs_;

  // Use Create() instead.
  AggFnEvaluator(const TAggregateFunction& desc);

  // TODO: these funtions below are not extensible and we need to use codegen to
  // generate the calls into the UDA functions (like for UDFs).
  // Remove these functions when this is supported.

  // Sets up the arguments to call fn. This converts from the agg-expr signature,
  // taking TupleRow to the UDA signature taking AnvVals.
  void UpdateOrMerge(TupleRow* row, Tuple* dst, void* fn);
  // Writes the result in src into dst pointed to by output_slot_desc_
  void SetOutputSlot(const impala_udf::AnyVal* src, Tuple* dst);
  // Sets 'dst' to the value from 'slot'.
  void SetAnyVal(const void* slot, PrimitiveType type, impala_udf::AnyVal* dst);
};

}

#endif
