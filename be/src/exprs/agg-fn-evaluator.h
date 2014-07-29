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

#include <boost/scoped_ptr.hpp>
#include "common/status.h"
#include "runtime/lib-cache.h"
#include "runtime/types.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class AggregationNode;
class Expr;
class ExprContext;
class MemPool;
class MemTracker;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class Tuple;
class TupleRow;
class TExprNode;

// This class evaluates aggregate functions. Aggregate functions can either be
// builtins or external UDAs. For both of types types, they can either use codegen
// or not.
// This class provides an interface that's 1:1 with the UDA interface and serves
// as glue code between the TupleRow/Tuple signature used by the AggregationNode
// and the AnyVal signature of the UDA interface. It handles evaluating input
// slots from TupleRows and aggregating the result to the result tuple.
class AggFnEvaluator {
 public:
  // TODO: The aggregation node has custom codegen paths for a few of the builtins.
  // That logic needs to be removed. For now, add some enums for those builtins.
  enum AggregationOp {
    COUNT,
    MIN,
    MAX,
    SUM,
    NDV,
    OTHER,
  };

  // Creates an AggFnEvaluator object from desc. The object is added to 'pool'
  // and returned in *result. This constructs the input Expr trees for
  // this aggregate function as specified in desc. The result is returned in
  // *result.
  static Status Create(ObjectPool* pool, const TExpr& desc,
      AggFnEvaluator** result);

  // Initializes the agg expr. 'desc' must be the row descriptor for the input TupleRow.
  // It is used to get the input values in the Update() and Merge() functions.
  // 'intermediate_slot_desc' is the slot into which this evaluator should write the
  // results of Update()/Merge()/Serialize().
  // 'output_slot_desc' is the slot into which this evaluator should write the results
  // of Finalize()
  Status Prepare(RuntimeState* state, const RowDescriptor& desc,
      const SlotDescriptor* intermediate_slot_desc,
      const SlotDescriptor* output_slot_desc);

  ~AggFnEvaluator();

  Status Open(RuntimeState* state);
  void Close(RuntimeState* state);

  bool is_merge() { return is_merge_; }
  AggregationOp agg_op() const { return agg_op_; }
  impala_udf::FunctionContext* ctx() { return ctx_.get(); }
  const std::vector<ExprContext*>& input_expr_ctxs() const { return input_expr_ctxs_; }
  bool is_count_star() const {
    return agg_op_ == COUNT && input_expr_ctxs_.empty();
  }
  bool is_builtin() const { return fn_.binary_type == TFunctionBinaryType::BUILTIN; }

  static std::string DebugString(const std::vector<AggFnEvaluator*>& exprs);
  std::string DebugString() const;

  // Functions for different phases of the aggregation.
  void Init(Tuple* dst);
  void Update(TupleRow* src, Tuple* dst);
  void Merge(TupleRow* src, Tuple* dst);
  void Serialize(Tuple* dst);
  void Finalize(Tuple* src, Tuple* dst);

  // TODO: implement codegen path. These functions would return IR functions with
  // the same signature as the interpreted ones above.
  // Function* GetIrInitFn();
  // Function* GetIrUpdateFn();
  // Function* GetIrMergeFn();
  // Function* GetIrSerializeFn();
  // Function* GetIrFinalizeFn();

 private:
  TFunction fn_;
  bool is_merge_; // indicates whether to Update() or Merge()
  std::vector<ExprContext*> input_expr_ctxs_;

  // The enum for some of the builtins that still require special cased logic.
  AggregationOp agg_op_;

  // Slot into which Update()/Merge()/Serialize() write their result. Not owned.
  const SlotDescriptor* intermediate_slot_desc_;

  // Slot into which Finalize() results are written. Not owned. Identical to
  // intermediate_slot_desc_ if this agg fn has the same intermediate and output type.
  const SlotDescriptor* output_slot_desc_;

  // Context to run all steps of this aggregation including Init(), Update()/Merge(),
  // Serialize()/Finalize().
  boost::scoped_ptr<impala_udf::FunctionContext> ctx_;

  // Pool used by ctx_ for allocations.
  boost::scoped_ptr<MemPool> ctx_pool_;

  // Created to a subclass of AnyVal for type(). We use this to convert values
  // from the UDA interface to the Expr interface.
  // These objects are allocated in the runtime state's object pool.
  // TODO: this is awful, remove this when exprs are updated.
  std::vector<impala_udf::AnyVal*> staging_input_vals_;
  impala_udf::AnyVal* staging_intermediate_val_;

  // Cache entry for the library containing the function ptrs.
  LibCache::LibCacheEntry* cache_entry_;

  // Function ptrs for the different phases of the aggregate function.
  void* init_fn_;
  void* update_fn_;
  void* merge_fn_;
  void* serialize_fn_;
  void* finalize_fn_;

  // Use Create() instead.
  AggFnEvaluator(const TExprNode& desc);

  // TODO: these functions below are not extensible and we need to use codegen to
  // generate the calls into the UDA functions (like for UDFs).
  // Remove these functions when this is supported.

  // Sets up the arguments to call fn. This converts from the agg-expr signature,
  // taking TupleRow to the UDA signature taking AnvVals.
  void UpdateOrMerge(TupleRow* row, Tuple* dst, void* fn);

  // Sets up the arguments to call fn. This converts from the agg-expr signature,
  // taking TupleRow to the UDA signature taking AnvVals. Writes the serialize/finalize
  // result to the given destination slot/tuple. The fn can be NULL to indicate the src
  // value should simply be written into the destination.
  void SerializeOrFinalize(Tuple* src, const SlotDescriptor* dst_slot_desc, Tuple* dst,
      void* fn);

  // Writes the result in src into dst pointed to by dst_slot_desc
  void SetDstSlot(const impala_udf::AnyVal* src, const SlotDescriptor* dst_slot_desc,
      Tuple* dst);
};

}

#endif
