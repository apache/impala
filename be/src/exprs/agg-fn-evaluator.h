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


#ifndef IMPALA_EXPRS_AGG_FN_EVALUATOR_H
#define IMPALA_EXPRS_AGG_FN_EVALUATOR_H

#include <string>

#include <boost/scoped_ptr.hpp>
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/lib-cache.h"
#include "runtime/tuple-row.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"

using namespace impala_udf;

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

/// This class evaluates aggregate functions. Aggregate functions can either be
/// builtins or external UDAs. For both of types types, they can either use codegen
/// or not.
//
/// This class provides an interface that's 1:1 with the UDA interface and serves
/// as glue code between the TupleRow/Tuple signature used by the AggregationNode
/// and the AnyVal signature of the UDA interface. It handles evaluating input
/// slots from TupleRows and aggregating the result to the result tuple.
//
/// This class is not threadsafe. However, it can be used for multiple interleaved
/// evaluations of the aggregation function by using multiple FunctionContexts.
class AggFnEvaluator {
 public:
  /// TODO: The aggregation node has custom codegen paths for a few of the builtins.
  /// That logic needs to be removed. For now, add some enums for those builtins.
  enum AggregationOp {
    COUNT,
    MIN,
    MAX,
    SUM,
    AVG,
    NDV,
    OTHER,
  };

  /// Creates an AggFnEvaluator object from desc. The object is added to 'pool'
  /// and returned in *result. This constructs the input Expr trees for
  /// this aggregate function as specified in desc. The result is returned in
  /// *result.
  static Status Create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result);

  /// Creates an AggFnEvaluator object from desc. If is_analytic_fn, the evaluator is
  /// prepared for analytic function evaluation.
  /// TODO: Avoid parameter for analytic fns, should this be added to TAggregateExpr?
  static Status Create(ObjectPool* pool, const TExpr& desc, bool is_analytic_fn,
      AggFnEvaluator** result);

  /// Initializes the agg expr. 'desc' must be the row descriptor for the input TupleRow.
  /// It is used to get the input values in the Update() and Merge() functions.
  /// 'intermediate_slot_desc' is the slot into which this evaluator should write the
  /// results of Update()/Merge()/Serialize().
  /// 'output_slot_desc' is the slot into which this evaluator should write the results
  /// of Finalize()
  /// 'agg_fn_ctx' will be initialized for the agg function using 'agg_fn_pool'. Caller
  /// is responsible for closing and deleting 'agg_fn_ctx'.
  Status Prepare(RuntimeState* state, const RowDescriptor& desc,
      const SlotDescriptor* intermediate_slot_desc,
      const SlotDescriptor* output_slot_desc,
      MemPool* agg_fn_pool, FunctionContext** agg_fn_ctx);

  ~AggFnEvaluator();

  /// 'agg_fn_ctx' may be cloned after calling Open(). Note that closing all
  /// FunctionContexts, including the original one returned by Prepare(), is the
  /// responsibility of the caller.
  Status Open(RuntimeState* state, FunctionContext* agg_fn_ctx);

  void Close(RuntimeState* state);

  const ColumnType& intermediate_type() const { return intermediate_slot_desc_->type(); }
  bool is_merge() const { return is_merge_; }
  AggregationOp agg_op() const { return agg_op_; }
  const std::vector<ExprContext*>& input_expr_ctxs() const { return input_expr_ctxs_; }
  bool is_count_star() const { return agg_op_ == COUNT && input_expr_ctxs_.empty(); }
  bool is_builtin() const { return fn_.binary_type == TFunctionBinaryType::BUILTIN; }
  bool SupportsRemove() const { return remove_fn_ != NULL; }
  bool SupportsSerialize() const { return serialize_fn_ != NULL; }
  const std::string& fn_name() const { return fn_.name.function_name; }
  const SlotDescriptor* output_slot_desc() const { return output_slot_desc_; }

  static std::string DebugString(const std::vector<AggFnEvaluator*>& exprs);
  std::string DebugString() const;

  /// Functions for different phases of the aggregation.
  void Init(FunctionContext* agg_fn_ctx, Tuple* dst);

  /// Updates the intermediate state dst based on adding the input src row. This can be
  /// called either to drive the UDA's Update() or Merge() function depending on
  /// is_merge_. That is, from the caller, it doesn't mater.
  void Add(FunctionContext* agg_fn_ctx, const TupleRow* src, Tuple* dst);

  /// Updates the intermediate state dst to remove the input src row, i.e. undoes
  /// Add(src, dst). Only used internally for analytic fn builtins.
  void Remove(FunctionContext* agg_fn_ctx, const TupleRow* src, Tuple* dst);

  /// Explicitly does a merge, even if this evalutor is not marked as merging.
  /// This is used by the partitioned agg node when it needs to merge spill results.
  /// In the non-spilling case, this node would normally not merge.
  void Merge(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst);

  void Serialize(FunctionContext* agg_fn_ctx, Tuple* dst);
  void Finalize(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst);

  /// Puts the finalized value from Tuple* src in Tuple* dst just as Finalize() does.
  /// However, unlike Finalize(), GetValue() does not clean up state in src.
  /// GetValue() can be called repeatedly with the same src. Only used internally for
  /// analytic fn builtins. Note that StringVal result is from local allocation (which
  /// will be freed in the next QueryMaintenance()) so it needs to be copied out if it
  /// needs to survive beyond QueryMaintenance() (e.g. if 'dst' lives in a row batch).
  void GetValue(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst);

  /// Helper functions for calling the above functions on many evaluators.
  static void Init(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, Tuple* dst);
  static void Add(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, const TupleRow* src, Tuple* dst);
  static void Remove(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, const TupleRow* src, Tuple* dst);
  static void Serialize(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, Tuple* dst);
  static void GetValue(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, Tuple* src, Tuple* dst);
  static void Finalize(const std::vector<AggFnEvaluator*>& evaluators,
      const std::vector<FunctionContext*>& fn_ctxs, Tuple* src, Tuple* dst);

  /// Gets the codegened update or merge function for this aggregate function.
  Status GetUpdateOrMergeFunction(LlvmCodeGen* codegen, llvm::Function** uda_fn);

 private:
  const TFunction fn_;
  /// Indicates whether to Update() or Merge()
  const bool is_merge_;
  /// Indicates which functions must be loaded.
  const bool is_analytic_fn_;

  /// Slot into which Update()/Merge()/Serialize() write their result. Not owned.
  const SlotDescriptor* intermediate_slot_desc_;

  /// Slot into which Finalize() results are written. Not owned. Identical to
  /// intermediate_slot_desc_ if this agg fn has the same intermediate and output type.
  const SlotDescriptor* output_slot_desc_;

  /// Expression contexts for this AggFnEvaluator. Empty if there is no
  /// expression (e.g. count(*)).
  std::vector<ExprContext*> input_expr_ctxs_;

  /// The types of the arguments to the aggregate function.
  const std::vector<FunctionContext::TypeDesc> arg_type_descs_;

  /// The enum for some of the builtins that still require special cased logic.
  AggregationOp agg_op_;

  /// Created to a subclass of AnyVal for type(). We use this to convert values
  /// from the UDA interface to the Expr interface.
  /// These objects are allocated in the runtime state's object pool.
  /// TODO: this is awful, remove this when exprs are updated.
  std::vector<impala_udf::AnyVal*> staging_input_vals_;
  impala_udf::AnyVal* staging_intermediate_val_;
  impala_udf::AnyVal* staging_merge_input_val_;

  /// Cache entry for the library containing the function ptrs.
  LibCacheEntry* cache_entry_;

  /// Function ptrs for the different phases of the aggregate function.
  void* init_fn_;
  void* update_fn_;
  void* remove_fn_;
  void* merge_fn_;
  void* serialize_fn_;
  void* get_value_fn_;
  void* finalize_fn_;

  /// Use Create() instead.
  AggFnEvaluator(const TExprNode& desc, bool is_analytic_fn);

  /// Return the intermediate type of the aggregate function.
  FunctionContext::TypeDesc GetIntermediateTypeDesc() const;

  /// Return the output type of the aggregate function.
  FunctionContext::TypeDesc GetOutputTypeDesc() const;

  /// TODO: these functions below are not extensible and we need to use codegen to
  /// generate the calls into the UDA functions (like for UDFs).
  /// Remove these functions when this is supported.

  /// Sets up the arguments to call fn. This converts from the agg-expr signature,
  /// taking TupleRow to the UDA signature taking AnvVals by populating the staging
  /// AnyVals.
  /// fn must be a function that implement's the UDA Update() signature.
  void Update(FunctionContext* agg_fn_ctx, const TupleRow* row, Tuple* dst, void* fn);

  /// Sets up the arguments to call 'fn'. This converts from the agg-expr signature,
  /// taking TupleRow to the UDA signature taking AnvVals. Writes the serialize/finalize
  /// result to the given destination slot/tuple. 'fn' can be NULL to indicate the src
  /// value should simply be written into the destination. Note that StringVal result is
  /// from local allocation (which will be freed in the next QueryMaintenance()) so it
  /// needs to be copied out if it needs to survive beyond QueryMaintenance() (e.g. if
  /// 'dst' lives in a row batch).
  void SerializeOrFinalize(FunctionContext* agg_fn_ctx, Tuple* src,
      const SlotDescriptor* dst_slot_desc, Tuple* dst, void* fn);

  /// Writes the result in src into dst pointed to by dst_slot_desc
  void SetDstSlot(FunctionContext* ctx, const impala_udf::AnyVal* src,
      const SlotDescriptor* dst_slot_desc, Tuple* dst);
};

inline void AggFnEvaluator::Add(
    FunctionContext* agg_fn_ctx, const TupleRow* row, Tuple* dst) {
  agg_fn_ctx->impl()->IncrementNumUpdates();
  Update(agg_fn_ctx, row, dst, is_merge() ? merge_fn_ : update_fn_);
}
inline void AggFnEvaluator::Remove(
    FunctionContext* agg_fn_ctx, const TupleRow* row, Tuple* dst) {
  agg_fn_ctx->impl()->IncrementNumRemoves();
  Update(agg_fn_ctx, row, dst, remove_fn_);
}
inline void AggFnEvaluator::Serialize(
    FunctionContext* agg_fn_ctx, Tuple* tuple) {
  SerializeOrFinalize(agg_fn_ctx, tuple, intermediate_slot_desc_, tuple, serialize_fn_);
}
inline void AggFnEvaluator::Finalize(
    FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst) {
  SerializeOrFinalize(agg_fn_ctx, src, output_slot_desc_, dst, finalize_fn_);
}
inline void AggFnEvaluator::GetValue(
    FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst) {
  SerializeOrFinalize(agg_fn_ctx, src, output_slot_desc_, dst, get_value_fn_);
}

inline void AggFnEvaluator::Init(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->Init(fn_ctxs[i], dst);
  }
}
inline void AggFnEvaluator::Add(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, const TupleRow* src, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->Add(fn_ctxs[i], src, dst);
  }
}
inline void AggFnEvaluator::Remove(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, const TupleRow* src, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->Remove(fn_ctxs[i], src, dst);
  }
}
inline void AggFnEvaluator::Serialize(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->Serialize(fn_ctxs[i], dst);
  }
}
inline void AggFnEvaluator::GetValue(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, Tuple* src, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->GetValue(fn_ctxs[i], src, dst);
  }
}
inline void AggFnEvaluator::Finalize(const std::vector<AggFnEvaluator*>& evaluators,
    const std::vector<FunctionContext*>& fn_ctxs, Tuple* src, Tuple* dst) {
  DCHECK_EQ(evaluators.size(), fn_ctxs.size());
  for (int i = 0; i < evaluators.size(); ++i) {
    evaluators[i]->Finalize(fn_ctxs[i], src, dst);
  }
}

}

#endif
