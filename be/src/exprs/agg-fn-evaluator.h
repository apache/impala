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
#include "runtime/primitive-type.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class AggregationNode;
class Expr;
class MemPool;
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
  // 'output_slot_desc' is the slot that this evaluator should write to.
  // The underlying aggregate function allocates memory from the 'pool'. This is
  // either string data for intermediate results or whatever memory the UDA might
  // need.
  // TODO: should we give them their own pool?
  Status Prepare(RuntimeState* state, const RowDescriptor& desc,
      MemPool* pool, const SlotDescriptor* output_slot_desc);

  ~AggFnEvaluator();

  Status Open(RuntimeState* state);
  void Close(RuntimeState* state);

  AggregationOp agg_op() const { return agg_op_; }
  const std::vector<Expr*>& input_exprs() const { return input_exprs_; }
  bool is_count_star() const {
    return agg_op_ == COUNT && input_exprs_.empty();
  }
  bool is_builtin() const { return fn_.binary_type == TFunctionBinaryType::BUILTIN; }

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
  TFunction fn_;

  const ColumnType return_type_;
  const ColumnType intermediate_type_;
  std::vector<Expr*> input_exprs_;

  // The enum for some of the builtins that still require special cased logic.
  AggregationOp agg_op_;

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
  // taking TupleRow to the UDA signature taking AnvVals.
  void SerializeOrFinalize(Tuple* tuple, void* fn);

  // Writes the result in src into dst pointed to by output_slot_desc_
  void SetOutputSlot(const impala_udf::AnyVal* src, Tuple* dst);
  // Sets 'dst' to the value from 'slot'.
  void SetAnyVal(const void* slot, const ColumnType& type, impala_udf::AnyVal* dst);
};

}

#endif
