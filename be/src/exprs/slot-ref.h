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

#ifndef IMPALA_EXPRS_SLOTREF_H
#define IMPALA_EXPRS_SLOTREF_H

#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"

namespace impala {

using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::DateVal;

/// Reference to a single slot of a tuple.
class SlotRef : public ScalarExpr {
 public:
  SlotRef(const TExprNode& node);
  SlotRef(const SlotDescriptor* desc);

  /// TODO: this is a hack to allow aggregation nodes to work around NULL slot
  /// descriptors. Ideally the FE would dictate the type of the intermediate SlotRefs.
  SlotRef(const SlotDescriptor* desc, const ColumnType& type);

  /// Used for testing.  GetValue will return tuple + offset interpreted as 'type'
  SlotRef(const ColumnType& type, int offset, const bool nullable = false);

  /// Exposed as public so AGG node can initialize its build expressions.
  virtual Status Init(const RowDescriptor& row_desc, bool is_entry_point,
      RuntimeState* state) override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;
  virtual Status GetCodegendComputeFnImpl(
      LlvmCodeGen* codegen, llvm::Function** fn) override WARN_UNUSED_RESULT;
  virtual bool IsSlotRef() const override { return true; }
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const override;
  const SlotId& slot_id() const { return slot_id_; }
  static const char* LLVM_CLASS_NAME;

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES
  virtual CollectionVal GetCollectionValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const override;

 private:
  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
  bool tuple_is_nullable_; // true if the tuple is nullable.
};

}

#endif
