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

#ifndef IMPALA_EXPRS_SLOTREF_H
#define IMPALA_EXPRS_SLOTREF_H

#include "exprs/expr.h"

namespace impala {

/// Reference to a single slot of a tuple.
class SlotRef : public Expr {
 public:
  SlotRef(const TExprNode& node);
  SlotRef(const SlotDescriptor* desc);

  /// TODO: this is a hack to allow aggregation nodes to work around NULL slot
  /// descriptors. Ideally the FE would dictate the type of the intermediate SlotRefs.
  SlotRef(const SlotDescriptor* desc, const ColumnType& type);

  /// Used for testing.  GetValue will return tuple + offset interpreted as 'type'
  SlotRef(const ColumnType& type, int offset, const bool nullable = false);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                         ExprContext* context);
  virtual std::string DebugString() const;
  virtual bool IsConstant() const { return false; }
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;
  const SlotId& slot_id() const { return slot_id_; }

  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

  virtual impala_udf::BooleanVal GetBooleanVal(ExprContext* context, TupleRow*);
  virtual impala_udf::TinyIntVal GetTinyIntVal(ExprContext* context, TupleRow*);
  virtual impala_udf::SmallIntVal GetSmallIntVal(ExprContext* context, TupleRow*);
  virtual impala_udf::IntVal GetIntVal(ExprContext* context, TupleRow*);
  virtual impala_udf::BigIntVal GetBigIntVal(ExprContext* context, TupleRow*);
  virtual impala_udf::FloatVal GetFloatVal(ExprContext* context, TupleRow*);
  virtual impala_udf::DoubleVal GetDoubleVal(ExprContext* context, TupleRow*);
  virtual impala_udf::StringVal GetStringVal(ExprContext* context, TupleRow*);
  virtual impala_udf::TimestampVal GetTimestampVal(ExprContext* context, TupleRow*);
  virtual impala_udf::DecimalVal GetDecimalVal(ExprContext* context, TupleRow*);
  virtual impala_udf::CollectionVal GetCollectionVal(ExprContext* context, TupleRow*);

 protected:
  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
  bool tuple_is_nullable_; // true if the tuple is nullable.
};

}

#endif
