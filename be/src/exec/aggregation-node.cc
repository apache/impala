// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/aggregation-node.h"

#include <sstream>
#include <boost/functional/hash.hpp>

#include "exprs/agg-expr.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;

// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::Init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    hash_fn_(this),
    equals_fn_(this),
    agg_tuple_id_(tnode.agg_node.agg_tuple_id),
    agg_tuple_desc_(NULL),
    singleton_output_tuple_(NULL),
    current_row_(NULL),
    tuple_pool_(new MemPool()) {
  // ignore return status for now
  Expr::CreateExprTrees(pool, tnode.agg_node.grouping_exprs, &grouping_exprs_);
  Expr::CreateExprTrees(pool, tnode.agg_node.aggregate_exprs, &aggregate_exprs_);
}

void AggregationNode::GroupingExprHash::Init(
    TupleDescriptor* agg_tuple_d, const vector<Expr*>& grouping_exprs) {
  agg_tuple_desc_ = agg_tuple_d;
  grouping_exprs_ = &grouping_exprs;
}

size_t AggregationNode::GroupingExprHash::operator()(Tuple* const& t) const {
  size_t seed = 0;
  for (int i = 0; i < grouping_exprs_->size(); ++i) {
    SlotDescriptor* slot_d = agg_tuple_desc_->slots()[i];
    const void* value;
    if (t != NULL) {
      if (t->IsNull(slot_d->null_indicator_offset())) {
        value = NULL;
      } else {
        value = t->GetSlot(slot_d->tuple_offset());
      }
    } else {
      // compute grouping exprs value over node->current_row_
      value = (*grouping_exprs_)[i]->GetValue(node_->current_row_);
    }
    // don't ignore NULLs; we want (1, NULL) to return a different hash
    // value than (NULL, 1)
    size_t hash_value =
        (value == NULL ? 0 : RawValue::GetHashValue(value, slot_d->type()));
    hash_combine(seed, hash_value);
  }
  return seed;
}

void AggregationNode::GroupingExprEquals::Init(
    TupleDescriptor* agg_tuple_d, const vector<Expr*>& grouping_exprs) {
  agg_tuple_desc_ = agg_tuple_d;
  grouping_exprs_ = &grouping_exprs;
}

bool AggregationNode::GroupingExprEquals::operator()(
    Tuple* const& t1, Tuple* const& t2) const {
  for (int i = 0; i < grouping_exprs_->size(); ++i) {
    SlotDescriptor* slot_d = agg_tuple_desc_->slots()[i];
    const void* value1;
    if (t1 != NULL) {
      if (t1->IsNull(slot_d->null_indicator_offset())) {
        value1 = NULL;
      } else {
        value1 = t1->GetSlot(slot_d->tuple_offset());
      }
    } else {
      value1 = (*grouping_exprs_)[i]->GetValue(node_->current_row_);
    }
    const void* value2;
    if (t2->IsNull(slot_d->null_indicator_offset())) {
      value2 = NULL;
    } else {
      value2 = t2->GetSlot(slot_d->tuple_offset());
    }
    if (value1 == NULL || value2 == NULL) {
      // nulls are considered equal for the purpose of grouping
      if (value1 != NULL || value2 != NULL) return false;
    } else {
      if (RawValue::Compare(value1, value2, slot_d->type()) != 0) return false;
    }
  }
  return true;
} 

Status AggregationNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  agg_tuple_desc_ = state->descs().GetTupleDescriptor(agg_tuple_id_);
  Expr::Prepare(grouping_exprs_, state, child(0)->row_desc());
  Expr::Prepare(aggregate_exprs_, state, child(0)->row_desc());
  input_tuple_descs_ = children_[0]->row_desc().tuple_descriptors();
  hash_fn_.Init(agg_tuple_desc_, grouping_exprs_);
  equals_fn_.Init(agg_tuple_desc_, grouping_exprs_);
  // TODO: how many buckets?
  hash_tbl_.reset(new HashTable(5, hash_fn_, equals_fn_));
  return Status::OK;
}

Status AggregationNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(children_[0]->Open(state));

  if (grouping_exprs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ = ConstructAggTuple(NULL);
  }

  RowBatch batch(input_tuple_descs_, state->batch_size());
  while (true) {
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch));
    for (int i = 0; i < batch.num_rows(); ++i) {
      current_row_ = batch.GetRow(i);
      Tuple* agg_tuple;
      if (singleton_output_tuple_ != NULL) {
        agg_tuple = singleton_output_tuple_;
      } else {
        // find(NULL) finds the entry for current_row_
        HashTable::iterator entry = hash_tbl_->find(NULL);
        if (entry == hash_tbl_->end()) {
          // new entry
          agg_tuple = ConstructAggTuple(current_row_);
          hash_tbl_->insert(agg_tuple);
        } else {
          agg_tuple = *entry;
        }
      }
      UpdateAggTuple(agg_tuple, current_row_);
    }
    if (batch.num_rows() < batch.capacity()) break;
    batch.Reset();
  }
  RETURN_IF_ERROR(children_[0]->Close(state));
  if (singleton_output_tuple_ != NULL) {
    hash_tbl_->insert(singleton_output_tuple_);
  }

  output_iterator_ = hash_tbl_->begin();
  return Status::OK;
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch) {
  while (output_iterator_ != hash_tbl_->end() && !row_batch->IsFull()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(0, *output_iterator_);
    if (ExecNode::EvalConjuncts(row)) row_batch->CommitLastRow();
    ++output_iterator_;
  }
  return Status::OK;
}

Status AggregationNode::Close(RuntimeState* state) {
  return Status::OK;
}

Tuple* AggregationNode::ConstructAggTuple(TupleRow* row) {
  Tuple* agg_tuple = Tuple::Create(agg_tuple_desc_->byte_size(), tuple_pool_.get());
  vector<SlotDescriptor*>::const_iterator slot_d = agg_tuple_desc_->slots().begin();
  // copy grouping values
  for (int i = 0; i < grouping_exprs_.size(); ++i, ++slot_d) {
    void* grouping_val = grouping_exprs_[i]->GetValue(row);
    if (grouping_val == NULL) {
      agg_tuple->SetNull((*slot_d)->null_indicator_offset());
    } else {
      RawValue::Write(grouping_val, agg_tuple, *slot_d, tuple_pool_.get());
    }
  }

  // All aggregate values except for COUNT start out with NULL
  // (so that SUM(<col>) stays NULL if <col> only contains NULL values).
  for (int i = 0; i < aggregate_exprs_.size(); ++i, ++slot_d) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    if (agg_expr->op() == TExprOperator::AGG_COUNT) {
      // we're only aggregating into bigint slots and never return NULL
      *reinterpret_cast<int64_t*>(agg_tuple->GetSlot((*slot_d)->tuple_offset())) = 0;
    } else {
      agg_tuple->SetNull((*slot_d)->null_indicator_offset());
    }
  }
  return agg_tuple;
}

template <typename T>
void UpdateMinSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = min(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateMaxSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = max(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateSumSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot += *static_cast<T*>(value);
  }
}

void AggregationNode::UpdateAggTuple(Tuple* tuple, TupleRow* row) {
  vector<SlotDescriptor*>::const_iterator slot_d =
      agg_tuple_desc_->slots().begin() + grouping_exprs_.size();
  for (vector<Expr*>::iterator expr = aggregate_exprs_.begin();
       expr != aggregate_exprs_.end(); ++expr, ++slot_d) {
    void* slot = tuple->GetSlot((*slot_d)->tuple_offset());
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

    // deal with COUNT(*) separately (no need to check the actual child expr value)
    if (agg_expr->op() == TExprOperator::AGG_COUNT && agg_expr->is_star()) {
      // we're only aggregating into bigint slots
      DCHECK_EQ((*slot_d)->type(), TYPE_BIGINT);
      ++*reinterpret_cast<int64_t*>(slot);
      continue;
    }

    // determine value of aggregate's child expr
    void* value = agg_expr->GetChild(0)->GetValue(row);
    if (value == NULL) {
      // NULLs don't get aggregated
      continue;
    }

    switch (agg_expr->op()) {
      case TExprOperator::AGG_COUNT:
        ++*reinterpret_cast<int64_t*>(slot);
        break;

      case TExprOperator::AGG_MIN:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMinSlot<bool>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TINYINT:
            UpdateMinSlot<int8_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_SMALLINT:
            UpdateMinSlot<int16_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_INT:
            UpdateMinSlot<int32_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_BIGINT:
            UpdateMinSlot<int64_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_FLOAT:
            UpdateMinSlot<float>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateMinSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TExprOperator::AGG_MAX:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMaxSlot<bool>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TINYINT:
            UpdateMaxSlot<int8_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_SMALLINT:
            UpdateMaxSlot<int16_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_INT:
            UpdateMaxSlot<int32_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_BIGINT:
            UpdateMaxSlot<int64_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_FLOAT:
            UpdateMaxSlot<float>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateMaxSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TExprOperator::AGG_SUM:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateSumSlot<bool>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TINYINT:
            UpdateSumSlot<int8_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_SMALLINT:
            UpdateSumSlot<int16_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_INT:
            UpdateSumSlot<int32_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_BIGINT:
            UpdateSumSlot<int64_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_FLOAT:
            UpdateSumSlot<float>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateSumSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      default:
        DCHECK(false) << "bad aggregate operator: " << agg_expr->op();
    }
  }
}

void AggregationNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode(tuple_id=" << agg_tuple_id_
       << " grouping_exprs=" << Expr::DebugString(grouping_exprs_)
       << " agg_exprs=" << Expr::DebugString(aggregate_exprs_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}
