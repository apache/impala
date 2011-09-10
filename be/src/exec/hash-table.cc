// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hash-table.h"

#include <boost/functional/hash.hpp>
#include <glog/logging.h>
#include <sstream>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"

using namespace std;
using namespace boost;

namespace impala {

HashTable::HashTable(const std::vector<Expr*>& build_exprs,
                     const std::vector<Expr*>& probe_exprs,
                     const RowDescriptor& build_row_desc,
                     int build_tuple_idx,
                     bool stores_nulls)
  : hash_fn_(this),
    equals_fn_(this),
    // TODO: how many buckets?
    hash_tbl_(new HashSet(17, hash_fn_, equals_fn_)),
    build_exprs_(build_exprs),
    probe_exprs_(probe_exprs),
    current_build_row_(NULL),
    current_probe_row_(NULL),
    build_row_data_(new Tuple*[build_row_desc.tuple_descriptors().size()]),
    build_row_(reinterpret_cast<TupleRow*>(build_row_data_.get())),
    build_tuple_idx_(build_tuple_idx),
    stores_nulls_(stores_nulls) {
}

size_t HashTable::HashFn::operator()(Tuple* const& t) const {
  const vector<Expr*>* exprs = NULL;
  TupleRow* row = NULL;
  if (t == NULL) {
    if (hash_tbl_->current_build_row_ != NULL) {
      DCHECK(hash_tbl_->current_probe_row_ == NULL);
      // compute hash value from build_exprs_ and current_build_row_
      row = hash_tbl_->current_build_row_;
      exprs = &hash_tbl_->build_exprs_;
    } else {
      DCHECK(hash_tbl_->current_probe_row_ != NULL);
      // compute hash value from probe_exprs_ and current_probe_row_
      row = hash_tbl_->current_probe_row_;
      exprs = &hash_tbl_->probe_exprs_;
    }
  } else {
    // evaluate build_exprs_ against t in context of build_row_
    // (not current_build_row_, which isn't set)
    hash_tbl_->build_row_->SetTuple(hash_tbl_->build_tuple_idx_, t);
    row = hash_tbl_->build_row_;
    exprs = &hash_tbl_->build_exprs_;
  }

  size_t seed = 0;
  for (int i = 0; i < exprs->size(); ++i) {
    const void* value;
    Expr* expr = (*exprs)[i];
    value = expr->GetValue(row);
    DCHECK(hash_tbl_->stores_nulls_ || t == NULL || value != NULL);
    // don't ignore NULLs; we want (1, NULL) to return a different hash
    // value than (NULL, 1)
    size_t hash_value =
        (value == NULL ? 0 : RawValue::GetHashValue(value, expr->type()));
    hash_combine(seed, hash_value);
  }
  return seed;
}

bool HashTable::EqualsFn::operator()(
    Tuple* const& t1, Tuple* const& t2) const {
  const vector<Expr*>* t1_exprs = NULL;
  TupleRow* t1_row = NULL;
  if (t1 == NULL) {
    if (hash_tbl_->current_build_row_ != NULL) {
      DCHECK(hash_tbl_->current_probe_row_ == NULL);
      // compute t1's value from build_exprs_ and current_build_row_
      t1_row = hash_tbl_->current_build_row_;
      t1_exprs = &hash_tbl_->build_exprs_;
    } else {
      DCHECK(hash_tbl_->current_probe_row_ != NULL);
      // compute t1's value from probe_exprs_ and current_probe_row_
      t1_row = hash_tbl_->current_probe_row_;
      t1_exprs = &hash_tbl_->probe_exprs_;
    }
  } else {
    // evaluate build_exprs_ against t1 in context of build_row_
    hash_tbl_->build_row_->SetTuple(hash_tbl_->build_tuple_idx_, t1);
    t1_row = hash_tbl_->build_row_;
    t1_exprs = &hash_tbl_->build_exprs_;
  }
  DCHECK(t2 != NULL);

  for (int i = 0; i < hash_tbl_->build_exprs_.size(); ++i) {
    const void* value1;
    value1 = (*t1_exprs)[i]->GetValue(t1_row);

    // t2 is always non-NULL and a resident tuple (ie, needs to be evaluated
    // in the context of build exprs)
    const void* value2;
    hash_tbl_->build_row_->SetTuple(hash_tbl_->build_tuple_idx_, t2);
    value2 = hash_tbl_->build_exprs_[i]->GetValue(hash_tbl_->build_row_);

    DCHECK_EQ((*t1_exprs)[i]->type(), hash_tbl_->build_exprs_[i]->type());
    if (value1 == NULL || value2 == NULL) {
      // if nulls are stored, they are always considered not-equal;
      // if they are stored, we pretend NULL == NULL
      if (!hash_tbl_->stores_nulls_ || value1 != NULL || value2 != NULL) return false;
    } else {
      if (RawValue::Compare(value1, value2, (*t1_exprs)[i]->type()) != 0) return false;
    }
  }
  return true;
} 

void HashTable::Insert(Tuple* t) {
  if (!stores_nulls_) {
    // check for nulls
    build_row_->SetTuple(build_tuple_idx_, t);
    for (int i = 0; i < build_exprs_.size(); ++i) {
      if (build_exprs_[i]->GetValue(build_row_) == NULL) return;
    }
  }
  hash_tbl_->insert(t);
}

void HashTable::Scan(TupleRow* probe_row, Iterator* it) {
  current_probe_row_ = probe_row;
  current_build_row_ = NULL;
  it->Reset(hash_tbl_->equal_range(NULL));
}

void HashTable::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "HashTbl(stores_nulls=" << (stores_nulls_ ? "true" : "false")
       << " build_tuple_idx=" << build_tuple_idx_
       << " build_exprs=" << Expr::DebugString(build_exprs_)
       << " probe_exprs=" << Expr::DebugString(probe_exprs_);
  *out << ")";
}

}
