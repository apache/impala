// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/binary-predicate.h"
#include "exprs/functions.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

BinaryPredicate::BinaryPredicate(const TExprNode& node)
  : Predicate(node), op_(node.op) {
}

Status BinaryPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  Expr::Prepare(state, row_desc);
  PrimitiveType op_type = children_[0]->type();
  DCHECK(type_ != INVALID_TYPE);
  DCHECK_EQ(children_.size(), 2);
  switch (op_) {
    case TExprOperator::EQ:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_eq_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad EQ type: " << TypeToString(op_type);
      }
      return Status::OK;

    case TExprOperator::NE:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_ne_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad NE type: " << TypeToString(op_type);
      }
      return Status::OK;

    case TExprOperator::LE:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_le_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad LE type: " << TypeToString(op_type);
      }
      return Status::OK;

    case TExprOperator::GE:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_ge_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad GE type: " << TypeToString(op_type);
      }
      return Status::OK;

    case TExprOperator::LT:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_lt_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad LT type: " << TypeToString(op_type);
      }
      return Status::OK;

    case TExprOperator::GT:
      switch (op_type) {
        case TYPE_BOOLEAN:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_bool;
          return Status::OK;
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::BinaryPredicate_gt_fn_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad GT type: " << TypeToString(op_type);
      }
      return Status::OK;

    default:
      DCHECK(false) << "bad binary predicate op: " << op_;
  }
  return Status::OK;
}

string BinaryPredicate::DebugString() const {
  stringstream out;
  out << "BinaryPredicate(op=" << op_ << " " << Expr::DebugString() << ")";
  return out.str();
}

}
