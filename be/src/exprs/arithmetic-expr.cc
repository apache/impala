// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/arithmetic-expr.h"
#include "exprs/functions.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

ArithmeticExpr::ArithmeticExpr(const TExprNode& node)
  : Expr(node), op_(node.op) {
}

// TODO: replace this with a generic function registry
// (registered by opcode and parameter types)
Status ArithmeticExpr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  Expr::Prepare(state, row_desc);
  DCHECK(type_ != INVALID_TYPE);
  DCHECK_LE(children_.size(), 2);
  DCHECK(children_.size() == 1 || children_[0]->type() == children_[1]->type());
  switch (op_) {
    case TExprOperator::MULTIPLY:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_double;
          return Status::OK;
        default:
          DCHECK(false) << "bad MULTIPLY type: " << type();
      }
      return Status::OK;

    case TExprOperator::DIVIDE:
      // in "<expr> / <expr>", operands are always cast to double
      assert(type_ == TYPE_DOUBLE
          && children_[0]->type() == TYPE_DOUBLE 
          && children_[1]->type() == TYPE_DOUBLE);
      compute_function_ = GetValueFunctions::ArithmeticExpr_divide_double;
      return Status::OK;

    case TExprOperator::MOD:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad DIVIDE type: " << type();
      }
      return Status::OK;

    case TExprOperator::INT_DIVIDE:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad INT_DIVIDE type: " << type();
      }
      return Status::OK;

    case TExprOperator::PLUS:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_double;
          return Status::OK;
        default:
          DCHECK(false) << "bad PLUS type: " << type();
      }
      return Status::OK;

    case TExprOperator::MINUS:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_double;
          return Status::OK;
        default:
          DCHECK(false) << "bad MINUS type: " << type();
      }
      return Status::OK;

    case TExprOperator::BITAND:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad BITAND type: " << type();
      }
      return Status::OK;

    case TExprOperator::BITOR:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad BITOR type: " << type();
      }
      return Status::OK;

    case TExprOperator::BITXOR:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad BITXOR type: " << type();
      }
      return Status::OK;

    case TExprOperator::BITNOT:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_long;
          return Status::OK;
        default:
          DCHECK(false) << "bad BITNOT type: " << type();
      }
      return Status::OK;
    default:
      DCHECK(false) << "bad arithmetic op: " << op_;
  }
  return Status::OK;
}

string ArithmeticExpr::DebugString() const {
  stringstream out;
  out << "ArithmeticExpr(op=" << op_ << " " << Expr::DebugString() << ")";
  return out.str();
}

}
