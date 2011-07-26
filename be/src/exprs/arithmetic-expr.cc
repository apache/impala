// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <glog/logging.h>

#include "exprs/arithmetic-expr.h"
#include "exprs/functions.h"

#include "gen-cpp/Exprs_types.h"

namespace impala {

ArithmeticExpr::ArithmeticExpr(const TExprNode& node)
  : Expr(node), op_(node.op) {
}

// TODO: replace this with a generic function registry
// (registered by opcode and parameter types)
void ArithmeticExpr::Prepare(RuntimeState* state) {
  // assert(type_ != INVALID_TYPE);
  // assert(children_.size() <= 2);
  // assert(children_.size() == 1 || chilren_[0].type() == children_[1].type());
  switch (op_) {
    case TExprOperator::MULTIPLY:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_long;
          return;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_float;
          return;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_multiply_double;
          return;
        default:
          DCHECK(false) << "bad MULTIPLY type: " << type();
      }
      return;

    case TExprOperator::DIVIDE:
      // in "<expr> / <expr>", operands are always cast to double
      assert(type_ == TYPE_DOUBLE
          && children_[0]->type() == TYPE_DOUBLE 
          && children_[1]->type() == TYPE_DOUBLE);
      compute_function_ = GetValueFunctions::ArithmeticExpr_divide_double;
      return;

    case TExprOperator::MOD:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_mod_long;
          return;
        default:
          DCHECK(false) << "bad DIVIDE type: " << type();
      }
      return;

    case TExprOperator::INT_DIVIDE:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_divide_long;
          return;
        default:
          DCHECK(false) << "bad INT_DIVIDE type: " << type();
      }
      return;

    case TExprOperator::PLUS:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_long;
          return;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_float;
          return;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_add_double;
          return;
        default:
          DCHECK(false) << "bad PLUS type: " << type();
      }
      return;

    case TExprOperator::MINUS:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_long;
          return;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_float;
          return;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::ArithmeticExpr_subtract_double;
          return;
        default:
          DCHECK(false) << "bad MINUS type: " << type();
      }
      return;

    case TExprOperator::BITAND:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitand_long;
          return;
        default:
          DCHECK(false) << "bad BITAND type: " << type();
      }
      return;

    case TExprOperator::BITOR:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitor_long;
          return;
        default:
          DCHECK(false) << "bad BITOR type: " << type();
      }
      return;

    case TExprOperator::BITXOR:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitxor_long;
          return;
        default:
          DCHECK(false) << "bad BITXOR type: " << type();
      }
      return;

    case TExprOperator::BITNOT:
      switch (type()) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_char;
          return;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_short;
          return;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_int;
          return;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::ArithmeticExpr_bitnot_long;
          return;
        default:
          DCHECK(false) << "bad BITNOT type: " << type();
      }
      return;
    default:
      DCHECK(false) << "bad arithmetic op: " << op_;
  }
}

}
