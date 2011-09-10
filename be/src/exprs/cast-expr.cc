// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "exprs/cast-expr.h"
#include "exprs/functions.h"

#include "gen-cpp/Exprs_types.h"

using namespace std;

namespace impala {

// TODO: generate cast eval functions between all legal combinations of source
// and target type

CastExpr::CastExpr(const TExprNode& node)
  : Expr(node) {
}

Status CastExpr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  Expr::Prepare(state, row_desc);
  DCHECK(type_ != INVALID_TYPE);
  DCHECK_LE(children_.size(), 1);
  switch (children_[0]->type()) {
    case TYPE_TINYINT:
      switch (type_) {
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_char_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_char_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_char_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_char_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_char_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_char_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_SMALLINT:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_short_char;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_short_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_short_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_short_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_short_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_short_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_INT:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_int_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_int_short;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_int_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_int_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_int_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_int_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_BIGINT:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_long_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_long_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_long_int;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_long_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_long_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_long_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_FLOAT:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_float_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_float_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_float_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_float_long;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_float_double;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_float_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_DOUBLE:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_double_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_double_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_double_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_double_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_double_float;
          return Status::OK;
        case TYPE_STRING:
          compute_function_ = GetValueFunctions::Cast_double_StringValue;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    case TYPE_STRING:
      switch (type_) {
        case TYPE_TINYINT:
          compute_function_ = GetValueFunctions::Cast_StringValue_char;
          return Status::OK;
        case TYPE_SMALLINT:
          compute_function_ = GetValueFunctions::Cast_StringValue_short;
          return Status::OK;
        case TYPE_INT:
          compute_function_ = GetValueFunctions::Cast_StringValue_int;
          return Status::OK;
        case TYPE_BIGINT:
          compute_function_ = GetValueFunctions::Cast_StringValue_long;
          return Status::OK;
        case TYPE_FLOAT:
          compute_function_ = GetValueFunctions::Cast_StringValue_float;
          return Status::OK;
        case TYPE_DOUBLE:
          compute_function_ = GetValueFunctions::Cast_StringValue_double;
          return Status::OK;
        default:
          DCHECK(false) << "bad cast type: " << TypeToString(type_);
      }
      return Status::OK;

    default:
      DCHECK(false) << "bad cast child type: " << TypeToString(children_[0]->type());
  }
  return Status::OK;
}

string CastExpr::DebugString() const {
  stringstream out;
  out << "CastExpr(" << Expr::DebugString() << ")";
  return out.str();
}

}
