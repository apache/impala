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

#include "exprs/decimal-literal.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"
#include "util/decimal-util.h"

using namespace llvm;
using namespace std;

namespace impala {

DecimalLiteral::DecimalLiteral(const TExprNode& node)
  : Expr(node) {
  DCHECK(type_.type == TYPE_DECIMAL) << type_;
  const uint8_t* buffer =
      reinterpret_cast<const uint8_t*>(&node.decimal_literal.value[0]);
  int len = node.decimal_literal.value.size();
  DCHECK_GT(len, 0);

  switch (type().GetByteSize()) {
    case 4:
      DCHECK_LE(len, 4);
      DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &result_.decimal4_val);
      break;
    case 8:
      DCHECK_LE(len, 8);
      DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &result_.decimal8_val);
      break;
    case 16:
      DCHECK_LE(len, 16);
      DecimalUtil::DecodeFromFixedLenByteArray(buffer, len, &result_.decimal16_val);
      break;
    default:
      DCHECK(false) << "DecimalLiteral ctor: bad type: " << type_.DebugString();
  }
}

void* DecimalLiteral::ReturnDecimal4Value(Expr* e, TupleRow* row) {
  return &reinterpret_cast<DecimalLiteral*>(e)->result_.decimal4_val;
}

void* DecimalLiteral::ReturnDecimal8Value(Expr* e, TupleRow* row) {
  return &reinterpret_cast<DecimalLiteral*>(e)->result_.decimal8_val;
}

void* DecimalLiteral::ReturnDecimal16Value(Expr* e, TupleRow* row) {
  return &reinterpret_cast<DecimalLiteral*>(e)->result_.decimal16_val;
}

Status DecimalLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  switch (type().GetByteSize()) {
    case 4:
      compute_fn_ = ReturnDecimal4Value;
      break;
    case 8:
      compute_fn_ = ReturnDecimal8Value;
      break;
    case 16:
      compute_fn_ = ReturnDecimal16Value;
      break;
    default:
      DCHECK(false) << "DecimalLiteral::DebugString(): bad type: " << type_.DebugString();
  }
  return Status::OK;
}

string DecimalLiteral::DebugString() const {
  stringstream out;
  out << "DecimalLiteral(type=" << type_ << " value=";
  switch (type().GetByteSize()) {
    case 4:
      out << result_.decimal4_val.ToString(type());
      break;
    case 8:
      out << result_.decimal8_val.ToString(type());
      break;
    case 16:
      out << result_.decimal16_val.ToString(type());
      break;
    default:
      DCHECK(false) << "DecimalLiteral::DebugString(): bad type: " << type_.DebugString();
  }
  out << ")";
  return out.str();
}

}
