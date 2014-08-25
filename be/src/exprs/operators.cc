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

#include "exprs/operators.h"
#include "exprs/anyval-util.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

#define BINARY_OP_FN(NAME, TYPE, OP) \
  TYPE Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return TYPE::null();\
    return TYPE(v1.val OP v2.val);\
  }

#define BINARY_OP_CHECK_ZERO_FN(NAME, TYPE, OP) \
  TYPE Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null || v2.val == 0) return TYPE::null();\
    return TYPE(v1.val OP v2.val);\
  }

#define BITNOT_FN(TYPE)\
  TYPE Operators::Bitnot_##TYPE(FunctionContext* c, const TYPE& v) {\
    if (v.is_null) return TYPE::null();\
    return TYPE(~v.val);\
  }

#define BINARY_PREDICATE_NUMERIC_FN(NAME, TYPE, OP) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    return BooleanVal(v1.val OP v2.val);\
  }

#define BINARY_PREDICATE_NONNUMERIC_FN(NAME, TYPE, IMPALA_TYPE, OP) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    IMPALA_TYPE iv1 = IMPALA_TYPE::From##TYPE(v1);\
    IMPALA_TYPE iv2 = IMPALA_TYPE::From##TYPE(v2);\
    return BooleanVal(iv1 OP iv2);\
  }

#define BINARY_PREDICATE_CHAR(NAME, OP) \
  BooleanVal Operators::NAME##_Char_Char(\
      FunctionContext* c, const StringVal& v1, const StringVal& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    const ColumnType& v1_type = AnyValUtil::TypeDescToColumnType(*c->GetArgType(0));\
    const ColumnType& v2_type = AnyValUtil::TypeDescToColumnType(*c->GetArgType(1));\
    StringValue iv1 = StringValue::FromStringVal(v1);\
    StringValue iv2 = StringValue::FromStringVal(v2);\
    iv1.len = StringValue::UnpaddedCharLength(iv1.ptr, v1_type.len);\
    iv2.len = StringValue::UnpaddedCharLength(iv2.ptr, v2_type.len);\
    return BooleanVal(iv1 OP iv2);\
  }

#define BINARY_OP_NUMERIC_TYPES(NAME, OP) \
  BINARY_OP_FN(NAME, TinyIntVal, OP); \
  BINARY_OP_FN(NAME, SmallIntVal, OP);\
  BINARY_OP_FN(NAME, IntVal, OP);\
  BINARY_OP_FN(NAME, BigIntVal, OP);\
  BINARY_OP_FN(NAME, FloatVal, OP);\
  BINARY_OP_FN(NAME, DoubleVal, OP);

#define BINARY_OP_INT_TYPES(NAME, OP) \
  BINARY_OP_FN(NAME, TinyIntVal, OP); \
  BINARY_OP_FN(NAME, SmallIntVal, OP);\
  BINARY_OP_FN(NAME, IntVal, OP);\
  BINARY_OP_FN(NAME, BigIntVal, OP);\

#define BINARY_OP_CHECK_ZERO_INT_TYPES(NAME, OP) \
  BINARY_OP_CHECK_ZERO_FN(NAME, TinyIntVal, OP); \
  BINARY_OP_CHECK_ZERO_FN(NAME, SmallIntVal, OP);\
  BINARY_OP_CHECK_ZERO_FN(NAME, IntVal, OP);\
  BINARY_OP_CHECK_ZERO_FN(NAME, BigIntVal, OP);\

#define BINARY_PREDICATE_ALL_TYPES(NAME, OP) \
  BINARY_PREDICATE_NUMERIC_FN(NAME, BooleanVal, OP); \
  BINARY_PREDICATE_NUMERIC_FN(NAME, TinyIntVal, OP); \
  BINARY_PREDICATE_NUMERIC_FN(NAME, SmallIntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, IntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, BigIntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, FloatVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, DoubleVal, OP);\
  BINARY_PREDICATE_NONNUMERIC_FN(NAME, StringVal, StringValue, OP);\
  BINARY_PREDICATE_NONNUMERIC_FN(NAME, TimestampVal, TimestampValue, OP);\
  BINARY_PREDICATE_CHAR(NAME, OP);

namespace impala {

BINARY_OP_NUMERIC_TYPES(Add, +);
BINARY_OP_NUMERIC_TYPES(Subtract, -);
BINARY_OP_NUMERIC_TYPES(Multiply, *);

BINARY_OP_FN(Divide, DoubleVal, /);

BINARY_OP_CHECK_ZERO_INT_TYPES(Int_divide, /);
BINARY_OP_CHECK_ZERO_INT_TYPES(Mod, %);
BINARY_OP_INT_TYPES(Bitand, &);
BINARY_OP_INT_TYPES(Bitxor, ^);
BINARY_OP_INT_TYPES(Bitor, |);

BITNOT_FN(TinyIntVal);
BITNOT_FN(SmallIntVal);
BITNOT_FN(IntVal);
BITNOT_FN(BigIntVal);

BINARY_PREDICATE_ALL_TYPES(Eq, ==);
BINARY_PREDICATE_ALL_TYPES(Ne, !=);
BINARY_PREDICATE_ALL_TYPES(Gt, >);
BINARY_PREDICATE_ALL_TYPES(Lt, <);
BINARY_PREDICATE_ALL_TYPES(Ge, >=);
BINARY_PREDICATE_ALL_TYPES(Le, <=);

} // namespace impala
