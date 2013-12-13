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

#include "exprs/math-functions.h"

#include <iomanip>
#include <sstream>
#include <math.h>

#include "exprs/expr.h"
#include "exprs/expr-inline.h"
#include "runtime/tuple-row.h"
#include "util/string-parser.h"
#include "opcode/functions.h"

using namespace std;

namespace impala { 

const char* MathFunctions::ALPHANUMERIC_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

void* MathFunctions::Pi(Expr* e, TupleRow* row) {
  e->result_.double_val = M_PI;
  return &e->result_.double_val;
}

void* MathFunctions::E(Expr* e, TupleRow* row) {
  e->result_.double_val = M_E;
  return &e->result_.double_val;
}

void* MathFunctions::Abs(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = fabs(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Sign(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.float_val = (*d > 0) ? 1.0f : ((*d < 0) ? -1.0f : 0.0f);
  return &e->result_.float_val;
}

void* MathFunctions::Sin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = sin(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Asin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = asin(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Cos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = cos(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Acos(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = acos(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Tan(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = tan(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Atan(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = atan(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Radians(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = *d * M_PI / 180.0;
  return &e->result_.double_val;
}

void* MathFunctions::Degrees(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = *d * 180 / M_PI;
  return &e->result_.double_val;
}

void* MathFunctions::Ceil(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = ceil(*d);
  return &e->result_.bigint_val;
}

void* MathFunctions::Floor(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = floor(*d);
  return &e->result_.bigint_val;
}

void* MathFunctions::Round(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.bigint_val = static_cast<int64_t>(*d + ((*d < 0) ? -0.5 : 0.5));
  return &e->result_.bigint_val;
}

void* MathFunctions::RoundUpTo(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  int32_t* scale = reinterpret_cast<int32_t*>(e->children()[1]->GetValue(row));
  if (d == NULL || scale == NULL) return NULL;
  e->result_.double_val = floor(*d * pow(10.0, *scale) + 0.5) / pow(10.0, *scale);
  e->output_scale_ = *scale;
  return &e->result_.double_val;
}

void* MathFunctions::Exp(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = exp(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Ln(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Log10(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log10(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Log2(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = log(*d) / log (2.0);
  return &e->result_.double_val;
}

void* MathFunctions::Log(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* base = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* val = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (val == NULL || base == NULL) return NULL;
  e->result_.double_val = log(*val) / log(*base);
  return &e->result_.double_val;
}

void* MathFunctions::Pow(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* base = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* exp = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (exp == NULL || base == NULL) return NULL;
  e->result_.double_val = pow(*base, *exp);
  return &e->result_.double_val;
}

void* MathFunctions::Sqrt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  double* d = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  if (d == NULL) return NULL;
  e->result_.double_val = sqrt(*d);
  return &e->result_.double_val;
}

void* MathFunctions::Rand(Expr* e, TupleRow* row) {
  // Use e->result_.int_val as state for the random number generator.
  // Cast is necessary, otherwise rand_r will complain.
  e->result_.int_val = rand_r(reinterpret_cast<uint32_t*>(&e->result_.int_val));
  // Normalize to [0,1].
  e->result_.double_val =
      static_cast<double>(e->result_.int_val) / static_cast<double>(RAND_MAX);
  return &e->result_.double_val;
}

void* MathFunctions::RandSeed(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int32_t* seed = reinterpret_cast<int32_t*>(e->children()[0]->GetValue(row));
  if (seed == NULL) return NULL;
  // Use e->result_.bool_val to indicate whether initial seed has been set.
  if (!e->result_.bool_val) {
    // Set user-defined seed upon this first call.
    e->result_.int_val = *seed;
    e->result_.bool_val = true;
  }
  return Rand(e, row);
}

void* MathFunctions::Bin(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  // Cast to an unsigned integer because it is compiler dependent
  // whether the sign bit will be shifted like a regular bit.
  // (logical vs. arithmetic shift for signed numbers)
  uint64_t* num = reinterpret_cast<uint64_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  uint64_t n = *num;
  const size_t max_bits = sizeof(uint64_t) * 8;
  char result[max_bits];
  uint32_t index = max_bits;
  do {
    result[--index] = '0' + (n & 1);
  } while (n >>= 1);
  StringValue val(result + index, max_bits - index);
  // Copies the data in result.
  e->result_.SetStringVal(val);
  return &e->result_.string_val;
}

void* MathFunctions::HexInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  int64_t* num = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  if (num == NULL) return NULL;
  stringstream ss;
  ss << hex << uppercase << *num;
  e->result_.SetStringVal(ss.str());
  return &e->result_.string_val;
}

void* MathFunctions::HexString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* s = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (s == NULL) return NULL;
  stringstream ss;
  ss << hex << uppercase << setw(2) << setfill('0');
  for (int i = 0; i < s->len; ++i) {
    ss << static_cast<int32_t>(s->ptr[i]);
  }
  e->result_.SetStringVal(ss.str());
  return &e->result_.string_val;
}

void* MathFunctions::Unhex(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  StringValue* s = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (s == NULL) return NULL;
  // For uneven number of chars return empty string like Hive does.
  if (s->len % 2 != 0) {
    e->result_.string_val.len = 0;
    e->result_.string_val.ptr = NULL;
    return &e->result_.string_val;
  }
  int result_len = s->len / 2;
  char result[result_len];
  int res_index = 0;
  int s_index = 0;
  while (s_index < s->len) {
    char c = 0;
    for (int j = 0; j < 2; ++j, ++s_index) {
      switch(s->ptr[s_index]) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
          c += (s->ptr[s_index] - '0') * ((j == 0) ? 16 : 1);
          break;
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
          // Map to decimal values [10, 15]
          c += (s->ptr[s_index] - 'A' + 10) * ((j == 0) ? 16 : 1);
          break;
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
          // Map to decimal [10, 15]
          c += (s->ptr[s_index] - 'a' + 10) * ((j == 0) ? 16 : 1);
          break;
        default:
          // Character not in hex alphabet, return empty string.
          e->result_.string_val.len = 0;
          e->result_.string_val.ptr = NULL;
          return &e->result_.string_val;
      }
    }
    result[res_index] = c;
    ++res_index;
  }
  StringValue val(result, result_len);
  // Copies the data in result.
  e->result_.SetStringVal(val);
  return &e->result_.string_val;
}

void* MathFunctions::ConvInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  int64_t* num = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  int8_t* src_base = reinterpret_cast<int8_t*>(e->children()[1]->GetValue(row));
  int8_t* dest_base = reinterpret_cast<int8_t*>(e->children()[2]->GetValue(row));
  if (num == NULL || src_base == NULL || dest_base == NULL) {
    return NULL;
  }
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(*src_base) < MIN_BASE || abs(*src_base) > MAX_BASE
      || abs(*dest_base) < MIN_BASE || abs(*dest_base) > MAX_BASE) {
    // Return NULL like Hive does.
    return NULL;
  }
  if (*src_base < 0 && *num >= 0) {
    // Invalid input.
    return NULL;
  }
  int64_t decimal_num = *num;
  if (*src_base != 10) {
    // Convert src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    if (!DecimalInBaseToDecimal(*num, *src_base, &decimal_num)) {
      // Handle overflow, setting decimal_num appropriately.
      HandleParseResult(*dest_base, &decimal_num, StringParser::PARSE_OVERFLOW);
    }
  }
  DecimalToBase(decimal_num, *dest_base, &e->result_);
  return &e->result_.string_val;
}

void* MathFunctions::ConvString(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 3);
  StringValue* num_str = reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  int8_t* src_base = reinterpret_cast<int8_t*>(e->children()[1]->GetValue(row));
  int8_t* dest_base = reinterpret_cast<int8_t*>(e->children()[2]->GetValue(row));
  if (num_str == NULL || src_base == NULL || dest_base == NULL) {
    return NULL;
  }
  // As in MySQL and Hive, min base is 2 and max base is 36.
  // (36 is max base representable by alphanumeric chars)
  // If a negative target base is given, num should be interpreted in 2's complement.
  if (abs(*src_base) < MIN_BASE || abs(*src_base) > MAX_BASE
      || abs(*dest_base) < MIN_BASE || abs(*dest_base) > MAX_BASE) {
    // Return NULL like Hive does.
    return NULL;
  }
  // Convert digits in num_str in src_base to decimal.
  StringParser::ParseResult parse_res;
  int64_t decimal_num = StringParser::StringToInt<int64_t>(num_str->ptr, num_str->len,
      *src_base, &parse_res);
  if (*src_base < 0 && decimal_num >= 0) {
    // Invalid input.
    return NULL;
  }
  if (!HandleParseResult(*dest_base, &decimal_num, parse_res)) {
    // Return 0 for invalid input strings like Hive does.
    StringValue val(const_cast<char*>("0"), 1);
    e->result_.SetStringVal(val);
    return &e->result_.string_val;
  }
  DecimalToBase(decimal_num, *dest_base, &e->result_);
  return &e->result_.string_val;
}

void MathFunctions::DecimalToBase(int64_t src_num, int8_t dest_base,
    ExprValue* expr_val) {
  // Max number of digits of any base (base 2 gives max digits), plus sign.
  const size_t max_digits = sizeof(uint64_t) * 8 + 1;
  char buf[max_digits];
  int32_t result_len = 0;
  int32_t buf_index = max_digits - 1;
  uint64_t temp_num;
  if (dest_base < 0) {
    // Dest base is negative, treat src_num as signed.
    temp_num = abs(src_num);
  } else {
    // Dest base is positive. We must interpret src_num in 2's complement.
    // Convert to an unsigned int to properly deal with 2's complement conversion.
    temp_num = static_cast<uint64_t>(src_num);
  }
  int abs_base = abs(dest_base);
  do {
    buf[buf_index] = ALPHANUMERIC_CHARS[temp_num % abs_base];
    temp_num /= abs_base;
    --buf_index;
    ++result_len;
  } while (temp_num > 0);
  // Add optional sign.
  if (src_num < 0 && dest_base < 0) {
    buf[buf_index] = '-';
    ++result_len;
  }
  StringValue val(buf + max_digits - result_len, result_len);
  // Copies the data in result.
  expr_val->SetStringVal(val);
}

bool MathFunctions::DecimalInBaseToDecimal(int64_t src_num, int8_t src_base,
    int64_t* result) {
  uint64_t temp_num = abs(src_num);
  int32_t place = 1;
  *result = 0;
  do {
    int32_t digit = temp_num % 10;
    // Reset result if digit is not representable in src_base.
    if (digit >= src_base) {
      *result = 0;
      place = 1;
    } else {
      *result += digit * place;
      place *= src_base;
      // Overflow.
      if (UNLIKELY(*result < digit)) {
        return false;
      }
    }
    temp_num /= 10;
  } while (temp_num > 0);
  *result = (src_num < 0) ? -(*result) : *result;
  return true;
}

bool MathFunctions::HandleParseResult(int8_t dest_base, int64_t* num,
    StringParser::ParseResult parse_res) {
  // On overflow set special value depending on dest_base.
  // This is consistent with Hive and MySQL's behavior.
  if (parse_res == StringParser::PARSE_OVERFLOW) {
    if (dest_base < 0) {
      *num = -1;
    } else {
      *num = numeric_limits<uint64_t>::max();
    }
  } else if (parse_res == StringParser::PARSE_FAILURE) {
    // Some other error condition.
    return false;
  }
  return true;
}

void* MathFunctions::PmodBigInt(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  int64_t* a = reinterpret_cast<int64_t*>(e->children()[0]->GetValue(row));
  int64_t* b = reinterpret_cast<int64_t*>(e->children()[1]->GetValue(row));
  if (a == NULL || b == NULL) return NULL;
  e->result_.bigint_val = ((*a % *b) + *b) % *b;
  return &e->result_.bigint_val;
}

void* MathFunctions::PmodDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  double* a = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* b = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (a == NULL || b == NULL) return NULL;
  e->result_.double_val = fmod(fmod(*a, *b) + *b, *b);
  return &e->result_.double_val;
}

void* MathFunctions::FmodFloat(Expr* e, TupleRow* row) {
  float* val1 = reinterpret_cast<float*>(e->children()[0]->GetValue(row));
  float* val2 = reinterpret_cast<float*>(e->children()[1]->GetValue(row));
  if (val1 == NULL || val2 == NULL || *val2 == 0) return NULL;
  e->result_.float_val = fmodf(*val1, *val2);
  return &e->result_.float_val;
}

void* MathFunctions::FmodDouble(Expr* e, TupleRow* row) {
  double* val1 = reinterpret_cast<double*>(e->children()[0]->GetValue(row));
  double* val2 = reinterpret_cast<double*>(e->children()[1]->GetValue(row));
  if (val1 == NULL || val2 == NULL || *val2 == 0) return NULL;
  e->result_.double_val = fmod(*val1, *val2);
  return &e->result_.double_val;
}

template <typename T> void* MathFunctions::Positive(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  T* i = reinterpret_cast<T*>(e->children()[0]->GetValue(row));
  if (i == NULL) return NULL;
  return e->result_.Set(*i);
}

template <typename T> void* MathFunctions::Negative(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  T* i = reinterpret_cast<T*>(e->children()[0]->GetValue(row));
  if (i == NULL) return NULL;
  return e->result_.Set(-*i);
}

void* MathFunctions::QuotientDouble(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  double* val1 = reinterpret_cast<double*>(op1->GetValue(row));
  Expr* op2 = e->children()[1];
  double* val2 = reinterpret_cast<double*>(op2->GetValue(row));
  if (val1 == NULL || val2 == NULL || static_cast<int64_t>(*val2) == 0) return NULL;
  e->result_.bigint_val = (static_cast<int64_t>(*val1) / static_cast<int64_t>(*val2));
  return &e->result_.bigint_val;
}

void* MathFunctions::QuotientBigInt(Expr* e, TupleRow* row) {
  return ComputeFunctions::Int_Divide_long_long(e, row);
}

template <typename T, bool ISLEAST>
void* MathFunctions::LeastGreatest(Expr* e, TupleRow* row) {
  DCHECK_GT(e->GetNumChildren(), 0);
  T* val = reinterpret_cast<T*>(e->children()[0]->GetValue(row));
  if (val == NULL) return NULL;
  T result_val = *val;
  int num_children = e->GetNumChildren();
  for (int i = 1; i < num_children; ++i) {
    val = reinterpret_cast<T*>(e->children()[i]->GetValue(row));
    if (val == NULL) return NULL;
    if (ISLEAST) {
      if (*val < result_val) result_val = *val;
    } else {
      if (*val > result_val) result_val = *val;
    }
  }
  return e->result_.Set(result_val);
}

template <bool ISLEAST>
void* MathFunctions::LeastGreatestString(Expr* e, TupleRow* row) {
  DCHECK_GT(e->GetNumChildren(), 0);
  StringValue* val =
      reinterpret_cast<StringValue*>(e->children()[0]->GetValue(row));
  if (val == NULL) return NULL;
  StringValue* result_val = val;
  int num_children = e->GetNumChildren();
  for (int i = 1; i < num_children; ++i) {
    val = reinterpret_cast<StringValue*>(e->children()[i]->GetValue(row));
    if (val == NULL) return NULL;
    if (ISLEAST) {
      if (val->Compare(*result_val) < 0) result_val = val;
    } else {
      if (val->Compare(*result_val) > 0) result_val = val;
    }
  }
  e->result_.string_val.ptr = result_val->ptr;
  e->result_.string_val.len = result_val->len;
  return &e->result_.string_val;
}

template void* MathFunctions::Positive<int8_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Positive<int16_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Positive<int32_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Positive<int64_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Positive<float>(Expr* e, TupleRow* row);
template void* MathFunctions::Positive<double>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<int8_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<int16_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<int32_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<int64_t>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<float>(Expr* e, TupleRow* row);
template void* MathFunctions::Negative<double>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int8_t, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int16_t, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int32_t, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int64_t, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<float, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<double, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<TimestampValue, true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatestString<true>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int8_t, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int16_t, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int32_t, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<int64_t, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<float, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<double, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatest<TimestampValue, false>(Expr* e, TupleRow* row);
template void* MathFunctions::LeastGreatestString<false>(Expr* e, TupleRow* row);

}

