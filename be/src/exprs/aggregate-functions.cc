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

#include "exprs/aggregate-functions.h"

#include <math.h>
#include <sstream>

#include "common/logging.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "exprs/anyval-util.h"

using namespace std;

// TODO: this file should be cross compiled and then all of the builtin
// aggregate functions will have a codegen enabled path. Then we can remove
// the custom code in aggregation node.
namespace impala {

// Delimiter to use if the separator is NULL.
static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

// Hyperloglog precision. Default taken from paper. Doesn't seem to matter very
// much when between [6,12]
const int HLL_PRECISION = 10;

void AggregateFunctions::InitNull(FunctionContext*, AnyVal* dst) {
  dst->is_null = true;
}

template<typename T>
void AggregateFunctions::InitZero(FunctionContext*, T* dst) {
  dst->is_null = false;
  dst->val = 0;
}

template<>
void AggregateFunctions::InitZero(FunctionContext*, DecimalVal* dst) {
  *dst = DecimalVal();
  dst->is_null = false;
}

StringVal AggregateFunctions::StringValSerializeOrFinalize(
    FunctionContext* ctx, const StringVal& src) {
  if (src.is_null) return src;
  StringVal result(ctx, src.len);
  memcpy(result.ptr, src.ptr, src.len);
  ctx->Free(src.ptr);
  return result;
}

void AggregateFunctions::CountUpdate(
    FunctionContext*, const AnyVal& src, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  if (!src.is_null) ++dst->val;
}

void AggregateFunctions::CountStarUpdate(FunctionContext*, BigIntVal* dst) {
  DCHECK(!dst->is_null);
  ++dst->val;
}

template<typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::Sum(FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DST_VAL>(ctx, dst);
  dst->val += src.val;
}

void AggregateFunctions::SumUpdate(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DecimalVal>(ctx, dst);
  const FunctionContext::TypeDesc* arg_desc = ctx->GetArgType(0);
  // Since the src and dst are guaranteed to be the same scale, we can just
  // do a simple add.
  if (arg_desc->precision <= 9) {
    dst->val16 += src.val4;
  } else if (arg_desc->precision <= 19) {
    dst->val16 += src.val8;
  } else {
    dst->val16 += src.val16;
  }
}

void AggregateFunctions::SumMerge(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) InitZero<DecimalVal>(ctx, dst);
  dst->val16 += src.val16;
}

template<typename T>
void AggregateFunctions::Min(FunctionContext*, const T& src, T* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val < dst->val) *dst = src;
}

template<typename T>
void AggregateFunctions::Max(FunctionContext*, const T& src, T* dst) {
  if (src.is_null) return;
  if (dst->is_null || src.val > dst->val) *dst = src;
}

void AggregateFunctions::InitNullString(FunctionContext* c, StringVal* dst) {
  dst->is_null = true;
  dst->ptr = NULL;
  dst->len = 0;
}

template<>
void AggregateFunctions::Min(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  if (dst->is_null ||
      StringValue::FromStringVal(src) < StringValue::FromStringVal(*dst)) {
    ctx->Free(dst->ptr);
    uint8_t* copy = ctx->Allocate(src.len);
    memcpy(copy, src.ptr, src.len);
    *dst = StringVal(copy, src.len);
  }
}

template<>
void AggregateFunctions::Max(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  if (dst->is_null ||
      StringValue::FromStringVal(src) > StringValue::FromStringVal(*dst)) {
    ctx->Free(dst->ptr);
    uint8_t* copy = ctx->Allocate(src.len);
    memcpy(copy, src.ptr, src.len);
    *dst = StringVal(copy, src.len);
  }
}

template<>
void AggregateFunctions::Min(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  const FunctionContext::TypeDesc* arg = ctx->GetArgType(0);
  DCHECK(arg != NULL);
  if (arg->precision <= 9) {
    if (dst->is_null || src.val4 < dst->val4) *dst = src;
  } else if (arg->precision <= 19) {
    if (dst->is_null || src.val8 < dst->val8) *dst = src;
  } else {
    if (dst->is_null || src.val16 < dst->val16) *dst = src;
  }
}

template<>
void AggregateFunctions::Max(FunctionContext* ctx,
    const DecimalVal& src, DecimalVal* dst) {
  if (src.is_null) return;
  const FunctionContext::TypeDesc* arg = ctx->GetArgType(0);
  DCHECK(arg != NULL);
  if (arg->precision <= 9) {
    if (dst->is_null || src.val4 > dst->val4) *dst = src;
  } else if (arg->precision <= 19) {
    if (dst->is_null || src.val8 > dst->val8) *dst = src;
  } else {
    if (dst->is_null || src.val16 > dst->val16) *dst = src;
  }
}

template<>
void AggregateFunctions::Min(FunctionContext*,
    const TimestampVal& src, TimestampVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) {
    *dst = src;
    return;
  }
  TimestampValue src_tv = TimestampValue::FromTimestampVal(src);
  TimestampValue dst_tv = TimestampValue::FromTimestampVal(*dst);
  if (src_tv < dst_tv) *dst = src;
}

template<>
void AggregateFunctions::Max(FunctionContext*,
    const TimestampVal& src, TimestampVal* dst) {
  if (src.is_null) return;
  if (dst->is_null) {
    *dst = src;
    return;
  }
  TimestampValue src_tv = TimestampValue::FromTimestampVal(src);
  TimestampValue dst_tv = TimestampValue::FromTimestampVal(*dst);
  if (src_tv > dst_tv) *dst = src;
}

void AggregateFunctions::StringConcat(FunctionContext* ctx, const StringVal& src,
      StringVal* result) {
  StringConcat(ctx, src, DEFAULT_STRING_CONCAT_DELIM, result);
}

void AggregateFunctions::StringConcat(FunctionContext* ctx, const StringVal& src,
      const StringVal& separator, StringVal* result) {
  if (src.is_null) return;
  if (result->is_null) {
    uint8_t* copy = ctx->Allocate(src.len);
    memcpy(copy, src.ptr, src.len);
    *result = StringVal(copy, src.len);
    return;
  }

  const StringVal* sep_ptr = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM :
      &separator;

  int new_size = result->len + sep_ptr->len + src.len;
  result->ptr = ctx->Reallocate(result->ptr, new_size);
  memcpy(result->ptr + result->len, sep_ptr->ptr, sep_ptr->len);
  result->len += sep_ptr->len;
  memcpy(result->ptr + result->len, src.ptr, src.len);
  result->len += src.len;
}

// Compute distinctpc and distinctpcsa using Flajolet and Martin's algorithm
// (Probabilistic Counting Algorithms for Data Base Applications)
// We have implemented two variants here: one with stochastic averaging (with PCSA
// postfix) and one without.
// There are 4 phases to compute the aggregate:
//   1. allocate a bitmap, stored in the aggregation tuple's output string slot
//   2. update the bitmap per row (UpdateDistinctEstimateSlot)
//   3. for distributed plan, merge the bitmaps from all the nodes
//      (UpdateMergeEstimateSlot)
//   4. compute the estimate using the bitmaps when all the rows are processed
//      (FinalizeEstimateSlot)
const static int NUM_PC_BITMAPS = 64; // number of bitmaps
const static int PC_BITMAP_LENGTH = 32; // the length of each bit map
const static float PC_THETA = 0.77351f; // the magic number to compute the final result

void AggregateFunctions::PcInit(FunctionContext* c, StringVal* dst) {
  // Initialize the distinct estimate bit map - Probabilistic Counting Algorithms for Data
  // Base Applications (Flajolet and Martin)
  //
  // The bitmap is a 64bit(1st index) x 32bit(2nd index) matrix.
  // So, the string length of 256 byte is enough.
  // The layout is:
  //   row  1: 8bit 8bit 8bit 8bit
  //   row  2: 8bit 8bit 8bit 8bit
  //   ...     ..
  //   ...     ..
  //   row 64: 8bit 8bit 8bit 8bit
  //
  // Using 32bit length, we can count up to 10^8. This will not be enough for Fact table
  // primary key, but once we approach the limit, we could interpret the result as
  // "every row is distinct".
  //
  // We use "string" type for DISTINCT_PC function so that we can use the string
  // slot to hold the bitmaps.
  dst->is_null = false;
  int str_len = NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8;
  dst->ptr = c->Allocate(str_len);
  dst->len = str_len;
  memset(dst->ptr, 0, str_len);
}

static inline void SetDistinctEstimateBit(uint8_t* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  // We need to convert Bitmap[alpha,index] into the index of the string.
  // alpha tells which of the 32bit we've to jump to.
  // index then lead us to the byte and bit.
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  int_bitmap[row_index] |= (1 << bit_index);
}

static inline bool GetDistinctEstimateBit(uint8_t* bitmap,
    uint32_t row_index, uint32_t bit_index) {
  uint32_t *int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
  return ((int_bitmap[row_index] & (1 << bit_index)) > 0);
}

template<typename T>
void AggregateFunctions::PcUpdate(FunctionContext* c, const T& input, StringVal* dst) {
  if (input.is_null) return;
  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. For simple averaging, we need to compute hash
  // values NUM_PC_BITMAPS times using NUM_PC_BITMAPS different hash functions (by using a
  // different seed).
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    uint32_t hash_value = AnyValUtil::Hash(input, *c->GetArgType(0), i);
    int bit_index = __builtin_ctz(hash_value);
    if (UNLIKELY(hash_value == 0)) bit_index = PC_BITMAP_LENGTH - 1;
    // Set bitmap[i, bit_index] to 1
    SetDistinctEstimateBit(dst->ptr, i, bit_index);
  }
}

template<typename T>
void AggregateFunctions::PcsaUpdate(FunctionContext* c, const T& input, StringVal* dst) {
  if (input.is_null) return;

  // Core of the algorithm. This is a direct translation of the code in the paper.
  // Please see the paper for details. Using stochastic averaging, we only need to
  // the hash value once for each row.
  uint32_t hash_value = AnyValUtil::Hash(input, *c->GetArgType(0), 0);
  uint32_t row_index = hash_value % NUM_PC_BITMAPS;

  // We want the zero-based position of the least significant 1-bit in binary
  // representation of hash_value. __builtin_ctz does exactly this because it returns
  // the number of trailing 0-bits in x (or undefined if x is zero).
  int bit_index = __builtin_ctz(hash_value / NUM_PC_BITMAPS);
  if (UNLIKELY(hash_value == 0)) bit_index = PC_BITMAP_LENGTH - 1;

  // Set bitmap[row_index, bit_index] to 1
  SetDistinctEstimateBit(dst->ptr, row_index, bit_index);
}

string DistinctEstimateBitMapToString(uint8_t* v) {
  stringstream debugstr;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    for (int j = 0; j < PC_BITMAP_LENGTH; ++j) {
      // print bitmap[i][j]
      debugstr << GetDistinctEstimateBit(v, i, j);
    }
    debugstr << "\n";
  }
  debugstr << "\n";
  return debugstr.str();
}

void AggregateFunctions::PcMerge(FunctionContext* c,
    const StringVal& src, StringVal* dst) {
  DCHECK(!src.is_null);
  DCHECK(!dst->is_null);
  DCHECK_EQ(src.len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);

  // Merge the bits
  // I think _mm_or_ps can do it, but perf doesn't really matter here. We call this only
  // once group per node.
  for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
    *(dst->ptr + i) |= *(src.ptr + i);
  }

  VLOG_ROW << "UpdateMergeEstimateSlot Src Bit map:\n"
           << DistinctEstimateBitMapToString(src.ptr);
  VLOG_ROW << "UpdateMergeEstimateSlot Dst Bit map:\n"
           << DistinctEstimateBitMapToString(dst->ptr);
}

double DistinceEstimateFinalize(const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);
  VLOG_ROW << "FinalizeEstimateSlot Bit map:\n"
           << DistinctEstimateBitMapToString(src.ptr);

  // We haven't processed any rows if none of the bits are set. Therefore, we have zero
  // distinct rows. We're overwriting the result in the same string buffer we've
  // allocated.
  bool is_empty = true;
  for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
    if (src.ptr[i] != 0) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) return 0;

  // Convert the bitmap to a number, please see the paper for details
  // In short, we count the average number of leading 1s (per row) in the bit map.
  // The number is proportional to the log2(1/NUM_PC_BITMAPS of  the actual number of
  // distinct).
  // To get the actual number of distinct, we'll do 2^avg / PC_THETA.
  // PC_THETA is a magic number.
  int sum = 0;
  for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
    int row_bit_count = 0;
    // Count the number of leading ones for each row in the bitmap
    // We could have used the build in __builtin_clz to count of number of leading zeros
    // but we first need to invert the 1 and 0.
    while (GetDistinctEstimateBit(src.ptr, i, row_bit_count) &&
        row_bit_count < PC_BITMAP_LENGTH) {
      ++row_bit_count;
    }
    sum += row_bit_count;
  }
  double avg = static_cast<double>(sum) / static_cast<double>(NUM_PC_BITMAPS);
  double result = pow(static_cast<double>(2), avg) / PC_THETA;
  return result;
}

StringVal AggregateFunctions::PcFinalize(FunctionContext* c, const StringVal& src) {
  double estimate = DistinceEstimateFinalize(src);
  int64_t result = estimate;
  // TODO: this should return bigint. this is a hack
  stringstream ss;
  ss << result;
  string str = ss.str();
  StringVal dst(c, str.length());
  memcpy(dst.ptr, str.c_str(), str.length());
  c->Free(src.ptr);
  return dst;
}

StringVal AggregateFunctions::PcsaFinalize(FunctionContext* c, const StringVal& src) {
  // When using stochastic averaging, the result has to be multiplied by NUM_PC_BITMAPS.
  double estimate = DistinceEstimateFinalize(src) * NUM_PC_BITMAPS;
  int64_t result = estimate;
  // TODO: this should return bigint. this is a hack
  stringstream ss;
  ss << result;
  string str = ss.str();
  StringVal dst = src;
  memcpy(dst.ptr, str.c_str(), str.length());
  dst.len = str.length();
  c->Free(src.ptr);
  return dst;
}

void AggregateFunctions::HllInit(FunctionContext* ctx, StringVal* dst) {
  int str_len = pow(2, HLL_PRECISION);
  dst->is_null = false;
  dst->ptr = ctx->Allocate(str_len);
  dst->len = str_len;
  memset(dst->ptr, 0, str_len);
}

template <typename T>
void AggregateFunctions::HllUpdate(FunctionContext* ctx, const T& src, StringVal* dst) {
  if (src.is_null) return;
  DCHECK(!dst->is_null);
  DCHECK_EQ(dst->len, pow(2, HLL_PRECISION));
  uint64_t hash_value =
      AnyValUtil::Hash64(src, *ctx->GetArgType(0), HashUtil::FNV64_SEED);
  if (hash_value != 0) {
    // Use the lower bits to index into the number of streams and then
    // find the first 1 bit after the index bits.
    int idx = hash_value % dst->len;
    uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_PRECISION) + 1;
    dst->ptr[idx] = ::max(dst->ptr[idx], first_one_bit);
  }
}

void AggregateFunctions::HllMerge(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
  DCHECK(!dst->is_null);
  DCHECK(!src.is_null);
  DCHECK_EQ(dst->len, pow(2, HLL_PRECISION));
  DCHECK_EQ(src.len, pow(2, HLL_PRECISION));
  for (int i = 0; i < src.len; ++i) {
    dst->ptr[i] = ::max(dst->ptr[i], src.ptr[i]);
  }
}

StringVal AggregateFunctions::HllFinalize(FunctionContext* ctx, const StringVal& src) {
  DCHECK(!src.is_null);
  DCHECK_EQ(src.len, pow(2, HLL_PRECISION));

  const int num_streams = pow(2, HLL_PRECISION);
  // Empirical constants for the algorithm.
  float alpha = 0;
  if (num_streams == 16) {
    alpha = 0.673f;
  } else if (num_streams == 32) {
    alpha = 0.697f;
  } else if (num_streams == 64) {
    alpha = 0.709f;
  } else {
    alpha = 0.7213f / (1 + 1.079f / num_streams);
  }

  float harmonic_mean = 0;
  int num_zero_registers = 0;
  for (int i = 0; i < src.len; ++i) {
    harmonic_mean += powf(2.0f, -src.ptr[i]);
    if (src.ptr[i] == 0) ++num_zero_registers;
  }
  harmonic_mean = 1.0f / harmonic_mean;
  int64_t estimate = alpha * num_streams * num_streams * harmonic_mean;

  if (num_zero_registers != 0) {
    // Estimated cardinality is too low. Hll is too inaccurate here, instead use
    // linear counting.
    estimate = num_streams * log(static_cast<float>(num_streams) / num_zero_registers);
  }

  // Output the estimate as ascii string
  stringstream out;
  out << estimate;
  string out_str = out.str();
  StringVal result_str(ctx, out_str.size());
  memcpy(result_str.ptr, out_str.c_str(), result_str.len);
  ctx->Free(src.ptr);
  return result_str;
}

// Stamp out the templates for the types we need.
template void AggregateFunctions::InitZero<BigIntVal>(FunctionContext*, BigIntVal* dst);

template void AggregateFunctions::Sum<TinyIntVal, BigIntVal>(
    FunctionContext*, const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Sum<SmallIntVal, BigIntVal>(
    FunctionContext*, const SmallIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Sum<IntVal, BigIntVal>(
    FunctionContext*, const IntVal& src, BigIntVal* dst);
template void AggregateFunctions::Sum<BigIntVal, BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Sum<FloatVal, DoubleVal>(
    FunctionContext*, const FloatVal& src, DoubleVal* dst);
template void AggregateFunctions::Sum<DoubleVal, DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);

template void AggregateFunctions::Min<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::Min<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::Min<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::Min<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::Min<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Min<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::Min<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::Min<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::Min<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::Max<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::Max<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::Max<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::Max<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::Max<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::Max<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::Max<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::Max<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::Max<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);

template void AggregateFunctions::PcUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::PcUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::PcsaUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);

template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const TimestampVal&, StringVal*);
template void AggregateFunctions::HllUpdate(
    FunctionContext*, const DecimalVal&, StringVal*);
}
