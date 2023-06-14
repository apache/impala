// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/mask-functions.h"

#include <boost/locale/generator.hpp>
#include <boost/locale/utf8_codecvt.hpp>
#include <gutil/strings/substitute.h>
#include <openssl/err.h>
#include <openssl/sha.h>

#include "exprs/anyval-util.h"
#include "exprs/math-functions.h"
#include "exprs/string-functions.h"
#include "util/openssl-util.h"
#include "util/ubsan.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace boost::locale;

const static int CHAR_COUNT = 4;
const static int MASKED_UPPERCASE = 'X';
const static int MASKED_LOWERCASE = 'x';
const static int MASKED_DIGIT = 'n';
const static int MASKED_OTHER_CHAR = -1;
const static int MASKED_NUMBER = 1;
const static int MASKED_DAY_COMPONENT_VAL = 1;
const static int MASKED_MONTH_COMPONENT_VAL = 0;
const static int MASKED_YEAR_COMPONENT_VAL = 1;
const static int UNMASKED_VAL = -1;

/// Masks the given unicode code point depending on its range and the (optional) given
/// locale. By default, if no locale is provided, i.e. loc == nullptr,
/// lowercase/uppercase/digit characters are only recognized in ascii character set.
/// UNMASKED_VAL(-1) means keeping the original value.
/// Returns the masked code point.
static inline uint32_t MaskTransform(uint32_t val, int masked_upper_char,
    int masked_lower_char, int masked_digit_char, int masked_other_char,
    std::locale* loc = nullptr) {
  // Fast code path for masking ascii characters only.
  if (loc == nullptr) {
    if ('A' <= val && val <= 'Z') {
      if (masked_upper_char == UNMASKED_VAL) return val;
      return masked_upper_char;
    }
    if ('a' <= val && val <= 'z') {
      if (masked_lower_char == UNMASKED_VAL) return val;
      return masked_lower_char;
    }
    if ('0' <= val && val <= '9') {
      if (masked_digit_char == UNMASKED_VAL) return val;
      return masked_digit_char;
    }
    if (masked_other_char == UNMASKED_VAL) return val;
    return masked_other_char;
  }
  // Check facet existence to avoid predicates throws exception.
  DCHECK(std::has_facet<std::ctype<wchar_t>>(*loc))
      << "Facet not found for locale " << loc->name();
  if (isupper((wchar_t)val, *loc)) {
    if (masked_upper_char == UNMASKED_VAL) return val;
    return masked_upper_char;
  }
  if (islower((wchar_t)val, *loc)) {
    if (masked_lower_char == UNMASKED_VAL) return val;
    return masked_lower_char;
  }
  if (isdigit((wchar_t)val, *loc)) {
    if (masked_digit_char == UNMASKED_VAL) return val;
    return masked_digit_char;
  }
  if (masked_other_char == UNMASKED_VAL) return val;
  return masked_other_char;
}

/// Mask the substring in range [start, end) of the given string value. Using rules in
/// 'MaskTransform'. Indices are counted in bytes.
static StringVal MaskSubStr(FunctionContext* ctx, const StringVal& val,
    int start, int end, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  DCHECK_GE(start, 0);
  DCHECK_LT(start, end);
  DCHECK_LE(end, val.len);
  StringVal result(ctx, val.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  Ubsan::MemCpy(result.ptr, val.ptr, start);
  if (end < val.len) Ubsan::MemCpy(result.ptr + end, val.ptr + end, val.len - end);
  for (int i = start; i < end; ++i) {
    result.ptr[i] = MaskTransform(val.ptr[i], masked_upper_char, masked_lower_char,
        masked_digit_char, masked_other_char);
  }
  return result;
}

/// Checks whether the unicode code point is malformed, i.e. illegal or incomplete, and
/// warns if it is. Returns true if any warning is added.
static bool CheckAndWarnCodePoint(FunctionContext* ctx, uint32_t code_point) {
  if (code_point == utf::illegal || code_point == utf::incomplete) {
    ctx->AddWarning(Substitute("String contains $0 code point. Return NULL.",
        code_point == utf::illegal ? "illegal" : "incomplete").c_str());
    return true;
  }
  return false;
}

/// Mask the substring in range [start, end) of the given string value. Using rules in
/// 'MaskTransform'. Indices are counted in UTF-8 code points.
static StringVal MaskSubStrUtf8(FunctionContext* ctx, const StringVal& val,
    int start, int end, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  DCHECK_GE(start, 0);
  DCHECK_LT(start, end);
  DCHECK_LE(end, val.len);
  const char* p_start = reinterpret_cast<char*>(val.ptr);
  const char* p_end = p_start + val.len;
  const char* p = p_start;
  utf8_codecvt<char>::state_type cvt_state;
  int char_cnt = 0;
  // Skip leading 'start' code points. Leading bytes will be copied directly.
  while (char_cnt < start && p != p_end) {
    uint32_t codepoint = utf8_codecvt<char>::to_unicode(cvt_state, p, p_end);
    if (CheckAndWarnCodePoint(ctx, codepoint)) return StringVal::null();
    ++char_cnt;
  }
  // Calculating the result length in bytes.
  int result_bytes = p - p_start;
  int leading_bytes = result_bytes;
  // Collect code points at range [start, end - 1) and mask them.
  vector<uint32_t> masked_code_points;
  // Create unicode locale for checking upper/lower cases or digits.
  // TODO(quanlong): Avoid creating this everytime if this is time/resource-consuming.
  boost::locale::generator gen;
  unique_ptr<std::locale> loc = make_unique<std::locale>(gen("en_US.UTF-8"));
  // Check facet existence to avoid predicates throws exception.
  if (!std::has_facet<std::ctype<wchar_t>>(*loc)) {
    ctx->SetError("Cannot mask unicode strings since locale en_US.UTF-8 not found!");
    return StringVal();
  }
  while (char_cnt < end && p != p_end) {
    // Parse and get the first code point in string range [p, p_end).
    // 'to_unicode' will update the pointer 'p'.
    uint32_t codepoint = utf8_codecvt<char>::to_unicode(cvt_state, p, p_end);
    if (CheckAndWarnCodePoint(ctx, codepoint)) return StringVal::null();
    codepoint = MaskTransform(codepoint, masked_upper_char, masked_lower_char,
        masked_digit_char, masked_other_char, loc.get());
    masked_code_points.push_back(codepoint);
    result_bytes += utf::utf_traits<char>::width(codepoint);
    ++char_cnt;
  }
  // Trailing bytes will be copied directly without masking.
  int tail_len = p_end - p;
  result_bytes += tail_len;

  StringVal result(ctx, result_bytes);
  if (UNLIKELY(result.is_null)) return result;
  // Copy leading bytes.
  Ubsan::MemCpy(result.ptr, val.ptr, leading_bytes);
  // Converting masked code points to UTF-8 encoded bytes.
  char* ptr = reinterpret_cast<char*>(result.ptr) + leading_bytes;
  p_end = reinterpret_cast<char*>(result.ptr) + result_bytes;
  for (uint32_t c : masked_code_points) {
    uint32_t width = utf8_codecvt<char>::from_unicode(cvt_state, c, ptr, p_end);
    DCHECK(width != utf::illegal && width != utf::incomplete);
    ptr += width;
    DCHECK(ptr <= p_end);
  }
  // Copy trailing bytes.
  if (tail_len > 0) {
    DCHECK(ptr < p_end);
    Ubsan::MemCpy(ptr, val.ptr + val.len - tail_len, tail_len);
  }
  result.len = result_bytes;
  return result;
}

/// Counting code points in the UTF-8 encoded string using the same method, 'to_unicode',
/// as MaskSubStrUtf8 uses. So we can have a consistent behavior.
/// Returns -1 if the string contains malformed(illegal/incomplete) code points.
static int GetUtf8CodePointCount(FunctionContext* ctx, const StringVal& val) {
  utf8_codecvt<char>::state_type cvt_state;
  const char* p = reinterpret_cast<char*>(val.ptr);
  const char* p_end = p + val.len;
  int char_cnt = 0;
  while (p != p_end) {
    uint32_t c = utf8_codecvt<char>::to_unicode(cvt_state, p, p_end);
    if (c == utf::illegal || c == utf::incomplete) {
      ctx->SetError(Substitute("The $0-th code point $1 is $2",
          char_cnt, AnyValUtil::ToString(val),
          c == utf::illegal ? "illegal" : "incomplete").c_str());
      return -1;
    }
    ++char_cnt;
  }
  return char_cnt;
}

/// Mask the given string except the first 'un_mask_char_count' chars. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowFirstN.
static inline StringVal MaskShowFirstNImpl(FunctionContext* ctx, const StringVal& val,
    int un_mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  // To be consistent with Hive, negative char_count is treated as 0.
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  if (val.is_null || val.len == 0 || un_mask_char_count >= val.len) return val;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return MaskSubStr(ctx, val, un_mask_char_count, val.len, masked_upper_char,
        masked_lower_char, masked_digit_char, masked_other_char);
  }
  return MaskSubStrUtf8(ctx, val, un_mask_char_count, val.len, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

/// Mask the given string except the last 'un_mask_char_count' chars. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowLastN.
static inline StringVal MaskShowLastNImpl(FunctionContext* ctx, const StringVal& val,
    int un_mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  // To be consistent with Hive, negative char_count is treated as 0.
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  if (val.is_null || val.len == 0 || un_mask_char_count >= val.len) return val;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return MaskSubStr(ctx, val, 0, val.len - un_mask_char_count, masked_upper_char,
        masked_lower_char, masked_digit_char, masked_other_char);
  }
  int end = GetUtf8CodePointCount(ctx, val) - un_mask_char_count;
  if (end <= 0) return val;
  return MaskSubStrUtf8(ctx, val, 0, end, masked_upper_char, masked_lower_char,
      masked_digit_char, masked_other_char);
}

/// Mask the first 'mask_char_count' chars of the given string. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskFirstN.
static inline StringVal MaskFirstNImpl(FunctionContext* ctx, const StringVal& val,
    int mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  if (mask_char_count <= 0 || val.is_null || val.len == 0) return val;
  if (mask_char_count > val.len) mask_char_count = val.len;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return MaskSubStr(ctx, val, 0, mask_char_count, masked_upper_char, masked_lower_char,
        masked_digit_char, masked_other_char);
  }
  return MaskSubStrUtf8(ctx, val, 0, mask_char_count, masked_upper_char,
      masked_lower_char, masked_digit_char, masked_other_char);
}

/// Mask the last 'mask_char_count' chars of the given string. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskLastN.
static inline StringVal MaskLastNImpl(FunctionContext* ctx, const StringVal& val,
    int mask_char_count, int masked_upper_char, int masked_lower_char,
    int masked_digit_char, int masked_other_char) {
  if (mask_char_count <= 0 || val.is_null || val.len == 0) return val;
  if (mask_char_count > val.len) mask_char_count = val.len;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return MaskSubStr(ctx, val, val.len - mask_char_count, val.len, masked_upper_char,
        masked_lower_char, masked_digit_char, masked_other_char);
  }
  int start = GetUtf8CodePointCount(ctx, val) - mask_char_count;
  if (start < 0) start = 0;
  return MaskSubStrUtf8(ctx, val, start, val.len, masked_upper_char, masked_lower_char,
      masked_digit_char, masked_other_char);
}

/// Mask the whole given string. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMask.
static inline StringVal MaskImpl(FunctionContext* ctx, const StringVal& val,
    int masked_upper_char, int masked_lower_char, int masked_digit_char,
    int masked_other_char) {
  if (val.is_null || val.len == 0) return val;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return MaskSubStr(ctx, val, 0, val.len, masked_upper_char,
        masked_lower_char, masked_digit_char, masked_other_char);
  }
  return MaskSubStrUtf8(ctx, val, 0, val.len, masked_upper_char, masked_lower_char,
      masked_digit_char, masked_other_char);
}

static inline int GetNumDigits(int64_t val) {
  if (val == 0) return 1;
  if (val < 0) val = -val;
  int num_digits = 0;
  while (val != 0) {
    num_digits++;
    val /= 10;
  }
  return num_digits;
}

/// Mask the numeric value by replacing the digits with 'masked_number'. The first
/// 'un_mask_char_count' digits will be kept unchanged. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowFirstN.
static inline BigIntVal MaskShowFirstNImpl(const BigIntVal& val, int un_mask_digit_count,
    int masked_number) {
  if (val.is_null) return val;
  // To be consistent with Hive, illegal masked_number is treated as default value.
  if (masked_number < 0 || masked_number > 9) masked_number = MASKED_NUMBER;
  if (un_mask_digit_count < 0) un_mask_digit_count = 0;
  if (val.val == 0) return un_mask_digit_count > 0 ? 0 : masked_number;
  int64_t unsigned_val = val.val;
  // The sign won't be masked.
  if (val.val < 0) unsigned_val = -unsigned_val;
  int num_digits = GetNumDigits(unsigned_val);
  // Number of digits to mask from the end
  int mask_count = num_digits - un_mask_digit_count;
  if (mask_count <= 0) return val;
  int64_t result = 0;
  int64_t base = 1;
  for (int i = 0; i < mask_count; ++i) { // loop from end to start
    result += masked_number * base;
    base *= 10;
    unsigned_val /= 10;
  }
  result += unsigned_val * base;
  return {val.val < 0 ? -result : result};
}

/// Mask the numeric value by replacing the digits with 'masked_number'. The last
/// 'un_mask_char_count' digits will be kept unchanged. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowLastN.
static inline BigIntVal MaskShowLastNImpl(const BigIntVal& val, int un_mask_char_count,
    int masked_number) {
  if (val.is_null) return val;
  // To be consistent with Hive, illegal masked_number is treated as default value.
  if (masked_number < 0 || masked_number > 9) masked_number = MASKED_NUMBER;
  if (un_mask_char_count < 0) un_mask_char_count = 0;
  if (val.val == 0) return un_mask_char_count > 0 ? 0 : masked_number;
  int64_t unsigned_val = val.val;
  // The sign won't be masked.
  if (val.val < 0) unsigned_val = -unsigned_val;
  int num_digits = GetNumDigits(unsigned_val);
  if (num_digits <= un_mask_char_count) return val;
  int64_t base = 1;
  for (int i = 0; i < un_mask_char_count; ++i) base *= 10;
  int64_t result = unsigned_val % base;
  for (int i = un_mask_char_count; i < num_digits; ++i) {
    // It's possible that result overflows, e.g. val is 2^63-1 and masked_number is 9.
    // Just continue with the overflowed result to be consistent with Hive.
    result += masked_number * base;
    base *= 10;
  }
  return {val.val < 0 ? -result : result};
}

/// Mask the numeric value by replacing the first 'mask_char_count' digits with
/// 'masked_number'. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskFirstN.
static inline BigIntVal MaskFirstNImpl(const BigIntVal& val, int mask_char_count,
    int masked_number) {
  if (val.is_null) return val;
  int num_digits = GetNumDigits(val.val);
  if (num_digits < mask_char_count) mask_char_count = num_digits;
  return MaskShowLastNImpl(val, num_digits - mask_char_count, masked_number);
}

/// Mask the numeric value by replacing the last 'mask_char_count' digits with
/// 'masked_number'. Ported from
/// org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskLastN.
static inline BigIntVal MaskLastNImpl(const BigIntVal& val, int mask_char_count,
    int masked_number) {
  if (val.is_null) return val;
  int num_digits = GetNumDigits(val.val);
  if (num_digits < mask_char_count) mask_char_count = num_digits;
  return MaskShowFirstNImpl(val, num_digits - mask_char_count, masked_number);
}

/// Mask the Date value by replacing the day by 'day_value', the month by 'month_value',
/// and the year by 'year_value'. UNMASKED_VAL(-1) means keeping the original value.
/// Ported from org.apache.hadoop.hive.ql.udf.generic.GenericUDFMask.
static DateVal MaskImpl(FunctionContext* ctx, const DateVal& val, int day_value,
    int month_value, int year_value) {
  if (val.is_null) return val;
  if (day_value != UNMASKED_VAL && !(1 <= day_value && day_value <= 31)) {
    day_value = MASKED_DAY_COMPONENT_VAL;
  }
  if (month_value != UNMASKED_VAL && !(0 <= month_value && month_value < 12)) {
    month_value = MASKED_MONTH_COMPONENT_VAL;
  }
  if (year_value != UNMASKED_VAL && (year_value <= 0 || year_value >= 9999)) {
    // Repalce illegal year for DateValue with default value.
    year_value = MASKED_YEAR_COMPONENT_VAL;
  }
  int year, month, day;
  DateValue dv = DateValue::FromDateVal(val);
  // Extract year, month, day.
  if (!dv.ToYearMonthDay(&year, &month, &day)) return DateVal::null();

  if (year_value != UNMASKED_VAL) year = year_value;
  // In DateValue, month starts from 1, so increase 'month_value' by 1.
  if (month_value != UNMASKED_VAL) month = month_value + 1;
  if (day_value != UNMASKED_VAL) day = day_value;
  return DateValue(year, month, day).ToDateVal();
}

/// Gets the first character of 'str'. Returns 'default_value' if 'str' is empty.
/// In UTF-8 mode, the first code point is returned.
/// Otherwise, the first char is returned.
static inline uint32_t GetFirstChar(FunctionContext* ctx, const StringVal& str,
    uint32_t default_value) {
  // To be consistent with Hive, empty string is converted to default value. String with
  // length > 1 will only use its first char.
  if (str.len == 0) return default_value;
  if (!ctx->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) return str.ptr[0];

  utf8_codecvt<char>::state_type cvt_state;
  const char* p = reinterpret_cast<char*>(str.ptr);
  uint32_t c = utf8_codecvt<char>::to_unicode(cvt_state, p, p + str.len);
  if (c == utf::illegal || c == utf::incomplete) {
    string msg = Substitute("$0 unicode code point found in the beginning of $1",
        c == utf::illegal ? "Illegal" : "Incomplete", AnyValUtil::ToString(str));
    ctx->SetError(msg.c_str());
    return default_value;
  }
  return c;
}

/// Get digit (masked_number) from StringVal. Only accept digits or -1.
static inline bool GetDigitFromString(FunctionContext* ctx, const StringVal& str,
    int* res) {
  if (str.len != 1 || str.ptr[0] < '0' || str.ptr[0] > '9') {
    ctx->SetError(Substitute(
        "Can't convert '$0' to a valid masked_number. Valid values: 0-9.",
        AnyValUtil::ToString(str)).c_str());
    return false;
  }
  *res = str.ptr[0] - '0';
  return true;
}

/// MaskShowFirstN overloads for string value
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val) {
  return MaskShowFirstNImpl(ctx, val, CHAR_COUNT, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count) {
  int un_mask_char_count = char_count.val;
  return MaskShowFirstNImpl(ctx, val, un_mask_char_count, MASKED_UPPERCASE,
      MASKED_LOWERCASE, MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  return MaskShowFirstNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const StringVal& other_char) {
  return MaskShowFirstNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return MaskShowFirstN(ctx, val, char_count, upper_char, lower_char, digit_char,
      other_char);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return MaskShowFirstNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      other_char.val);
}
StringVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskShowFirstNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

/// MaskShowFirstN overloads for numeric value
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val) {
  return {MaskShowFirstNImpl(val, CHAR_COUNT, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskShowFirstNImpl(val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskShowFirstNImpl(val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskShowFirstNImpl(val, char_count.val, masked_number)};
}
BigIntVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskShowFirstNImpl(val, char_count.val, number_char.val)};
}

DateVal MaskFunctions::MaskShowFirstN(FunctionContext* ctx, const DateVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  // Currently we follow Hive's behavior to always mask date values to '0001-01-01'.
  // TODO: Implement this after HIVE-24641 is resolved.
  return Mask(ctx, val);
}

/// MaskShowLastN overloads for string value
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val) {
  return MaskShowLastNImpl(ctx, val, CHAR_COUNT, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count) {
  int un_mask_char_count = char_count.val;
  return MaskShowLastNImpl(ctx, val, un_mask_char_count, MASKED_UPPERCASE,
      MASKED_LOWERCASE, MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  return MaskShowLastNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const StringVal& other_char) {
  return MaskShowLastNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return MaskShowLastN(ctx, val, char_count, upper_char, lower_char, digit_char,
      other_char);
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return MaskShowLastNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      other_char.val);
}
StringVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskShowLastNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

/// MaskShowLastN overloads for numeric value
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const BigIntVal& val) {
  return {MaskShowLastNImpl(val, CHAR_COUNT, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskShowLastNImpl(val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskShowLastNImpl(val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskShowLastNImpl(val, char_count.val, masked_number)};
}
BigIntVal MaskFunctions::MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskShowLastNImpl(val, char_count.val, number_char.val)};
}

/// MaskFirstN overloads for string value
StringVal MaskFunctions::MaskFirstN(FunctionContext *ctx, const StringVal &val) {
  return MaskFirstNImpl(ctx, val, CHAR_COUNT, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count) {
  return MaskFirstNImpl(ctx, val, char_count.val, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  return MaskFirstNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const StringVal& other_char) {
  return MaskFirstNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return MaskFirstN(ctx, val, char_count, upper_char, lower_char, digit_char,
      other_char);
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return MaskFirstNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      other_char.val);
}
StringVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskFirstNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

/// MaskFirstN overloads for numeric value
BigIntVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const BigIntVal& val) {
  return {MaskFirstNImpl(val, CHAR_COUNT, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskFirstNImpl(val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskFirstNImpl(val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskFirstNImpl(val, char_count.val, masked_number)};
}
BigIntVal MaskFunctions::MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskFirstNImpl(val, char_count.val, number_char.val)};
}

/// MaskLastN overloads for string value
StringVal MaskFunctions::MaskLastN(FunctionContext *ctx, const StringVal &val) {
  return MaskLastNImpl(ctx, val, CHAR_COUNT, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count) {
  return MaskLastNImpl(ctx, val, char_count.val, MASKED_UPPERCASE, MASKED_LOWERCASE,
      MASKED_DIGIT, MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  return MaskLastNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const StringVal& other_char) {
  return MaskLastNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return MaskLastN(ctx, val, char_count, upper_char, lower_char, digit_char,
      other_char);
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return MaskLastNImpl(ctx, val, char_count.val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      other_char.val);
}
StringVal MaskFunctions::MaskLastN(FunctionContext* ctx, const StringVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return MaskLastNImpl(ctx, val, char_count.val, upper_char.val, lower_char.val,
      digit_char.val, other_char.val);
}

/// MaskLastN overloads for numeric value
BigIntVal MaskFunctions::MaskLastN(FunctionContext* ctx, const BigIntVal& val) {
  return {MaskLastNImpl(val, CHAR_COUNT, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count) {
  return {MaskLastNImpl(val, char_count.val, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::MaskLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskLastNImpl(val, char_count.val, number_char.val)};
}
BigIntVal MaskFunctions::MaskLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskLastNImpl(val, char_count.val, masked_number)};
}
BigIntVal MaskFunctions::MaskLastN(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char) {
  return {MaskLastNImpl(val, char_count.val, number_char.val)};
}

/// Mask() overloads for string value
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val) {
  return MaskImpl(ctx, val, MASKED_UPPERCASE, MASKED_LOWERCASE, MASKED_DIGIT,
      MASKED_OTHER_CHAR);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char) {
  return MaskImpl(ctx, val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return Mask(ctx, val, upper_char, lower_char, digit_char, other_char);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char) {
  return MaskImpl(ctx, val, upper_char.val, lower_char.val, digit_char.val,
      other_char.val);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return MaskImpl(ctx, val,
      GetFirstChar(ctx, upper_char, MASKED_UPPERCASE),
      GetFirstChar(ctx, lower_char, MASKED_LOWERCASE),
      GetFirstChar(ctx, digit_char, MASKED_DIGIT),
      other_char.val);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return Mask(ctx, val, upper_char, lower_char, digit_char, other_char);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return Mask(ctx, val, upper_char, lower_char, digit_char, other_char, number_char);
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const IntVal& upper_char, const IntVal& lower_char,
    const IntVal& digit_char, const StringVal& other_char) {
  return MaskImpl(ctx, val, upper_char.val, lower_char.val, digit_char.val,
      GetFirstChar(ctx, other_char, MASKED_OTHER_CHAR));
}
StringVal MaskFunctions::Mask(FunctionContext* ctx, const StringVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
    const IntVal& month_value, const IntVal& year_value) {
  return MaskImpl(ctx, val, upper_char.val, lower_char.val, digit_char.val,
      other_char.val);
}

/// Mask() overloads for Date value
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val) {
  return MaskImpl(ctx, val, MASKED_DAY_COMPONENT_VAL, MASKED_MONTH_COMPONENT_VAL,
      MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value) {
  return MaskImpl(ctx, val, day_value.val, MASKED_MONTH_COMPONENT_VAL,
      MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value) {
  return MaskImpl(ctx, val, day_value.val, month_value.val, MASKED_YEAR_COMPONENT_VAL);
}
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return MaskImpl(ctx, val, day_value.val, month_value.val, year_value.val);
}
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
    const IntVal& month_value, const IntVal& year_value) {
  return MaskImpl(ctx, val, day_value.val, month_value.val, year_value.val);
}
DateVal MaskFunctions::Mask(FunctionContext* ctx, const DateVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return MaskImpl(ctx, val, day_value.val, month_value.val, year_value.val);
}

/// Mask() overloads for numeric value
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val) {
  return {MaskShowFirstNImpl(val, 0, MASKED_NUMBER)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char) {
  return {MaskShowFirstNImpl(val, 0, number_char.val)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskShowFirstNImpl(val, 0, masked_number)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char) {
  return {MaskShowFirstNImpl(val, 0, number_char.val)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const StringVal& other_char,
    const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return {MaskShowFirstNImpl(val, 0, number_char.val)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  int masked_number;
  if (!GetDigitFromString(ctx, number_char, &masked_number)) return BigIntVal::null();
  return {MaskShowFirstNImpl(val, 0, masked_number)};
}
BigIntVal MaskFunctions::Mask(FunctionContext* ctx, const BigIntVal& val,
    const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
    const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
    const IntVal& month_value, const IntVal& year_value) {
  return {MaskShowFirstNImpl(val, 0, number_char.val)};
}
BooleanVal MaskFunctions::Mask(FunctionContext* ctx, const BooleanVal& val) {
  return BooleanVal::null();
}
BooleanVal MaskFunctions::Mask(FunctionContext* ctx, const BooleanVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return BooleanVal::null();
}
DoubleVal MaskFunctions::Mask(FunctionContext* ctx, const DoubleVal& val) {
  return DoubleVal::null();
}
DoubleVal MaskFunctions::Mask(FunctionContext* ctx, const DoubleVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return DoubleVal::null();
}
TimestampVal MaskFunctions::Mask(FunctionContext* ctx, const TimestampVal& val) {
  return TimestampVal::null();
}
TimestampVal MaskFunctions::Mask(FunctionContext* ctx, const TimestampVal& val,
    const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
    const IntVal& year_value) {
  return TimestampVal::null();
}

StringVal MaskFunctions::MaskHash(FunctionContext* ctx, const StringVal& val) {
  // Hive hash the value by sha256 and encoding it into a lower case hex string in
  // non FIPS mode. In FIPS enabled mode, it's required to use sha512 for mask hash.
  if (IsFIPSMode()) {
    StringVal sha512_hash(ctx, SHA512_DIGEST_LENGTH);
    if (UNLIKELY(sha512_hash.is_null)) return StringVal::null();
    discard_result(SHA512(val.ptr, val.len, sha512_hash.ptr));
    return StringFunctions::Lower(ctx, MathFunctions::HexString(ctx, sha512_hash));
  } else {
    StringVal sha256_hash(ctx, SHA256_DIGEST_LENGTH);
    if (UNLIKELY(sha256_hash.is_null)) return StringVal::null();
    discard_result(SHA256(val.ptr, val.len, sha256_hash.ptr));
    return StringFunctions::Lower(ctx, MathFunctions::HexString(ctx, sha256_hash));
  }
}
// For other types, the hash values are always NULL.
BigIntVal MaskFunctions::MaskHash(FunctionContext* ctx, const BigIntVal& val) {
  return BigIntVal::null();
}
DoubleVal MaskFunctions::MaskHash(FunctionContext* ctx, const DoubleVal& val) {
  return DoubleVal::null();
}
BooleanVal MaskFunctions::MaskHash(FunctionContext* ctx, const BooleanVal& val) {
  return BooleanVal::null();
}
TimestampVal MaskFunctions::MaskHash(FunctionContext* ctx, const TimestampVal& val) {
  return TimestampVal::null();
}
DateVal MaskFunctions::MaskHash(FunctionContext* ctx, const DateVal& val) {
  return DateVal::null();
}

/// Nullify overloads for other types
BooleanVal MaskFunctions::MaskNull(FunctionContext* ctx, const BooleanVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return BooleanVal::null();
}
DoubleVal MaskFunctions::MaskNull(FunctionContext* ctx, const DoubleVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return DoubleVal::null();
}
TimestampVal MaskFunctions::MaskNull(FunctionContext* ctx, const TimestampVal& val,
    const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
    const StringVal& digit_char, const IntVal& other_char,
    const StringVal& number_char) {
  return TimestampVal::null();
}
