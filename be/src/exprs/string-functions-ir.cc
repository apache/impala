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

#include "exprs/string-functions.h"

#include <cctype>
#include <numeric>
#include <stdint.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <boost/static_assert.hpp>

#include "exprs/anyval-util.h"
#include "exprs/scalar-expr.h"
#include "gutil/strings/charset.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"
#include "util/coding-util.h"
#include "util/ubsan.h"
#include "util/url-parser.h"

#include "common/names.h"

using namespace impala_udf;
using std::bitset;

// NOTE: be careful not to use string::append.  It is not performant.
namespace impala {

// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
StringVal StringFunctions::Substring(FunctionContext* context,
    const StringVal& str, const BigIntVal& pos, const BigIntVal& len) {
  if (str.is_null || pos.is_null || len.is_null) return StringVal::null();
  int fixed_pos = pos.val;
  if (fixed_pos < 0) fixed_pos = str.len + fixed_pos + 1;
  int max_len = str.len - fixed_pos + 1;
  int fixed_len = ::min(static_cast<int>(len.val), max_len);
  if (fixed_pos > 0 && fixed_pos <= str.len && fixed_len > 0) {
    return StringVal(str.ptr + fixed_pos - 1, fixed_len);
  } else {
    return StringVal();
  }
}

StringVal StringFunctions::Substring(FunctionContext* context,
    const StringVal& str, const BigIntVal& pos) {
  // StringVal.len is an int => INT32_MAX
  return Substring(context, str, pos, BigIntVal(INT32_MAX));
}

// This behaves identically to the mysql implementation.
StringVal StringFunctions::Left(
    FunctionContext* context, const StringVal& str, const BigIntVal& len) {
  return Substring(context, str, 1, len);
}

// This behaves identically to the mysql implementation.
StringVal StringFunctions::Right(
    FunctionContext* context, const StringVal& str, const BigIntVal& len) {
  // Don't index past the beginning of str, otherwise we'll get an empty string back
  int64_t pos = ::max(-len.val, static_cast<int64_t>(-str.len));
  return Substring(context, str, BigIntVal(pos), len);
}

StringVal StringFunctions::Space(FunctionContext* context, const BigIntVal& len) {
  if (len.is_null) return StringVal::null();
  if (len.val <= 0) return StringVal();
  StringVal result(context, len.val);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memset(result.ptr, ' ', len.val);
  return result;
}

StringVal StringFunctions::Repeat(
    FunctionContext* context, const StringVal& str, const BigIntVal& n) {
  if (str.is_null || n.is_null) return StringVal::null();
  if (str.len == 0 || n.val <= 0) return StringVal();
  if (n.val > StringVal::MAX_LENGTH) {
    context->SetError("Number of repeats in repeat() call is larger than allowed limit "
        "of 1 GB character data.");
    return StringVal::null();
  }
  static_assert(numeric_limits<int64_t>::max() / numeric_limits<int>::max()
      >= StringVal::MAX_LENGTH,
      "multiplying StringVal::len with positive int fits in int64_t");
  int64_t out_len = str.len * n.val;
  if (out_len > StringVal::MAX_LENGTH) {
    context->SetError(
        "repeat() result is larger than allowed limit of 1 GB character data.");
    return StringVal::null();
  }
  StringVal result(context, static_cast<int>(out_len));
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  for (int64_t i = 0; i < n.val; ++i) {
    memcpy(ptr, str.ptr, str.len);
    ptr += str.len;
  }
  return result;
}

StringVal StringFunctions::Lpad(FunctionContext* context, const StringVal& str,
    const BigIntVal& len, const StringVal& pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) return StringVal::null();
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad.len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (len.val <= str.len || pad.len == 0) return StringVal(str.ptr, len.val);

  StringVal result(context, len.val);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  int padded_prefix_len = len.val - str.len;
  int pad_index = 0;
  int result_index = 0;
  uint8_t* ptr = result.ptr;

  // Prepend chars of pad.
  while (result_index < padded_prefix_len) {
    ptr[result_index++] = pad.ptr[pad_index++];
    pad_index = pad_index % pad.len;
  }

  // Append given string.
  memcpy(ptr + result_index, str.ptr, str.len);
  return result;
}

StringVal StringFunctions::Rpad(FunctionContext* context, const StringVal& str,
    const BigIntVal& len, const StringVal& pad) {
  if (str.is_null || len.is_null || pad.is_null || len.val < 0) return StringVal::null();
  // Corner cases: Shrink the original string, or leave it alone.
  // TODO: Hive seems to go into an infinite loop if pad->len == 0,
  // so we should pay attention to Hive's future solution to be compatible.
  if (len.val <= str.len || pad.len == 0) {
    return StringVal(str.ptr, len.val);
  }

  StringVal result(context, len.val);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memcpy(result.ptr, str.ptr, str.len);

  // Append chars of pad until desired length
  uint8_t* ptr = result.ptr;
  int pad_index = 0;
  int result_len = str.len;
  while (result_len < len.val) {
    ptr[result_len++] = pad.ptr[pad_index++];
    pad_index = pad_index % pad.len;
  }
  return result;
}

IntVal StringFunctions::Length(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  return IntVal(str.len);
}

IntVal StringFunctions::CharLength(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  const FunctionContext::TypeDesc* t = context->GetArgType(0);
  DCHECK_EQ(t->type, FunctionContext::TYPE_FIXED_BUFFER);
  return StringValue::UnpaddedCharLength(reinterpret_cast<char*>(str.ptr), t->len);
}

StringVal StringFunctions::Lower(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::tolower(str.ptr[i]);
  }
  return result;
}

StringVal StringFunctions::Upper(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::toupper(str.ptr[i]);
  }
  return result;
}

// Returns a string identical to the input, but with the first character
// of each word mapped to its upper-case equivalent. All other characters
// will be mapped to their lower-case equivalents. If input == NULL it
// will return NULL
StringVal StringFunctions::InitCap(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* result_ptr = result.ptr;
  bool word_start = true;
  for (int i = 0; i < str.len; ++i) {
    if (isspace(str.ptr[i])) {
      result_ptr[i] = str.ptr[i];
      word_start = true;
    } else {
      result_ptr[i] = (word_start ? toupper(str.ptr[i]) : tolower(str.ptr[i]));
      word_start = false;
    }
  }
  return result;
}

struct ReplaceContext {
  ReplaceContext(StringVal *pattern_in) {
    pattern = StringValue::FromStringVal(*pattern_in);
    search = StringSearch(&pattern);
  }
  StringValue pattern;
  StringSearch search;
};

void StringFunctions::ReplacePrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (pattern->is_null || pattern->len == 0) return;

  struct ReplaceContext* replace = context->Allocate<ReplaceContext>();
  if (replace != nullptr) {
    new(replace) ReplaceContext(pattern);
    context->SetFunctionState(scope, replace);
  }
}

void StringFunctions::ReplaceClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  ReplaceContext* rptr = reinterpret_cast<ReplaceContext*>
      (context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  context->Free(reinterpret_cast<uint8_t*>(rptr));
  context->SetFunctionState(scope, nullptr);
}

StringVal StringFunctions::Replace(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const StringVal& replace) {
  DCHECK_LE(str.len, StringVal::MAX_LENGTH);
  DCHECK_LE(pattern.len, StringVal::MAX_LENGTH);
  DCHECK_LE(replace.len, StringVal::MAX_LENGTH);
  if (str.is_null || pattern.is_null || replace.is_null) return StringVal::null();
  if (pattern.len == 0 || pattern.len > str.len) return str;

  // StringSearch keeps a pointer to the StringValue object, so it must remain
  // in scope if used.
  StringSearch search;
  StringValue needle;
  const StringSearch *search_ptr;
  const ReplaceContext* rptr = reinterpret_cast<ReplaceContext*>
      (context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  if (UNLIKELY(rptr == nullptr)) {
    needle = StringValue::FromStringVal(pattern);
    search = StringSearch(&needle);
    search_ptr = &search;
  } else {
    search_ptr = &rptr->search;
  }

  const StringValue haystack = StringValue::FromStringVal(str);
  int64_t match_pos = search_ptr->Search(&haystack);

  // No match?  Skip everything.
  if (match_pos < 0) return str;

  DCHECK_GT(pattern.len, 0);
  DCHECK_GE(haystack.len, pattern.len);
  int buffer_space;
  const int delta = replace.len - pattern.len;
  // MAX_LENGTH is unsigned, so convert back to int to do correctly signed compare
  DCHECK_LE(delta, static_cast<int>(StringVal::MAX_LENGTH) - 1);
  if ((delta > 0 && delta < 128) && haystack.len <= 128) {
    // Quick estimate for potential matches - this heuristic is needed to win
    // over regexp_replace on expanding patterns.  128 is arbitrarily chosen so
    // we can't massively over-estimate the buffer size.
    int matches_possible = 0;
    char c = pattern.ptr[0];
    for (int i = 0; i <= haystack.len - pattern.len; ++i) {
      if (haystack.ptr[i] == c) ++matches_possible;
    }
    buffer_space = haystack.len + matches_possible * delta;
  } else {
    // Note - cannot overflow because pattern.len is at least one
    static_assert(StringVal::MAX_LENGTH - 1 + StringVal::MAX_LENGTH <=
        std::numeric_limits<decltype(buffer_space)>::max(),
        "Buffer space computation can overflow");
    buffer_space = haystack.len + delta;
  }

  StringVal result(context, buffer_space);
  // result may be NULL if we went over MAX_LENGTH or the allocation failed.
  if (UNLIKELY(result.is_null)) return result;

  uint8_t* ptr = result.ptr;
  int consumed = 0;
  while (match_pos + pattern.len <= haystack.len) {
    // Copy in original string
    const int unmatched_bytes = match_pos - consumed;
    memcpy(ptr, &haystack.ptr[consumed], unmatched_bytes);
    DCHECK_LE(ptr - result.ptr + unmatched_bytes, buffer_space);
    ptr += unmatched_bytes;

    // Copy in replacement - always safe since we always leave room for one more replace
    DCHECK_LE(ptr - result.ptr + replace.len, buffer_space);
    Ubsan::MemCpy(ptr, replace.ptr, replace.len);
    ptr += replace.len;

    // Don't want to re-match within already replaced pattern
    match_pos += pattern.len;
    consumed = match_pos;

    StringValue haystack_substring = haystack.Substring(match_pos);
    int match_pos_in_substring = search_ptr->Search(&haystack_substring);
    if (match_pos_in_substring < 0) break;

    match_pos += match_pos_in_substring;

    // If we had an enlarging pattern, we may need more space
    if (delta > 0) {
      const int bytes_produced = ptr - result.ptr;
      const int bytes_remaining = haystack.len - consumed;
      DCHECK_LE(bytes_produced, StringVal::MAX_LENGTH);
      DCHECK_LE(bytes_remaining, StringVal::MAX_LENGTH - 1);
      // Note: by above, cannot overflow
      const int min_output = bytes_produced + bytes_remaining;
      DCHECK_LE(min_output, StringVal::MAX_LENGTH);
      // Also no overflow: min_output <= MAX_LENGTH and delta <= MAX_LENGTH - 1
      const int64_t space_needed = min_output + delta;
      if (UNLIKELY(space_needed > buffer_space)) {
        // Check to see if we can allocate a large enough buffer.
        if (space_needed > StringVal::MAX_LENGTH) {
          context->SetError(
              "String length larger than allowed limit of 1 GB character data.");
          return StringVal::null();
        }
        // Double the buffer size whenever it fills up to amortise cost of resizing.
        // Must compute next power of two using 64-bit math to avoid signed overflow.
        buffer_space = min<int>(StringVal::MAX_LENGTH,
            static_cast<int>(BitUtil::RoundUpToPowerOfTwo(space_needed)));

        // Give up if the allocation fails or we hit an error. This prevents us from
        // continuing to blow past the mem limit.
        if (UNLIKELY(!result.Resize(context, buffer_space) || context->has_error())) {
          return StringVal::null();
        }
        // Don't forget to move the pointer
        ptr = result.ptr + bytes_produced;
      }
    }
  }

  // Copy in remainder and re-adjust size
  const int bytes_remaining = haystack.len - consumed;
  result.len = ptr - result.ptr + bytes_remaining;
  DCHECK_LE(result.len, buffer_space);
  memcpy(ptr, &haystack.ptr[consumed], bytes_remaining);

  return result;
}

StringVal StringFunctions::Reverse(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  BitUtil::ByteSwap(result.ptr, str.ptr, str.len);
  return result;
}

StringVal StringFunctions::Translate(FunctionContext* context, const StringVal& str,
    const StringVal& src, const StringVal& dst) {
  if (str.is_null || src.is_null || dst.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return result;

  // TODO: if we know src and dst are constant, we can prebuild a conversion
  // table to remove the inner loop.
  int result_len = 0;
  for (int i = 0; i < str.len; ++i) {
    bool matched_src = false;
    for (int j = 0; j < src.len; ++j) {
      if (str.ptr[i] == src.ptr[j]) {
        if (j < dst.len) {
          result.ptr[result_len++] = dst.ptr[j];
        } else {
          // src[j] doesn't map to any char in dst, the char is dropped.
        }
        matched_src = true;
        break;
      }
    }
    if (!matched_src) result.ptr[result_len++] = str.ptr[i];
  }
  result.len = result_len;
  return result;
}

void StringFunctions::TrimPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  // Create a bitset to hold the unique characters to trim.
  bitset<256>* unique_chars = new bitset<256>();
  context->SetFunctionState(scope, unique_chars);
  // If the caller didn't specify the set of characters to trim, it means
  // that we're only trimming whitespace. Return early in that case.
  // There can be either 1 or 2 arguments.
  DCHECK(context->GetNumArgs() == 1 || context->GetNumArgs() == 2);
  if (context->GetNumArgs() == 1) {
    unique_chars->set(static_cast<int>(' '), true);
    return;
  }
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* chars_to_trim = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (chars_to_trim->is_null) return; // We shouldn't peek into Null StringVals
  for (int32_t i = 0; i < chars_to_trim->len; ++i) {
    unique_chars->set(static_cast<int>(chars_to_trim->ptr[i]), true);
  }
}

void StringFunctions::TrimClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  bitset<256>* unique_chars = reinterpret_cast<bitset<256>*>(
      context->GetFunctionState(scope));
  delete unique_chars;
  context->SetFunctionState(scope, nullptr);
}

template <StringFunctions::TrimPosition D, bool IS_IMPLICIT_WHITESPACE>
StringVal StringFunctions::DoTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  if (str.is_null) return StringVal::null();
  bitset<256>* unique_chars = reinterpret_cast<bitset<256>*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  // When 'chars_to_trim' is unique for each element (e.g. when 'chars_to_trim'
  // is each element of a table column), we need to prepare a bitset of unique
  // characters here instead of using the bitset from function context.
  if (!IS_IMPLICIT_WHITESPACE && !ctx->IsArgConstant(1)) {
    if (chars_to_trim.is_null) return str;
    unique_chars->reset();
    for (int32_t i = 0; i < chars_to_trim.len; ++i) {
      unique_chars->set(static_cast<int>(chars_to_trim.ptr[i]), true);
    }
  }
  // Find new starting position.
  int32_t begin = 0;
  int32_t end = str.len - 1;
  if (D == LEADING || D == BOTH) {
    while (begin < str.len &&
        unique_chars->test(static_cast<int>(str.ptr[begin]))) {
      ++begin;
    }
  }
  // Find new ending position.
  if (D == TRAILING || D == BOTH) {
    while (end >= begin && unique_chars->test(static_cast<int>(str.ptr[end]))) {
      --end;
    }
  }
  return StringVal(str.ptr + begin, end - begin + 1);
}

StringVal StringFunctions::Trim(FunctionContext* context, const StringVal& str) {
  return DoTrimString<BOTH, true>(context, str, StringVal(" "));
}

StringVal StringFunctions::Ltrim(FunctionContext* context, const StringVal& str) {
  return DoTrimString<LEADING, true>(context, str, StringVal(" "));
}

StringVal StringFunctions::Rtrim(FunctionContext* context, const StringVal& str) {
  return DoTrimString<TRAILING, true>(context, str, StringVal(" "));
}

StringVal StringFunctions::LTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  return DoTrimString<LEADING, false>(ctx, str, chars_to_trim);
}

StringVal StringFunctions::RTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  return DoTrimString<TRAILING, false>(ctx, str, chars_to_trim);
}

StringVal StringFunctions::BTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  return DoTrimString<BOTH, false>(ctx, str, chars_to_trim);
}

IntVal StringFunctions::Ascii(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  // Hive returns 0 when given an empty string.
  return IntVal((str.len == 0) ? 0 : static_cast<int32_t>(str.ptr[0]));
}

IntVal StringFunctions::Instr(FunctionContext* context, const StringVal& str,
    const StringVal& substr, const BigIntVal& start_position,
    const BigIntVal& occurrence) {
  if (str.is_null || substr.is_null || start_position.is_null || occurrence.is_null) {
    return IntVal::null();
  }
  if (occurrence.val <= 0) {
    stringstream ss;
    ss << "Invalid occurrence parameter to instr function: " << occurrence.val;
    context->SetError(ss.str().c_str());
    return IntVal(0);
  }
  if (start_position.val == 0) return IntVal(0);

  StringValue haystack = StringValue::FromStringVal(str);
  StringValue needle = StringValue::FromStringVal(substr);
  StringSearch search(&needle);
  if (start_position.val > 0) {
    // A positive starting position indicates regular searching from the left.
    int search_start_pos = start_position.val - 1;
    if (search_start_pos >= haystack.len) return IntVal(0);
    int match_pos = -1;
    for (int match_num = 0; match_num < occurrence.val; ++match_num) {
      DCHECK_LE(search_start_pos, haystack.len);
      StringValue haystack_substring = haystack.Substring(search_start_pos);
      int match_pos_in_substring = search.Search(&haystack_substring);
      if (match_pos_in_substring < 0) return IntVal(0);
      match_pos = search_start_pos + match_pos_in_substring;
      search_start_pos = match_pos + 1;
    }
    // Return positions starting from 1 at the leftmost position.
    return IntVal(match_pos + 1);
  } else {
    // A negative starting position indicates searching from the right.
    int search_start_pos = haystack.len + start_position.val;
    // The needle must fit between search_start_pos and the end of the string
    if (search_start_pos + needle.len > haystack.len) {
      search_start_pos = haystack.len - needle.len;
    }
    if (search_start_pos < 0) return IntVal(0);
    int match_pos = -1;
    for (int match_num = 0; match_num < occurrence.val; ++match_num) {
      DCHECK_GE(search_start_pos + needle.len, 0);
      DCHECK_LE(search_start_pos + needle.len, haystack.len);
      StringValue haystack_substring =
          haystack.Substring(0, search_start_pos + needle.len);
      match_pos = search.RSearch(&haystack_substring);
      if (match_pos < 0) return IntVal(0);
      search_start_pos = match_pos - 1;
    }
    // Return positions starting from 1 at the leftmost position.
    return IntVal(match_pos + 1);
  }
}

IntVal StringFunctions::Instr(FunctionContext* context, const StringVal& str,
    const StringVal& substr, const BigIntVal& start_position) {
  return Instr(context, str, substr, start_position, BigIntVal(1));
}

IntVal StringFunctions::Instr(
    FunctionContext* context, const StringVal& str, const StringVal& substr) {
  return Instr(context, str, substr, BigIntVal(1), BigIntVal(1));
}

IntVal StringFunctions::Locate(FunctionContext* context, const StringVal& substr,
    const StringVal& str) {
  return Instr(context, str, substr);
}

IntVal StringFunctions::LocatePos(FunctionContext* context, const StringVal& substr,
    const StringVal& str, const BigIntVal& start_pos) {
  if (str.is_null || substr.is_null || start_pos.is_null) return IntVal::null();
  // Hive returns 0 for *start_pos <= 0,
  // but throws an exception for *start_pos > str->len.
  // Since returning 0 seems to be Hive's error condition, return 0.
  if (start_pos.val <= 0 || start_pos.val > str.len) return IntVal(0);
  StringValue substr_sv = StringValue::FromStringVal(substr);
  StringSearch search(&substr_sv);
  // Input start_pos.val starts from 1.
  StringValue adjusted_str(reinterpret_cast<char*>(str.ptr) + start_pos.val - 1,
       str.len - start_pos.val + 1);
  int32_t match_pos = search.Search(&adjusted_str);
  if (match_pos >= 0) {
    // Hive returns the position in the original string starting from 1.
    return IntVal(start_pos.val + match_pos);
  } else {
    return IntVal(0);
  }
}

// The caller owns the returned regex. Returns NULL if the pattern could not be compiled.
re2::RE2* CompileRegex(const StringVal& pattern, string* error_str,
    const StringVal& match_parameter) {
  DCHECK(error_str != NULL);
  re2::StringPiece pattern_sp(reinterpret_cast<char*>(pattern.ptr), pattern.len);
  re2::RE2::Options options;
  // Disable error logging in case e.g. every row causes an error
  options.set_log_errors(false);
  // Return the leftmost longest match (rather than the first match).
  options.set_longest_match(true);
  if (!match_parameter.is_null &&
      !StringFunctions::SetRE2Options(match_parameter, error_str, &options)) {
    return NULL;
  }
  re2::RE2* re = new re2::RE2(pattern_sp, options);
  if (!re->ok()) {
    stringstream ss;
    ss << "Could not compile regexp pattern: " << AnyValUtil::ToString(pattern) << endl
       << "Error: " << re->error();
    *error_str = ss.str();
    delete re;
    return NULL;
  }
  return re;
}

// This function sets options in the RE2 library before pattern matching.
bool StringFunctions::SetRE2Options(const StringVal& match_parameter,
    string* error_str, re2::RE2::Options* opts) {
  for (int i = 0; i < match_parameter.len; i++) {
    char match = match_parameter.ptr[i];
    switch (match) {
      case 'i':
        opts->set_case_sensitive(false);
        break;
      case 'c':
        opts->set_case_sensitive(true);
        break;
      case 'm':
        opts->set_posix_syntax(true);
        opts->set_one_line(false);
        break;
      case 'n':
        opts->set_never_nl(false);
        opts->set_dot_nl(true);
        break;
      default:
        stringstream error;
        error << "Illegal match parameter " << match;
        *error_str = error.str();
        return false;
    }
  }
  return true;
}

void StringFunctions::RegexpPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (pattern->is_null) return;

  string error_str;
  re2::RE2* re = CompileRegex(*pattern, &error_str, StringVal::null());
  if (re == NULL) {
    context->SetError(error_str.c_str());
    return;
  }
  context->SetFunctionState(scope, re);
}

void StringFunctions::RegexpClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  re2::RE2* re = reinterpret_cast<re2::RE2*>(context->GetFunctionState(scope));
  delete re;
  context->SetFunctionState(scope, nullptr);
}

StringVal StringFunctions::RegexpEscape(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (str.len == 0) return str;

  static const strings::CharSet REGEX_ESCAPE_CHARACTERS(".\\+*?[^]$(){}=!<>|:-");
  const uint8_t* const start_ptr = str.ptr;
  const uint8_t* const end_ptr = start_ptr + str.len;
  StringVal result(context, str.len * 2);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* dest_ptr = result.ptr;
  for (const uint8_t* c = start_ptr; c < end_ptr; ++c) {
    if (REGEX_ESCAPE_CHARACTERS.Test(*c)) {
      *dest_ptr++ = '\\';
    }
    *dest_ptr++ = *c;
  }
  result.len = dest_ptr - result.ptr;
  DCHECK_GE(result.len, str.len);

  return result;
}

StringVal StringFunctions::RegexpExtract(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const BigIntVal& index) {
  if (str.is_null || pattern.is_null || index.is_null) return StringVal::null();
  if (index.val < 0) return StringVal();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if we have to locally compile it
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str, StringVal::null());
    if (re == NULL) {
      context->AddWarning(error_str.c_str());
      return StringVal::null();
    }
    scoped_re.reset(re);
  }

  re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
  int max_matches = 1 + re->NumberOfCapturingGroups();
  if (index.val >= max_matches) return StringVal();
  // Use a vector because clang complains about non-POD varlen arrays
  // TODO: fix this
  vector<re2::StringPiece> matches(max_matches);
  bool success =
      re->Match(str_sp, 0, str.len, re2::RE2::UNANCHORED, matches.data(), max_matches);
  if (!success) return StringVal();
  // matches[0] is the whole string, matches[1] the first group, etc.
  const re2::StringPiece& match = matches[index.val];
  return AnyValUtil::FromBuffer(context, match.data(), match.size());
}

StringVal StringFunctions::RegexpReplace(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const StringVal& replace) {
  if (str.is_null || pattern.is_null || replace.is_null) return StringVal::null();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if state->re is NULL
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str, StringVal::null());
    if (re == NULL) {
      context->AddWarning(error_str.c_str());
      return StringVal::null();
    }
    scoped_re.reset(re);
  }

  re2::StringPiece replace_str =
      re2::StringPiece(reinterpret_cast<char*>(replace.ptr), replace.len);
  string result_str = AnyValUtil::ToString(str);
  re2::RE2::GlobalReplace(&result_str, *re, replace_str);
  return AnyValUtil::FromString(context, result_str);
}

void StringFunctions::RegexpMatchCountPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  int num_args = context->GetNumArgs();
  DCHECK(num_args == 2 || num_args == 4);
  if (!context->IsArgConstant(1) || (num_args == 4 && !context->IsArgConstant(3))) return;

  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (pattern->is_null) return;

  StringVal* match_parameter = NULL;
  if (num_args == 4) {
    DCHECK_EQ(context->GetArgType(3)->type, FunctionContext::TYPE_STRING);
    match_parameter = reinterpret_cast<StringVal*>(context->GetConstantArg(3));
  }
  string error_str;
  re2::RE2* re = CompileRegex(*pattern, &error_str, match_parameter == NULL ?
      StringVal::null() : *match_parameter);
  if (re == NULL) {
    context->SetError(error_str.c_str());
    return;
  }
  context->SetFunctionState(scope, re);
}

IntVal StringFunctions::RegexpMatchCount2Args(FunctionContext* context,
    const StringVal& str, const StringVal& pattern) {
  return RegexpMatchCount4Args(context, str, pattern, IntVal::null(), StringVal::null());
}

IntVal StringFunctions::RegexpMatchCount4Args(FunctionContext* context,
    const StringVal& str, const StringVal& pattern, const IntVal& start_pos,
    const StringVal& match_parameter) {
  if (str.is_null || pattern.is_null) return IntVal::null();

  int offset = 0;
  DCHECK_GE(str.len, 0);
  // The parameter "start_pos" starts counting at 1 instead of 0. If "start_pos" is
  // beyond the end of the string, "str" will be considered an empty string.
  if (!start_pos.is_null) offset = min(start_pos.val - 1, str.len);
  if (offset < 0) {
    stringstream error;
    error << "Illegal starting position " << start_pos.val << endl;
    context->SetError(error.str().c_str());
    return IntVal::null();
  }

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  // Destroys re if we have to locally compile it.
  scoped_ptr<re2::RE2> scoped_re;
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1) || (context->GetNumArgs() == 4 &&
        !context->IsArgConstant(3)));
    string error_str;
    re = CompileRegex(pattern, &error_str, match_parameter);
    if (re == NULL) {
      context->SetError(error_str.c_str());
      return IntVal::null();
    }
    scoped_re.reset(re);
  }

  DCHECK_GE(str.len, offset);
  re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
  int count = 0;
  re2::StringPiece match;
  while (offset <= str.len &&
      re->Match(str_sp, offset, str.len, re2::RE2::UNANCHORED, &match, 1)) {
    // Empty string is a valid match for pattern with '*'. Start matching at the next
    // character until we reach the end of the string.
    count++;
    if (match.size() == 0) {
      if (offset == str.len) {
        break;
      }
      offset++;
    } else {
      // Make sure forward progress is being made or we will be in an infinite loop.
      DCHECK_GT(match.data() - str_sp.data() + match.size(), offset);
      offset = match.data() - str_sp.data() + match.size();
    }
  }
  return IntVal(count);
}

StringVal StringFunctions::Concat(FunctionContext* context, int num_children,
    const StringVal* strs) {
  return ConcatWs(context, StringVal(), num_children, strs);
}

StringVal StringFunctions::ConcatWs(FunctionContext* context, const StringVal& sep,
    int num_children, const StringVal* strs) {
  DCHECK_GE(num_children, 1);
  DCHECK(strs != NULL);
  if (sep.is_null) return StringVal::null();

  // Pass through if there's only one argument
  if (num_children == 1) return strs[0];

  if (strs[0].is_null) return StringVal::null();
  int32_t total_size = strs[0].len;

  // Loop once to compute the final size and reserve space.
  for (int32_t i = 1; i < num_children; ++i) {
    if (strs[i].is_null) return StringVal::null();
    total_size += sep.len + strs[i].len;
  }
  StringVal result(context, total_size);
  if (UNLIKELY(result.is_null)) return StringVal::null();

  // Loop again to append the data.
  uint8_t* ptr = result.ptr;
  Ubsan::MemCpy(ptr, strs[0].ptr, strs[0].len);
  ptr += strs[0].len;
  for (int32_t i = 1; i < num_children; ++i) {
    Ubsan::MemCpy(ptr, sep.ptr, sep.len);
    ptr += sep.len;
    Ubsan::MemCpy(ptr, strs[i].ptr, strs[i].len);
    ptr += strs[i].len;
  }
  return result;
}

IntVal StringFunctions::FindInSet(FunctionContext* context, const StringVal& str,
    const StringVal& str_set) {
  if (str.is_null || str_set.is_null) return IntVal::null();
  // Check str for commas.
  for (int i = 0; i < str.len; ++i) {
    if (str.ptr[i] == ',') return IntVal(0);
  }
  // The result index starts from 1 since 0 is an error condition.
  int32_t token_index = 1;
  int32_t start = 0;
  int32_t end;
  StringValue str_sv = StringValue::FromStringVal(str);
  do {
    end = start;
    // Position end.
    while (end < str_set.len && str_set.ptr[end] != ',') ++end;
    StringValue token(reinterpret_cast<char*>(str_set.ptr) + start, end - start);
    if (str_sv.Eq(token)) return IntVal(token_index);

    // Re-position start and end past ','
    start = end + 1;
    ++token_index;
  } while (start < str_set.len);
  return IntVal(0);
}

void StringFunctions::ParseUrlPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  if (!ctx->IsArgConstant(1)) return;
  DCHECK_EQ(ctx->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* part = reinterpret_cast<StringVal*>(ctx->GetConstantArg(1));
  if (part->is_null) return;
  auto url_part = make_unique<UrlParser::UrlPart>(
      UrlParser::GetUrlPart(StringValue::FromStringVal(*part)));
  if (*url_part == UrlParser::INVALID) {
    stringstream ss;
    ss << "Invalid URL part: " << AnyValUtil::ToString(*part) << endl
       << "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', "
       << "'USERINFO', and 'QUERY')";
    ctx->SetError(ss.str().c_str());
    return;
  }
  ctx->SetFunctionState(scope, url_part.release());
}

StringVal StringFunctions::ParseUrl(
    FunctionContext* ctx, const StringVal& url, const StringVal& part) {
  if (url.is_null || part.is_null) return StringVal::null();
  void* state = ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL);
  UrlParser::UrlPart url_part;
  if (state != NULL) {
    url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
  } else {
    DCHECK(!ctx->IsArgConstant(1));
    url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(part));
  }

  StringValue result;
  if (!UrlParser::ParseUrl(StringValue::FromStringVal(url), url_part, &result)) {
    // url is malformed, or url_part is invalid.
    if (url_part == UrlParser::INVALID) {
      stringstream ss;
      ss << "Invalid URL part: " << AnyValUtil::ToString(part);
      ctx->AddWarning(ss.str().c_str());
    } else {
      stringstream ss;
      ss << "Could not parse URL: " << AnyValUtil::ToString(url);
      ctx->AddWarning(ss.str().c_str());
    }
    return StringVal::null();
  }
  StringVal result_sv;
  result.ToStringVal(&result_sv);
  return result_sv;
}

void StringFunctions::ParseUrlClose(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  UrlParser::UrlPart* url_part =
      reinterpret_cast<UrlParser::UrlPart*>(ctx->GetFunctionState(scope));
  delete url_part;
  ctx->SetFunctionState(scope, nullptr);
}

StringVal StringFunctions::ParseUrlKey(FunctionContext* ctx, const StringVal& url,
    const StringVal& part, const StringVal& key) {
  if (url.is_null || part.is_null || key.is_null) return StringVal::null();
  void* state = ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL);
  UrlParser::UrlPart url_part;
  if (state != NULL) {
    url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
  } else {
    DCHECK(!ctx->IsArgConstant(1));
    url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(part));
  }

  StringValue result;
  if (!UrlParser::ParseUrlKey(StringValue::FromStringVal(url), url_part,
                              StringValue::FromStringVal(key), &result)) {
    // url is malformed, or url_part is invalid.
    if (url_part == UrlParser::INVALID) {
      stringstream ss;
      ss << "Invalid URL part: " << AnyValUtil::ToString(part);
      ctx->AddWarning(ss.str().c_str());
    } else {
      stringstream ss;
      ss << "Could not parse URL: " << AnyValUtil::ToString(url);
      ctx->AddWarning(ss.str().c_str());
    }
    return StringVal::null();
  }
  StringVal result_sv;
  result.ToStringVal(&result_sv);
  return result_sv;
}

StringVal StringFunctions::Chr(FunctionContext* ctx, const IntVal& val) {
  if (val.is_null) return StringVal::null();
  if (val.val < 0 || val.val > 255) return "";
  char c = static_cast<char>(val.val);
  return AnyValUtil::FromBuffer(ctx, &c, 1);
}

// Similar to strstr() except that the strings are not null-terminated
static char* LocateSubstring(char* haystack, int hay_len, const char* needle, int needle_len) {
  DCHECK_GT(needle_len, 0);
  DCHECK(needle != NULL);
  DCHECK(hay_len == 0 || haystack != NULL);
  for (int i = 0; i < hay_len - needle_len + 1; ++i) {
    char* possible_needle = haystack + i;
    if (strncmp(possible_needle, needle, needle_len) == 0) return possible_needle;
  }
  return NULL;
}

StringVal StringFunctions::SplitPart(FunctionContext* context,
    const StringVal& str, const StringVal& delim, const BigIntVal& field) {
  if (str.is_null || delim.is_null || field.is_null) return StringVal::null();
  int field_pos = field.val;
  if (field_pos <= 0) {
    stringstream ss;
    ss << "Invalid field position: " << field.val;
    context->SetError(ss.str().c_str());
    return StringVal::null();
  }
  if (delim.len == 0) return str;
  char* str_start = reinterpret_cast<char*>(str.ptr);
  char* str_part = str_start;
  char* delimiter = reinterpret_cast<char*>(delim.ptr);
  for (int cur_pos = 1; ; ++cur_pos) {
    int remaining_len = str.len - (str_part - str_start);
    char* delim_ref = LocateSubstring(str_part, remaining_len, delimiter, delim.len);
    if (delim_ref == NULL) {
      if (cur_pos == field_pos) {
        return StringVal(reinterpret_cast<uint8_t*>(str_part), remaining_len);
      }
      // Return empty string if required field position is not found.
      return StringVal();
    }
    if (cur_pos == field_pos) {
      return StringVal(reinterpret_cast<uint8_t*>(str_part),
          delim_ref - str_part);
    }
    str_part = delim_ref + delim.len;
  }
  return StringVal();
}

StringVal StringFunctions::Base64Encode(FunctionContext* ctx, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (str.len == 0) return StringVal(ctx, 0);
  int64_t out_max = 0;
  if (UNLIKELY(!Base64EncodeBufLen(str.len, &out_max))) {
    stringstream ss;
    ss << "Could not base64 encode a string of length " << str.len;
    ctx->AddWarning(ss.str().c_str());
    return StringVal::null();
  }
  StringVal result(ctx, out_max);
  if (UNLIKELY(result.is_null)) return result;
  int64_t out_len = 0;
  if (UNLIKELY(!impala::Base64Encode(
          reinterpret_cast<const char*>(str.ptr), str.len,
          out_max, reinterpret_cast<char*>(result.ptr), &out_len))) {
    stringstream ss;
    ss << "Could not base64 encode input in space " << out_max
       << "; actual output length " << out_len;
    ctx->AddWarning(ss.str().c_str());
    return StringVal::null();
  }
  result.len = out_len;
  return result;
}

StringVal StringFunctions::Base64Decode(FunctionContext* ctx, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (0 == str.len) return StringVal(ctx, 0);
  int64_t out_max = 0;
  if (UNLIKELY(!Base64DecodeBufLen(
          reinterpret_cast<const char*>(str.ptr), static_cast<int64_t>(str.len),
          &out_max))) {
    stringstream ss;
    ss << "Invalid base64 string; input length is " << str.len
       << ", which is not a multiple of 4.";
    ctx->AddWarning(ss.str().c_str());
    return StringVal::null();
  }
  StringVal result(ctx, out_max);
  if (UNLIKELY(result.is_null)) return result;
  int64_t out_len = 0;
  if (UNLIKELY(!impala::Base64Decode(
          reinterpret_cast<const char*>(str.ptr), static_cast<int64_t>(str.len),
          out_max, reinterpret_cast<char*>(result.ptr), &out_len))) {
    stringstream ss;
    ss << "Could not base64 decode input in space " << out_max
       << "; actual output length " << out_len;
    ctx->AddWarning(ss.str().c_str());
    return StringVal::null();
  }
  result.len = out_len;
  return result;
}

StringVal StringFunctions::GetJsonObject(FunctionContext *ctx, const StringVal &json_str,
    const StringVal &path_str) {
  return GetJsonObjectImpl(ctx, json_str, path_str);
}

IntVal StringFunctions::Levenshtein(
    FunctionContext* ctx, const StringVal& s1, const StringVal& s2) {
  // Adapted from https://bit.ly/2SbDgN4
  // under the Creative Commons Attribution-ShareAlike License

  int s1len = s1.len;
  int s2len = s2.len;

  // error if either input exceeds 255 characters
  if (s1len > 255 || s2len > 255) {
    ctx->SetError("levenshtein argument exceeds maximum length of 255 characters");
    return IntVal(-1);
  }

  // short cut cases:
  // - null strings
  // - zero length strings
  // - identical length and value strings
  if (s1.is_null || s2.is_null) return IntVal::null();
  if (s1len == 0) return IntVal(s2len);
  if (s2len == 0) return IntVal(s1len);
  if (s1len == s2len && memcmp(s1.ptr, s2.ptr, s1len) == 0) return IntVal(0);

  int column_start = 1;

  auto column = reinterpret_cast<int*>(ctx->Allocate(sizeof(int) * (s1len + 1)));

  std::iota(column + column_start - 1, column + s1len + 1, column_start - 1);

  for (int x = column_start; x <= s2len; x++) {
    column[0] = x;
    int last_diagonal = x - column_start;
    for (int y = column_start; y <= s1len; y++) {
      int old_diagonal = column[y];
      auto possibilities = {column[y] + 1, column[y - 1] + 1,
          last_diagonal + (s1.ptr[y - 1] == s2.ptr[x - 1] ? 0 : 1)};
      column[y] = std::min(possibilities);
      last_diagonal = old_diagonal;
    }
  }
  int result = column[s1len];
  ctx->Free(reinterpret_cast<uint8_t*>(column));

  return IntVal(result);
}
}
