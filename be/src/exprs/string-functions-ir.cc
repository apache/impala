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
#include "gen-cpp/Metrics_types.h"
#include "gutil/strings/charset.h"
#include "gutil/strings/substitute.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"
#include "util/coding-util.h"
#include "util/pretty-printer.h"
#include "util/string-util.h"
#include "util/ubsan.h"
#include "util/url-parser.h"

#include "common/names.h"

using namespace impala_udf;
using std::bitset;
using std::any_of;

// NOTE: be careful not to use string::append.  It is not performant.
namespace impala {

const char* ERROR_CHARACTER_LIMIT_EXCEEDED =
  "$0 is larger than allowed limit of $1 character data.";

uint64_t StringFunctions::re2_mem_limit_ = 8 << 20;

// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
StringVal StringFunctions::Substring(FunctionContext* context,
    const StringVal& str, const BigIntVal& pos, const BigIntVal& len) {
  if (str.is_null || pos.is_null || len.is_null) return StringVal::null();
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return Utf8Substring(context, str, pos, len);
  }
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

StringVal StringFunctions::Utf8Substring(FunctionContext* context, const StringVal& str,
    const BigIntVal& pos) {
  return Utf8Substring(context, str, pos, BigIntVal(INT32_MAX));
}

StringVal StringFunctions::Utf8Substring(FunctionContext* context, const StringVal& str,
    const BigIntVal& pos, const BigIntVal& len) {
  if (str.is_null || pos.is_null || len.is_null) return StringVal::null();
  if (str.len == 0 || pos.val == 0 || len.val <= 0) return StringVal();

  int byte_pos;
  int utf8_cnt = 0;
  // pos.val starts at 1 (1-indexed positions).
  if (pos.val > 0) {
    // Seek to the start byte of the pos-th UTF-8 character.
    for (byte_pos = 0; utf8_cnt < pos.val && byte_pos < str.len; ++byte_pos) {
      if (BitUtil::IsUtf8StartByte(str.ptr[byte_pos])) ++utf8_cnt;
    }
    // Not enough UTF-8 characters.
    if (utf8_cnt < pos.val) return StringVal();
    // Back to the start byte of the pos-th UTF-8 character.
    --byte_pos;
    int byte_start = byte_pos;
    // Seek to the end until we get enough UTF-8 characters.
    for (utf8_cnt = 0; utf8_cnt < len.val && byte_pos < str.len; ++byte_pos) {
      if (BitUtil::IsUtf8StartByte(str.ptr[byte_pos])) ++utf8_cnt;
    }
    if (utf8_cnt == len.val) {
      // We are now at the middle byte of the last UTF-8 character. Seek to the end of it.
      while (byte_pos < str.len && !BitUtil::IsUtf8StartByte(str.ptr[byte_pos])) {
        ++byte_pos;
      }
    }
    return StringVal(str.ptr + byte_start, byte_pos - byte_start);
  }
  // pos.val is negative. Seek from the end of the string.
  int byte_end = str.len;
  utf8_cnt = 0;
  byte_pos = str.len - 1;
  while (utf8_cnt < -pos.val && byte_pos >= 0) {
    if (BitUtil::IsUtf8StartByte(str.ptr[byte_pos])) {
      ++utf8_cnt;
      // Remember the end of the substring's last UTF-8 character.
      if (utf8_cnt > 0 && utf8_cnt == -pos.val - len.val) byte_end = byte_pos;
    }
    --byte_pos;
  }
  // Not enough UTF-8 characters.
  if (utf8_cnt < -pos.val) return StringVal();
  // Back to the start byte of the substring's first UTF-8 character.
  ++byte_pos;
  return StringVal(str.ptr + byte_pos, byte_end - byte_pos);
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
  if (len.val > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
         "space() result",
         PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
  }
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
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
        "Number of repeats in repeat() call",
        PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
  }
  static_assert(numeric_limits<int64_t>::max() / numeric_limits<int>::max()
      >= StringVal::MAX_LENGTH,
      "multiplying StringVal::len with positive int fits in int64_t");
  int64_t out_len = str.len * n.val;
  if (out_len > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
        "repeat() result",
        PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
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
  if (len.val > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
        "lpad() result",
        PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
  }
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
  if (len.val > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
        "rpad() result",
        PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
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
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE, 0)) {
    return Utf8Length(context, str);
  }
  return IntVal(str.len);
}
IntVal StringFunctions::Bytes(FunctionContext* context,const StringVal& str){
  if(str.is_null) return IntVal::null();
  return IntVal(str.len);
}

IntVal StringFunctions::CharLength(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  const FunctionContext::TypeDesc* t = context->GetArgType(0);
  DCHECK_EQ(t->type, FunctionContext::TYPE_FIXED_BUFFER);
  return StringValue::UnpaddedCharLength(reinterpret_cast<char*>(str.ptr), t->len);
}

static int CountUtf8Chars(uint8_t* ptr, int len) {
  if (ptr == nullptr) return 0;
  int cnt = 0;
  for (int i = 0; i < len; ++i) {
    if (BitUtil::IsUtf8StartByte(ptr[i])) ++cnt;
  }
  return cnt;
}

IntVal StringFunctions::Utf8Length(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  return IntVal(CountUtf8Chars(str.ptr, str.len));
}

StringVal StringFunctions::Lower(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return LowerUtf8(context, str);
  }
  return LowerAscii(context, str);
}

StringVal StringFunctions::LowerAscii(FunctionContext* context, const StringVal& str) {
  // Not in UTF-8 mode, only English alphabetic characters will be converted.
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::tolower(str.ptr[i]);
  }
  return result;
}

StringVal StringFunctions::Upper(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return UpperUtf8(context, str);
  }
  return UpperAscii(context, str);
}

StringVal StringFunctions::UpperAscii(FunctionContext* context, const StringVal& str) {
  // Not in UTF-8 mode, only English alphabetic characters will be converted.
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
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return InitCapUtf8(context, str);
  }
  return InitCapAscii(context, str);
}

StringVal StringFunctions::InitCapAscii(FunctionContext* context, const StringVal& str) {
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

/// Reports the error in parsing multibyte characters with leading bytes and current
/// locale. Used in Utf8CaseConversion().
static void ReportErrorBytes(FunctionContext* context, const StringVal& str,
    int current_idx) {
  DCHECK_LT(current_idx, str.len);
  stringstream ss;
  ss << "[0x" << std::hex << (int)DCHECK_NOTNULL(str.ptr)[current_idx];
  for (int k = 1; k < 4 && current_idx + k < str.len; ++k) {
    ss << ", 0x" << std::hex << (int)str.ptr[current_idx + k];
  }
  ss << "]";
  context->AddWarning(Substitute(
      "Illegal multi-byte character in string. Leading bytes: $0. Current locale: $1",
      ss.str(), std::locale("").name()).c_str());
}

/// Converts string based on the transform function 'fn'. The unit of the conversion is
/// a wchar_t (i.e. uint32_t) which is parsed from multi bytes using std::mbtowc().
/// The transform function 'fn' accepts two parameters: the original wchar_t and a flag
/// indicating whether it's the first character of a word.
/// After the transformation, the wchar_t is converted back to bytes.
static StringVal Utf8CaseConversion(FunctionContext* context, const StringVal& str,
    uint32_t (*fn)(uint32_t, bool*)) {
  // Usually the upper/lower cases have the same size in bytes. Here we add 4 bytes
  // buffer in case of illegal Unicodes.
  int max_result_bytes = str.len + 4;
  StringVal result(context, max_result_bytes);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  wchar_t wc;
  int wc_bytes;
  bool word_start = true;
  uint8_t* result_ptr = result.ptr;
  std::mbstate_t wc_state{};
  std::mbstate_t mb_state{};
  for (int i = 0; i < str.len; i += wc_bytes) {
    // std::mbtowc converts a multibyte sequence to a wide character. It's not
    // thread safe. Here we use std::mbrtowc instead.
    wc_bytes = std::mbrtowc(&wc, reinterpret_cast<char*>(str.ptr + i), str.len - i,
        &wc_state);
    bool needs_conversion = true;
    if (wc_bytes == 0) {
      // std::mbtowc returns 0 when hitting '\0'.
      wc = 0;
      wc_bytes = 1;
    } else if (wc_bytes < 0) {
      ReportErrorBytes(context, str, i);
      // Replace it to the replacement character (U+FFFD)
      wc = 0xFFFD;
      needs_conversion = false;
      // Jump to the next legal UTF-8 start byte.
      wc_bytes = 1;
      while (i + wc_bytes < str.len && !BitUtil::IsUtf8StartByte(str.ptr[i + wc_bytes])) {
        wc_bytes++;
      }
    }
    if (needs_conversion) wc = fn(wc, &word_start);
    // std::wctomb converts a wide character to a multibyte sequence. It's not
    // thread safe. Here we use std::wcrtomb instead.
    int res_bytes = std::wcrtomb(reinterpret_cast<char*>(result_ptr), wc, &mb_state);
    if (res_bytes <= 0) {
      if (needs_conversion) {
        context->AddWarning(Substitute(
            "Ignored illegal wide character in results: $0. Current locale: $1",
            wc, std::locale("").name()).c_str());
      }
      continue;
    }
    result_ptr += res_bytes;
    if (result_ptr - result.ptr > max_result_bytes - 4) {
      // Double the result buffer for overflow
      max_result_bytes *= 2;
      max_result_bytes = min<int>(StringVal::MAX_LENGTH,
          static_cast<int>(BitUtil::RoundUpToPowerOfTwo(max_result_bytes)));
      int offset = result_ptr - result.ptr;
      if (UNLIKELY(!result.Resize(context, max_result_bytes))) return StringVal::null();
      result_ptr = result.ptr + offset;
    }
  }
  result.len = result_ptr - result.ptr;
  return result;
}

StringVal StringFunctions::LowerUtf8(FunctionContext* context, const StringVal& str) {
  return Utf8CaseConversion(context, str,
      [](uint32_t wide_char, bool* word_start) {
        return std::towlower(wide_char);
      });
}

StringVal StringFunctions::UpperUtf8(FunctionContext* context, const StringVal& str) {
  return Utf8CaseConversion(context, str,
      [](uint32_t wide_char, bool* word_start) {
        return std::towupper(wide_char);
      });
}

StringVal StringFunctions::InitCapUtf8(FunctionContext* context, const StringVal& str) {
  return Utf8CaseConversion(context, str,
      [](uint32_t wide_char, bool* word_start) {
        if (UNLIKELY(iswspace(wide_char))) {
          *word_start = true;
          return wide_char;
        }
        uint32_t res = *word_start ? std::towupper(wide_char) : std::towlower(wide_char);
        *word_start = false;
        return res;
      });
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

  StringValue::SimpleString haystack_s = haystack.ToSimpleString();

  DCHECK_GT(pattern.len, 0);
  DCHECK_GE(haystack_s.len, pattern.len);
  int buffer_space;
  const int delta = replace.len - pattern.len;
  // MAX_LENGTH is unsigned, so convert back to int to do correctly signed compare
  DCHECK_LE(delta, static_cast<int>(StringVal::MAX_LENGTH) - 1);
  if ((delta > 0 && delta < 128) && haystack_s.len <= 128) {
    // Quick estimate for potential matches - this heuristic is needed to win
    // over regexp_replace on expanding patterns.  128 is arbitrarily chosen so
    // we can't massively over-estimate the buffer size.
    int matches_possible = 0;
    char c = pattern.ptr[0];
    for (int i = 0; i <= haystack_s.len - pattern.len; ++i) {
      if (haystack_s.ptr[i] == c) ++matches_possible;
    }
    buffer_space = haystack_s.len + matches_possible * delta;
  } else {
    // Note - cannot overflow because pattern.len is at least one
    static_assert(StringVal::MAX_LENGTH - 1 + StringVal::MAX_LENGTH <=
        std::numeric_limits<decltype(buffer_space)>::max(),
        "Buffer space computation can overflow");
    buffer_space = haystack_s.len + delta;
  }

  StringVal result(context, buffer_space);
  // result may be NULL if we went over MAX_LENGTH or the allocation failed.
  if (UNLIKELY(result.is_null)) return result;

  uint8_t* ptr = result.ptr;
  int consumed = 0;
  while (match_pos + pattern.len <= haystack_s.len) {
    // Copy in original string
    const int unmatched_bytes = match_pos - consumed;
    memcpy(ptr, &haystack_s.ptr[consumed], unmatched_bytes);
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
      const int bytes_remaining = haystack_s.len - consumed;
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
          context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
              "replace() result",
              PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
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
  const int bytes_remaining = haystack_s.len - consumed;
  result.len = ptr - result.ptr + bytes_remaining;
  DCHECK_LE(result.len, buffer_space);
  memcpy(ptr, &haystack_s.ptr[consumed], bytes_remaining);

  return result;
}

StringVal StringFunctions::Reverse(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE)) {
    return Utf8Reverse(context, str);
  }
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  BitUtil::ByteSwap(result.ptr, str.ptr, str.len);
  return result;
}

static inline void InPlaceReverse(uint8_t* ptr, int len) {
  for (int i = 0, j = len - 1; i < j; ++i, --j) {
    uint8_t tmp = ptr[i];
    ptr[i] = ptr[j];
    ptr[j] = tmp;
  }
}

// Returns a string with the UTF-8 characters (code points) in revrese order. Note that
// this function operates on Unicode code points and not user visible characters (or
// grapheme clusters). This is consistent with other systems, e.g. Hive, SparkSQL.
StringVal StringFunctions::Utf8Reverse(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (str.len == 0) return StringVal();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  // First make a copy of the reversed string.
  BitUtil::ByteSwap(result.ptr, str.ptr, str.len);
  // Then reverse bytes inside each UTF-8 character.
  int last = result.len;
  for (int i = result.len - 1; i >= 0; --i) {
    if (BitUtil::IsUtf8StartByte(result.ptr[i])) {
      // Only reverse bytes of a UTF-8 character
      if (last - i > 1) InPlaceReverse(result.ptr + i + 1, last - i);
      last = i;
    }
  }
  if (last > 0) InPlaceReverse(result.ptr, last + 1);
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

void StringFunctions::TrimContext::Reset(const StringVal& chars_to_trim) {
  single_byte_chars_.reset();
  double_byte_chars_.clear();
  triple_byte_chars_.clear();
  quadruple_byte_chars_.clear();

  if (!utf8_mode_) {
    for (size_t i = 0; i < chars_to_trim.len; ++i) {
      single_byte_chars_.set(chars_to_trim.ptr[i], true);
    }
    return;
  }

  for (size_t i = 0, char_size = 0; i < chars_to_trim.len; i += char_size) {
    char_size = BitUtil::NumBytesInUtf8Encoding(chars_to_trim.ptr[i]);

    // If the remaining number of bytes does not match the number of bytes specified by
    // the UTF-8 character, we may have encountered an illegal UTF-8 character.
    // In order to prevent subsequent data access from going out of bounds, restrictions
    // are placed here to ensure that accessing pointers to multi-byte characters is
    // always safe.
    if (UNLIKELY(i + char_size > chars_to_trim.len)) {
      char_size = chars_to_trim.len - i;
    }

    switch (char_size) {
      case 1: single_byte_chars_.set(chars_to_trim.ptr[i], true); break;
      case 2: double_byte_chars_.push_back(&chars_to_trim.ptr[i]); break;
      case 3: triple_byte_chars_.push_back(&chars_to_trim.ptr[i]); break;
      case 4: quadruple_byte_chars_.push_back(&chars_to_trim.ptr[i]); break;
      default: DCHECK(false); break;
    }
  }
}

bool StringFunctions::TrimContext::Contains(const uint8_t* utf8_char, int len) const {
  auto eq = [&](const uint8_t* c){ return memcmp(c, utf8_char, len) == 0; };
  switch (len) {
    case 1: return single_byte_chars_.test(*utf8_char);
    case 2: return any_of(double_byte_chars_.begin(), double_byte_chars_.end(), eq);
    case 3: return any_of(triple_byte_chars_.begin(), triple_byte_chars_.end(), eq);
    case 4: return any_of(quadruple_byte_chars_.begin(), quadruple_byte_chars_.end(), eq);
    default: DCHECK(false); return false;
  }
}

void StringFunctions::TrimPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  bool utf8_mode = context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE);
  DoTrimPrepare(context, scope, utf8_mode);
}

void StringFunctions::Utf8TrimPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  DoTrimPrepare(context, scope, true /* utf8_mode */);
}

void StringFunctions::DoTrimPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope, bool utf8_mode) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  TrimContext* trim_ctx = new TrimContext(utf8_mode);
  context->SetFunctionState(scope, trim_ctx);

  // If the caller didn't specify the set of characters to trim, it means
  // that we're only trimming whitespace. Return early in that case.
  // There can be either 1 or 2 arguments.
  DCHECK(context->GetNumArgs() == 1 || context->GetNumArgs() == 2);
  if (context->GetNumArgs() == 1) {
    trim_ctx->Reset(StringVal(" "));
    return;
  }
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* chars_to_trim = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (chars_to_trim->is_null) return; // We shouldn't peek into Null StringVals
  trim_ctx->Reset(*chars_to_trim);
}

void StringFunctions::TrimClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  TrimContext* trim_ctx =
      reinterpret_cast<TrimContext*>(context->GetFunctionState(scope));
  delete trim_ctx;
  context->SetFunctionState(scope, nullptr);
}

template <StringFunctions::TrimPosition D, bool IS_IMPLICIT_WHITESPACE>
StringVal StringFunctions::DoTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  if (str.is_null) return StringVal::null();
  TrimContext* trim_ctx = reinterpret_cast<TrimContext*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));

  // When 'chars_to_trim' is not a constant, we need to reset TrimContext with new
  // 'chars_to_trim'.
  if (!IS_IMPLICIT_WHITESPACE && !ctx->IsArgConstant(1)) {
    if (chars_to_trim.is_null) return str;
    trim_ctx->Reset(chars_to_trim);
  }

  // When dealing with UTF-8 characters in UTF-8 mode, use DoUtf8TrimString().
  if (trim_ctx->utf8_mode()) {
    return DoUtf8TrimString<D>(str, *trim_ctx);
  }

  // Otherwise, we continue to maintain the old behavior.
  int32_t begin = 0;
  int32_t end = str.len - 1;
  // Find new starting position.
  if constexpr (D == LEADING || D == BOTH) {
    while (begin < str.len && trim_ctx->Contains(str.ptr[begin])) {
      ++begin;
    }
  }
  // Find new ending position.
  if constexpr (D == TRAILING || D == BOTH) {
    while (end >= begin && trim_ctx->Contains(str.ptr[end])) {
      --end;
    }
  }
  return StringVal(str.ptr + begin, end - begin + 1);
}

template <StringFunctions::TrimPosition D>
StringVal StringFunctions::DoUtf8TrimString(const StringVal& str,
    const TrimContext& trim_ctx) {
  if (UNLIKELY(str.len == 0)) return str;

  const uint8_t* begin = str.ptr;
  const uint8_t* end = begin + str.len;
  // Find new starting position.
  if constexpr (D == LEADING || D == BOTH) {
    while (begin < end) {
      size_t char_size = BitUtil::NumBytesInUtf8Encoding(*begin);
      if (UNLIKELY(begin + char_size > end)) char_size = end - begin;
      if (!trim_ctx.Contains(begin, char_size)) break;
      begin += char_size;
    }
  }
  // Find new ending position.
  if constexpr (D == TRAILING || D == BOTH) {
    while (begin < end) {
      int char_index = FindUtf8PosBackward(begin, end - begin, 0);
      DCHECK_NE(char_index, -1);
      const uint8_t* char_begin = begin + char_index;
      if (!trim_ctx.Contains(char_begin, end - char_begin)) break;
      end = char_begin;
    }
  }

  return StringVal(const_cast<uint8_t*>(begin), end - begin);
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

  bool utf8_mode = context->impl()->GetConstFnAttr(FunctionContextImpl::UTF8_MODE);
  StringValue haystack = StringValue::FromStringVal(str);
  StringValue::SimpleString haystack_s = haystack.ToSimpleString();
  StringValue needle = StringValue::FromStringVal(substr);
  StringValue::SimpleString needle_s = needle.ToSimpleString();
  StringSearch search(&needle);
  int match_pos = -1;
  if (start_position.val > 0) {
    // A positive starting position indicates regular searching from the left.
    int search_start_pos = start_position.val - 1;
    if (utf8_mode) {
      search_start_pos = FindUtf8PosForward(str.ptr, str.len, search_start_pos);
    }
    if (search_start_pos >= haystack_s.len) return IntVal(0);
    for (int match_num = 0; match_num < occurrence.val; ++match_num) {
      DCHECK_LE(search_start_pos, haystack_s.len);
      StringValue haystack_substring = haystack.Substring(search_start_pos);
      int match_pos_in_substring = search.Search(&haystack_substring);
      if (match_pos_in_substring < 0) return IntVal(0);
      match_pos = search_start_pos + match_pos_in_substring;
      search_start_pos = match_pos + 1;
    }
  } else {
    // A negative starting position indicates searching from the right.
    int search_start_pos = utf8_mode ?
        FindUtf8PosBackward(str.ptr, str.len, -start_position.val - 1) :
        haystack_s.len + start_position.val;
    // The needle must fit between search_start_pos and the end of the string
    if (search_start_pos + needle_s.len > haystack_s.len) {
      search_start_pos = haystack_s.len - needle_s.len;
    }
    if (search_start_pos < 0) return IntVal(0);
    for (int match_num = 0; match_num < occurrence.val; ++match_num) {
      DCHECK_GE(search_start_pos + needle_s.len, 0);
      DCHECK_LE(search_start_pos + needle_s.len, haystack_s.len);
      StringValue haystack_substring =
          haystack.Substring(0, search_start_pos + needle_s.len);
      match_pos = search.RSearch(&haystack_substring);
      if (match_pos < 0) return IntVal(0);
      search_start_pos = match_pos - 1;
    }
  }
  // In UTF8 mode, positions are counted by Unicode characters in UTF8 encoding.
  // If not in UTF8 mode, return positions starting from 1 at the leftmost position.
  return utf8_mode ? IntVal(CountUtf8Chars(str.ptr, match_pos) + 1) :
      IntVal(match_pos + 1);
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
  return Instr(context, str, substr, start_pos);
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
  // Set the maximum memory used by re2's regex engine for storage
  StringFunctions::SetRE2MemOpt(&options);
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

void StringFunctions::SetRE2MemLimit(int64_t re2_mem_limit) {
  // TODO: include the memory requirement for re2 in the memory planner estimates
  DCHECK(re2_mem_limit > 0);
  StringFunctions::re2_mem_limit_ = re2_mem_limit;
}

// Set the maximum memory used by re2's regex engine for a compiled regex expression's
// storage. By default, it uses 8 MiB. This can be used to avoid DFA state cache flush
// resulting in slower execution
void StringFunctions::SetRE2MemOpt(re2::RE2::Options* opts) {
  opts->set_max_mem(StringFunctions::re2_mem_limit_);
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

// NULL handling of function Concat and ConcatWs are different.
// Function concat was reimplemented to keep the original
// NULL handling.
StringVal StringFunctions::Concat(
    FunctionContext* context, int num_children, const StringVal* strs) {
  DCHECK_GE(num_children, 1);
  DCHECK(strs != nullptr);
  // Pass through if there's only one argument.
  if (num_children == 1) return strs[0];

  // Loop once to compute the final size and reserve space.
  int64_t total_size = 0;
  for (int32_t i = 0; i < num_children; ++i) {
    if (strs[i].is_null) return StringVal::null();
    total_size += strs[i].len;
  }

  if (total_size > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
         "Concatenated string length",
         PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
  }

  // If total_size is zero, directly returns empty string
  if (total_size <= 0) return StringVal();

  StringVal result(context, total_size);
  if (UNLIKELY(result.is_null)) return StringVal::null();

  // Loop again to append the data.
  uint8_t* ptr = result.ptr;
  for (int32_t i = 0; i < num_children; ++i) {
    Ubsan::MemCpy(ptr, strs[i].ptr, strs[i].len);
    ptr += strs[i].len;
  }
  return result;
}

StringVal StringFunctions::ConcatWs(FunctionContext* context, const StringVal& sep,
    int num_children, const StringVal* strs) {
  DCHECK_GE(num_children, 1);
  DCHECK(strs != nullptr);
  if (sep.is_null) return StringVal::null();

  // Loop once to compute valid start index, final string size and valid string object
  // count.
  int32_t valid_num_children = 0;
  int32_t valid_start_index = -1;
  int64_t total_size = 0;
  for (int32_t i = 0; i < num_children; ++i) {
    if (strs[i].is_null) continue;

    if (valid_start_index == -1) {
      valid_start_index = i;
      // Calculate the space required by first valid string object.
      total_size += strs[i].len;
    } else {
      // Calculate the space required by subsequent valid string object.
      total_size += sep.len + strs[i].len;
    }
    // Record the count of valid string object.
    valid_num_children++;
  }

  if (total_size > StringVal::MAX_LENGTH) {
    context->SetError(Substitute(ERROR_CHARACTER_LIMIT_EXCEEDED,
         "Concatenated string length",
         PrettyPrinter::Print(StringVal::MAX_LENGTH, TUnit::BYTES)).c_str());
    return StringVal::null();
  }

  // If all data are invalid, or data size is zero, return empty string.
  if (valid_start_index < 0 || total_size <= 0) {
    return StringVal();
  }
  DCHECK_GT(valid_num_children, 0);

  // Pass through if there's only one argument.
  if (valid_num_children == 1) return strs[valid_start_index];

  // Reserve space needed by final result.
  StringVal result(context, total_size);
  if (UNLIKELY(result.is_null)) return StringVal::null();

  // Loop to append the data.
  uint8_t* ptr = result.ptr;
  Ubsan::MemCpy(ptr, strs[valid_start_index].ptr, strs[valid_start_index].len);
  ptr += strs[valid_start_index].len;
  for (int32_t i = valid_start_index + 1; i < num_children; ++i) {
    if (strs[i].is_null) continue;
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
// Parameter 'direction' controls the direction of searching, can be either 1 or -1
static char* LocateSubstring(char* haystack, const int hay_len, const char* needle,
    const int needle_len, const int direction = 1) {
  DCHECK_GT(needle_len, 0);
  DCHECK(needle != NULL);
  DCHECK(hay_len == 0 || haystack != NULL);
  DCHECK(direction == 1 || direction == -1);
  if (hay_len < needle_len) return nullptr;
  char* start = haystack;
  if (direction == -1) start += hay_len - needle_len;
  for (int i = 0; i < hay_len - needle_len + 1; ++i) {
    char* possible_needle = start + direction * i;
    if (strncmp(possible_needle, needle, needle_len) == 0) return possible_needle;
  }
  return nullptr;
}

StringVal StringFunctions::SplitPart(FunctionContext* context,
    const StringVal& str, const StringVal& delim, const BigIntVal& field) {
  if (str.is_null || delim.is_null || field.is_null) return StringVal::null();
  int field_pos = field.val;
  if (field_pos == 0) {
    stringstream ss;
    ss << "Invalid field position: " << field.val;
    context->SetError(ss.str().c_str());
    return StringVal::null();
  }
  if (delim.len == 0) return str;
  char* str_start = reinterpret_cast<char*>(str.ptr);
  char* delimiter = reinterpret_cast<char*>(delim.ptr);
  const int DIRECTION = field_pos > 0 ? 1 : -1;
  char* window_start = str_start;
  char* window_end = str_start + str.len;
  for (int cur_pos = DIRECTION; ; cur_pos += DIRECTION) {
    int remaining_len = window_end - window_start;
    char* delim_ref = LocateSubstring(window_start, remaining_len, delimiter, delim.len,
        DIRECTION);
    if (delim_ref == nullptr) {
      if (cur_pos == field_pos) {
        return StringVal(reinterpret_cast<uint8_t*>(window_start), remaining_len);
      }
      // Return empty string if required field position is not found.
      return StringVal();
    }
    if (cur_pos == field_pos) {
      if (DIRECTION < 0) {
        window_start = delim_ref + delim.len;
      }
      else {
        window_end = delim_ref;
      }
      return StringVal(reinterpret_cast<uint8_t*>(window_start),
          window_end - window_start);
    }
    if (DIRECTION < 0) {
      window_end = delim_ref;
    } else {
      window_start = delim_ref + delim.len;
    }
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
  unsigned out_len = 0;
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
  unsigned out_len = 0;
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

  int* column = reinterpret_cast<int*>(ctx->Allocate(sizeof(int) * (s1len + 1)));
  if (UNLIKELY(column == nullptr)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return IntVal::null();
  }

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

// Based on https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
// Implements Jaro similarity
DoubleVal StringFunctions::JaroSimilarity(
    FunctionContext* ctx, const StringVal& s1, const StringVal& s2) {

  int s1len = s1.len;
  int s2len = s2.len;

  // error if either input exceeds 255 characters
  if (s1len > 255 || s2len > 255) {
    ctx->SetError("jaro argument exceeds maximum length of 255 characters");
    return DoubleVal(-1.0);
  }

  // short cut cases:
  // - null strings
  // - zero length strings
  // - identical length and value strings
  if (s1.is_null || s2.is_null) return DoubleVal::null();
  if (s1len == 0 && s2len == 0) return DoubleVal(1.0);
  if (s1len == 0 || s2len == 0) return DoubleVal(0.0);
  if (s1len == s2len && memcmp(s1.ptr, s2.ptr, s1len) == 0) return DoubleVal(1.0);

  // the window size to search for matches in the other string
  int max_range = std::max(0, std::max(s1len, s2len) / 2 - 1);

  int* s1_matching = reinterpret_cast<int*>(ctx->Allocate(sizeof(int) * (s1len)));
  if (UNLIKELY(s1_matching == nullptr)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return DoubleVal::null();
  }

  int* s2_matching = reinterpret_cast<int*>(ctx->Allocate(sizeof(int) * (s2len)));
  if (UNLIKELY(s2_matching == nullptr)) {
    ctx->Free(reinterpret_cast<uint8_t*>(s1_matching));
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return DoubleVal::null();
  }

  std::fill_n(s1_matching, s1len, -1);
  std::fill_n(s2_matching, s2len, -1);

  // calculate matching characters
  int matching_characters = 0;
  for (int i = 0; i < s1len; i++) {
    // matching window
    int min_index = std::max(i - max_range, 0);
    int max_index = std::min(i + max_range + 1, s2len);
    if (min_index >= max_index) break;

    for (int j = min_index; j < max_index; j++) {
      if (s2_matching[j] == -1 && s1.ptr[i] == s2.ptr[j]) {
        s1_matching[i] = i;
        s2_matching[j] = j;
        matching_characters++;
        break;
      }
    }
  }

  if (matching_characters == 0) {
    ctx->Free(reinterpret_cast<uint8_t*>(s1_matching));
    ctx->Free(reinterpret_cast<uint8_t*>(s2_matching));
    return DoubleVal(0.0);
  }

  // transpositions (one-way only)
  double transpositions = 0.0;
  for (int i = 0, s1i = 0, s2i = 0; i < matching_characters; i++) {
    while (s1_matching[s1i] == -1) {
      s1i++;
    }
    while (s2_matching[s2i] == -1) {
      s2i++;
    }
    if (s1.ptr[s1i] != s2.ptr[s2i]) transpositions += 0.5;
    s1i++;
    s2i++;
  }
  double m = static_cast<double>(matching_characters);
  double jaro_similarity = 1.0 / 3.0  * ( m / static_cast<double>(s1len)
                                        + m / static_cast<double>(s2len)
                                        + (m - transpositions) / m );

  ctx->Free(reinterpret_cast<uint8_t*>(s1_matching));
  ctx->Free(reinterpret_cast<uint8_t*>(s2_matching));

  return DoubleVal(jaro_similarity);
}

DoubleVal StringFunctions::JaroDistance(
    FunctionContext* ctx, const StringVal& s1, const StringVal& s2) {

  DoubleVal jaro_similarity = StringFunctions::JaroSimilarity(ctx, s1, s2);
  if (jaro_similarity.is_null) return DoubleVal::null();
  if (jaro_similarity.val == -1.0) return DoubleVal(-1.0);
  return DoubleVal(1.0 - jaro_similarity.val);
}

DoubleVal StringFunctions::JaroWinklerDistance(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2) {
  return StringFunctions::JaroWinklerDistance(ctx, s1, s2,
    DoubleVal(0.1), DoubleVal(0.7));
}

DoubleVal StringFunctions::JaroWinklerDistance(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2,
      const DoubleVal& scaling_factor) {
  return StringFunctions::JaroWinklerDistance(ctx, s1, s2,
    scaling_factor, DoubleVal(0.7));
}

// Based on https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
// Implements Jaro-Winkler distance
// Extended with boost_theshold: Winkler's modification only applies if Jaro exceeds it
DoubleVal StringFunctions::JaroWinklerDistance(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2,
      const DoubleVal& scaling_factor, const DoubleVal& boost_threshold) {

  DoubleVal jaro_winkler_similarity = StringFunctions::JaroWinklerSimilarity(
    ctx, s1, s2, scaling_factor, boost_threshold);

  if (jaro_winkler_similarity.is_null) return DoubleVal::null();
  if (jaro_winkler_similarity.val == -1.0) return DoubleVal(-1.0);
  return DoubleVal(1.0 - jaro_winkler_similarity.val);
}

DoubleVal StringFunctions::JaroWinklerSimilarity(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2) {
  return StringFunctions::JaroWinklerSimilarity(ctx, s1, s2,
    DoubleVal(0.1), DoubleVal(0.7));
}

DoubleVal StringFunctions::JaroWinklerSimilarity(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2,
      const DoubleVal& scaling_factor) {
  return StringFunctions::JaroWinklerSimilarity(ctx, s1, s2,
    scaling_factor, DoubleVal(0.7));
}

// Based on https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
// Implements Jaro-Winkler similarity
// Extended with boost_theshold: Winkler's modification only applies if Jaro exceeds it
DoubleVal StringFunctions::JaroWinklerSimilarity(FunctionContext* ctx,
      const StringVal& s1, const StringVal& s2,
      const DoubleVal& scaling_factor, const DoubleVal& boost_threshold) {

  constexpr int MAX_PREFIX_LENGTH = 4;
  int s1len = s1.len;
  int s2len = s2.len;

  // error if either input exceeds 255 characters
  if (s1len > 255 || s2len > 255) {
    ctx->SetError("jaro-winkler argument exceeds maximum length of 255 characters");
    return DoubleVal(-1.0);
  }
  // scaling factor has to be between 0.0 and 0.25
  if (scaling_factor.val < 0.0 || scaling_factor.val > 0.25) {
    ctx->SetError("jaro-winkler scaling factor values can range between 0.0 and 0.25");
    return DoubleVal(-1.0);
  }
  // error if boost threshold is out of range 0.0..1.0
  if (boost_threshold.val < 0.0 || boost_threshold.val > 1.0) {
    ctx->SetError("jaro-winkler boost threshold values can range between 0.0 and 1.0");
    return DoubleVal(-1.0);
  }

  if (s1.is_null || s2.is_null) return DoubleVal::null();

  DoubleVal jaro_similarity = StringFunctions::JaroSimilarity(ctx, s1, s2);
  if (jaro_similarity.is_null) return DoubleVal::null();
  if (jaro_similarity.val == -1.0) return DoubleVal(-1.0);

  double jaro_winkler_similarity = jaro_similarity.val;

  if (jaro_similarity.val > boost_threshold.val) {
    int common_length = std::min(MAX_PREFIX_LENGTH, std::min(s1len, s2len));
    int common_prefix = 0;
    while (common_prefix < common_length &&
           s1.ptr[common_prefix] == s2.ptr[common_prefix]) {
      common_prefix++;
    }

    jaro_winkler_similarity += common_prefix * scaling_factor.val *
      (1.0 - jaro_similarity.val);
  }
  return DoubleVal(jaro_winkler_similarity);
}

IntVal StringFunctions::DamerauLevenshtein(
    FunctionContext* ctx, const StringVal& s1, const StringVal& s2) {
  // Based on https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance
  // Implements restricted Damerau-Levenshtein (optimal string alignment)

  int s1len = s1.len;
  int s2len = s2.len;

  // error if either input exceeds 255 characters
  if (s1len > 255 || s2len > 255) {
    ctx->SetError("damerau-levenshtein argument exceeds maximum length of 255 "
                  "characters");
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

  int i;
  int j;
  int l_cost;
  int ptr_array_length = sizeof(int*) * (s1len + 1);
  int int_array_length = sizeof(int) * (s2len + 1) * (s1len + 1);

  // Allocating a 2D array (with d being an array of pointers to the start of the rows)
  int** d = reinterpret_cast<int**>(ctx->Allocate(ptr_array_length));
  if (UNLIKELY(d == nullptr)) {
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return IntVal::null();
  }
  int* rows = reinterpret_cast<int*>(ctx->Allocate(int_array_length));
  if (UNLIKELY(rows == nullptr)) {
    ctx->Free(reinterpret_cast<uint8_t*>(d));
    DCHECK(!ctx->impl()->state()->GetQueryStatus().ok());
    return IntVal::null();
  }
  // Setting the pointers in the pointer-array to the start of (s2len + 1) length
  // intervals and initializing its values based on the mentioned algorithm.
  for (i = 0; i <= s1len; ++i) {
    d[i] = rows + (s2len + 1) * i;
    d[i][0] = i;
  }
  std::iota(d[0], d[0] + s2len + 1, 0);

  for (i = 1; i <= s1len; ++i) {
    for (j = 1; j <= s2len; ++j) {
      if (s1.ptr[i - 1] == s2.ptr[j - 1]) {
        l_cost = 0;
      } else {
        l_cost = 1;
      }
      d[i][j] = std::min(d[i - 1][j - 1] + l_cost, // substitution
                         std::min(d[i][j - 1] + 1, // insertion
                                  d[i - 1][j] + 1) // deletion
      );
      if (i > 1 && j > 1 && s1.ptr[i - 1] == s2.ptr[j - 2]
          && s1.ptr[i - 2] == s2.ptr[j - 1]) {
        d[i][j] = std::min(d[i][j], d[i - 2][j - 2] + l_cost); // transposition
      }
    }
  }
  int result = d[s1len][s2len];

  ctx->Free(reinterpret_cast<uint8_t*>(d));
  ctx->Free(reinterpret_cast<uint8_t*>(rows));
  return IntVal(result);
}

template <typename T>
static StringVal prettyPrint(FunctionContext* context, const T& int_val,
    const TUnit::type& unit) {
  if (int_val.is_null) {
    return StringVal::null();
  }

  const string& fmt_str = PrettyPrinter::Print(int_val.val, unit);

  StringVal result(context, fmt_str.size());
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  memcpy(ptr, fmt_str.c_str(), fmt_str.size());

  return result;
}

StringVal StringFunctions::PrettyPrintMemory(FunctionContext* context,
    const BigIntVal& bytes) {
  return prettyPrint(context, bytes, TUnit::BYTES);
}

StringVal StringFunctions::PrettyPrintMemory(FunctionContext* context,
    const IntVal& bytes) {
  return prettyPrint(context, bytes, TUnit::BYTES);
}

StringVal StringFunctions::PrettyPrintMemory(FunctionContext* context,
    const SmallIntVal& bytes) {
  return prettyPrint(context, bytes, TUnit::BYTES);
}

StringVal StringFunctions::PrettyPrintMemory(FunctionContext* context,
    const TinyIntVal& bytes) {
  return prettyPrint(context, bytes, TUnit::BYTES);
}

}
