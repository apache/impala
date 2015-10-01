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

#include "exprs/string-functions.h"

#include <cctype>
#include <stdint.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <bitset>

#include "exprs/anyval-util.h"
#include "exprs/expr.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple-row.h"
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
  memset(result.ptr, ' ', len.val);
  return result;
}

StringVal StringFunctions::Repeat(
    FunctionContext* context, const StringVal& str, const BigIntVal& n) {
  if (str.is_null || n.is_null) return StringVal::null();
  if (str.len == 0 || n.val <= 0) return StringVal();
  StringVal result(context, str.len * n.val);
  if (UNLIKELY(result.is_null)) return result;
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
  if (result.is_null) return result;
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
  if (UNLIKELY(result.is_null)) return result;
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
  if (UNLIKELY(result.is_null)) return result;
  for (int i = 0; i < str.len; ++i) {
    result.ptr[i] = ::tolower(str.ptr[i]);
  }
  return result;
}

StringVal StringFunctions::Upper(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return result;
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
  if (UNLIKELY(result.is_null)) return result;
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

StringVal StringFunctions::Reverse(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  StringVal result(context, str.len);
  if (UNLIKELY(result.is_null)) return result;
  std::reverse_copy(str.ptr, str.ptr + str.len, result.ptr);
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

StringVal StringFunctions::Trim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str.len && str.ptr[begin] == ' ') {
    ++begin;
  }
  // Find new ending position.
  int32_t end = str.len - 1;
  while (end > begin && str.ptr[end] == ' ') {
    --end;
  }
  return StringVal(str.ptr + begin, end - begin + 1);
}

StringVal StringFunctions::Ltrim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str.len && str.ptr[begin] == ' ') {
    ++begin;
  }
  return StringVal(str.ptr + begin, str.len - begin);
}

StringVal StringFunctions::Rtrim(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return StringVal::null();
  if (str.len == 0) return str;
  // Find new ending position.
  int32_t end = str.len - 1;
  while (end > 0 && str.ptr[end] == ' ') {
    --end;
  }
  DCHECK_GE(end, 0);
  return StringVal(str.ptr, (str.ptr[end] == ' ') ? end : end + 1);
}

IntVal StringFunctions::Ascii(FunctionContext* context, const StringVal& str) {
  if (str.is_null) return IntVal::null();
  // Hive returns 0 when given an empty string.
  return IntVal((str.len == 0) ? 0 : static_cast<int32_t>(str.ptr[0]));
}

IntVal StringFunctions::Instr(FunctionContext* context, const StringVal& str,
    const StringVal& substr) {
  if (str.is_null || substr.is_null) return IntVal::null();
  StringValue str_sv = StringValue::FromStringVal(str);
  StringValue substr_sv = StringValue::FromStringVal(substr);
  StringSearch search(&substr_sv);
  // Hive returns positions starting from 1.
  return IntVal(search.Search(&str_sv) + 1);
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
re2::RE2* CompileRegex(const StringVal& pattern, string* error_str) {
  re2::StringPiece pattern_sp(reinterpret_cast<char*>(pattern.ptr), pattern.len);
  re2::RE2::Options options;
  // Disable error logging in case e.g. every row causes an error
  options.set_log_errors(false);
  // Return the leftmost longest match (rather than the first match).
  options.set_longest_match(true);
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

void StringFunctions::RegexpPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* pattern = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  if (pattern->is_null) return;

  string error_str;
  re2::RE2* re = CompileRegex(*pattern, &error_str);
  if (re == NULL) {
    context->SetError(error_str.c_str());
    return;
  }
  context->SetFunctionState(scope, re);
}

void StringFunctions::RegexpClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  re2::RE2* re = reinterpret_cast<re2::RE2*>(context->GetFunctionState(scope));
  delete re;
}

StringVal StringFunctions::RegexpExtract(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const BigIntVal& index) {
  if (str.is_null || pattern.is_null || index.is_null) return StringVal::null();
  if (index.val < 0) return StringVal();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if we have to locally compile it
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str);
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
      re->Match(str_sp, 0, str.len, re2::RE2::UNANCHORED, &matches[0], max_matches);
  if (!success) return StringVal();
  // matches[0] is the whole string, matches[1] the first group, etc.
  const re2::StringPiece& match = matches[index.val];
  return AnyValUtil::FromBuffer(context, match.data(), match.size());
}

StringVal StringFunctions::RegexpReplace(FunctionContext* context, const StringVal& str,
    const StringVal& pattern, const StringVal& replace) {
  if (str.is_null || pattern.is_null || replace.is_null) return StringVal::null();

  re2::RE2* re = reinterpret_cast<re2::RE2*>(
      context->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
  scoped_ptr<re2::RE2> scoped_re; // destroys re if state->re is NULL
  if (re == NULL) {
    DCHECK(!context->IsArgConstant(1));
    string error_str;
    re = CompileRegex(pattern, &error_str);
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

StringVal StringFunctions::Concat(FunctionContext* context, int num_children,
    const StringVal* strs) {
  return ConcatWs(context, StringVal(), num_children, strs);
}

StringVal StringFunctions::ConcatWs(FunctionContext* context, const StringVal& sep,
    int num_children, const StringVal* strs) {
  DCHECK_GE(num_children, 1);
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
  uint8_t* ptr = result.ptr;

  // Loop again to append the data.
  memcpy(ptr, strs[0].ptr, strs[0].len);
  ptr += strs[0].len;
  for (int32_t i = 1; i < num_children; ++i) {
    memcpy(ptr, sep.ptr, sep.len);
    ptr += sep.len;
    memcpy(ptr, strs[i].ptr, strs[i].len);
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
    while(str_set.ptr[end] != ',' && end < str_set.len) ++end;
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
  UrlParser::UrlPart* url_part = new UrlParser::UrlPart;
  *url_part = UrlParser::GetUrlPart(StringValue::FromStringVal(*part));
  if (*url_part == UrlParser::INVALID) {
    stringstream ss;
    ss << "Invalid URL part: " << AnyValUtil::ToString(*part) << endl
       << "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', "
       << "'USERINFO', and 'QUERY')";
    ctx->SetError(ss.str().c_str());
    return;
  }
  ctx->SetFunctionState(scope, url_part);
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
  if (url_part == NULL) return;
  delete url_part;
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

void StringFunctions::BTrimPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  // Create a bitset to hold the unique characters to trim.
  bitset<256>* unique_chars = new bitset<256>();
  context->SetFunctionState(scope, unique_chars);
  if (!context->IsArgConstant(1)) return;
  DCHECK_EQ(context->GetArgType(1)->type, FunctionContext::TYPE_STRING);
  StringVal* chars_to_trim = reinterpret_cast<StringVal*>(context->GetConstantArg(1));
  for (int32_t i = 0; i < chars_to_trim->len; ++i) {
    unique_chars->set(static_cast<int>(chars_to_trim->ptr[i]), true);
  }
}

void StringFunctions::BTrimClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  bitset<256>* unique_chars = reinterpret_cast<bitset<256>*>(
      context->GetFunctionState(scope));
  if (unique_chars != NULL) delete unique_chars;
}

StringVal StringFunctions::BTrimString(FunctionContext* ctx,
    const StringVal& str, const StringVal& chars_to_trim) {
  if (str.is_null) return StringVal::null();
  bitset<256>* unique_chars = reinterpret_cast<bitset<256>*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  // When 'chars_to_trim' is unique for each element (e.g. when 'chars_to_trim'
  // is each element of a table column), we need to prepare a bitset of unique
  // characters here instead of using the bitset from function context.
  if (!ctx->IsArgConstant(1)) {
    unique_chars->reset();
    for (int32_t i = 0; i < chars_to_trim.len; ++i) {
      unique_chars->set(static_cast<int>(chars_to_trim.ptr[i]), true);
    }
  }
  // Find new starting position.
  int32_t begin = 0;
  while (begin < str.len &&
      unique_chars->test(static_cast<int>(str.ptr[begin]))) {
    ++begin;
  }
  // Find new ending position.
  int32_t end = str.len - 1;
  while (end > begin && unique_chars->test(static_cast<int>(str.ptr[end]))) {
    --end;
  }
  return StringVal(str.ptr + begin, end - begin + 1);
}

// Similar to strstr() except that the strings are not null-terminated
static char* locate_substring(char* haystack, int hay_len, char* needle, int needle_len) {
  DCHECK_GT(needle_len, 0);
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
    char* delim_ref = locate_substring(str_part, remaining_len, delimiter, delim.len);
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

}
