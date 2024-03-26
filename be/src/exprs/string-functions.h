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


#ifndef IMPALA_EXPRS_STRING_FUNCTIONS_H
#define IMPALA_EXPRS_STRING_FUNCTIONS_H

#include <re2/re2.h>
#include <bitset>

#include "runtime/string-value.h"
#include "runtime/string-search.h"

using namespace impala_udf;

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
 public:
  // String trimming position or direction
  enum TrimPosition {
    LEADING, // Trim from the begining, or leading end
    TRAILING, // Trim from the right, or trailing end
    BOTH // Trim from both ends of string
  };

  // A utility class for supporting the UTF-8 Trim() function, initialized with the input
  // string to be trimmed. After Reset(), the Contains function can be used to determine
  // if a character needs to be trimmed.
  class TrimContext {
   public:
    TrimContext(bool utf8_mode) : utf8_mode_(utf8_mode) { }

    void Reset(const StringVal& chars_to_trim);

    inline bool Contains(uint8_t single_char) const {
      return single_byte_chars_.test(single_char);
    }

    inline bool Contains(const uint8_t* utf8_char, int len) const;

    bool utf8_mode() const { return utf8_mode_; }

   private:
    const bool utf8_mode_;

    // The bitset to hold the unique characters to trim, used for non-UTF-8 characters
    // or single-byte UTF-8 characters.
    std::bitset<256> single_byte_chars_;

    // Pointers to multi-byte UTF-8 characters used to check whether characters of the
    // corresponding byte count need to be trimmed.
    std::vector<const uint8_t*> double_byte_chars_;
    std::vector<const uint8_t*> triple_byte_chars_;
    std::vector<const uint8_t*> quadruple_byte_chars_;
  };

  static StringVal Substring(FunctionContext*, const StringVal& str, const BigIntVal& pos,
      const BigIntVal& len);
  static StringVal Substring(FunctionContext*, const StringVal& str,
      const BigIntVal& pos);
  static StringVal Utf8Substring(FunctionContext*, const StringVal& str,
      const BigIntVal& pos, const BigIntVal& len);
  static StringVal Utf8Substring(FunctionContext*, const StringVal& str,
      const BigIntVal& pos);
  static StringVal SplitPart(FunctionContext* context, const StringVal& str,
      const StringVal& delim, const BigIntVal& field);
  static StringVal Left(FunctionContext*, const StringVal& str, const BigIntVal& len);
  static StringVal Right(FunctionContext*, const StringVal& str, const BigIntVal& len);
  static StringVal Space(FunctionContext*, const BigIntVal& len);
  static StringVal Repeat(FunctionContext*, const StringVal& str, const BigIntVal& n);
  static StringVal Lpad(FunctionContext*, const StringVal& str, const BigIntVal& len,
      const StringVal& pad);
  static StringVal Rpad(FunctionContext*, const StringVal& str, const BigIntVal&,
      const StringVal& pad);
  static IntVal Bytes(FunctionContext*, const StringVal& str);
  static IntVal Length(FunctionContext*, const StringVal& str);
  static IntVal CharLength(FunctionContext*, const StringVal& str);
  static IntVal Utf8Length(FunctionContext*, const StringVal& str);
  static StringVal Lower(FunctionContext*, const StringVal& str);
  static StringVal LowerAscii(FunctionContext*, const StringVal& str);
  static StringVal LowerUtf8(FunctionContext*, const StringVal& str);
  static StringVal Upper(FunctionContext*, const StringVal& str);
  static StringVal UpperAscii(FunctionContext*, const StringVal& str);
  static StringVal UpperUtf8(FunctionContext*, const StringVal& str);
  static StringVal InitCap(FunctionContext*, const StringVal& str);
  static StringVal InitCapAscii(FunctionContext*, const StringVal& str);
  static StringVal InitCapUtf8(FunctionContext*, const StringVal& str);
  static void ReplacePrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static void ReplaceClose(FunctionContext*, FunctionContext::FunctionStateScope);
  static StringVal Replace(FunctionContext*, const StringVal& str,
      const StringVal& pattern, const StringVal& replace);
  static StringVal Reverse(FunctionContext*, const StringVal& str);
  static StringVal Utf8Reverse(FunctionContext*, const StringVal& str);
  static StringVal Translate(FunctionContext*, const StringVal& str, const StringVal& src,
      const StringVal& dst);
  static StringVal Trim(FunctionContext*, const StringVal& str);
  static StringVal Ltrim(FunctionContext*, const StringVal& str);
  static StringVal Rtrim(FunctionContext*, const StringVal& str);

  /// Sets up arguments and function context for the *TrimString functions below.
  static void TrimPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static void Utf8TrimPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  /// Cleans up the work done by TrimPrepare above.
  static void TrimClose(FunctionContext*, FunctionContext::FunctionStateScope);

  /// Trims occurrences of the characters in 'chars_to_trim' string from
  /// the beginning of string 'str'.
  static StringVal LTrimString(FunctionContext* ctx, const StringVal& str,
      const StringVal& chars_to_trim);
  /// Trims occurrences of the characters in 'chars_to_trim' string from
  /// the end of string 'str'.
  static StringVal RTrimString(FunctionContext* ctx, const StringVal& str,
      const StringVal& chars_to_trim);
  /// Trims occurrences of the characters in 'chars_to_trim' string from
  /// both ends of string 'str'.
  static StringVal BTrimString(FunctionContext* ctx, const StringVal& str,
      const StringVal& chars_to_trim);

  static IntVal Ascii(FunctionContext*, const StringVal& str);
  static IntVal Instr(FunctionContext*, const StringVal& str, const StringVal& substr,
      const BigIntVal& start_position, const BigIntVal& occurrence);
  static IntVal Instr(FunctionContext*, const StringVal& str, const StringVal& substr,
      const BigIntVal& start_position);
  static IntVal Instr(FunctionContext*, const StringVal& str, const StringVal& substr);

  static IntVal Locate(FunctionContext*, const StringVal& substr, const StringVal& str);
  static IntVal LocatePos(FunctionContext*, const StringVal& substr, const StringVal& str,
      const BigIntVal& start_pos);

  static bool SetRE2Options(const StringVal& match_parameter, std::string* error_str,
      re2::RE2::Options* opts);
  static void SetRE2MemOpt(re2::RE2::Options* opts);
  static void RegexpPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static void RegexpClose(FunctionContext*, FunctionContext::FunctionStateScope);
  static StringVal RegexpEscape(FunctionContext*, const StringVal& str);
  static StringVal RegexpExtract(FunctionContext*, const StringVal& str,
      const StringVal& pattern, const BigIntVal& index);
  static StringVal RegexpReplace(FunctionContext*, const StringVal& str,
      const StringVal& pattern, const StringVal& replace);
  static void RegexpMatchCountPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  static IntVal RegexpMatchCount2Args(FunctionContext* context, const StringVal& str,
      const StringVal& pattern);
  static IntVal RegexpMatchCount4Args(FunctionContext* context, const StringVal& str,
      const StringVal& pattern, const IntVal& start_pos,
      const StringVal& match_parameter);
  static StringVal Concat(FunctionContext*, int num_children, const StringVal* strs);
  static StringVal ConcatWs(FunctionContext*, const StringVal& sep, int num_children,
      const StringVal* strs);
  static IntVal FindInSet(FunctionContext*, const StringVal& str,
      const StringVal& str_set);

  static void ParseUrlPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static StringVal ParseUrl(FunctionContext*, const StringVal& url,
      const StringVal& part);
  static StringVal ParseUrlKey(FunctionContext*, const StringVal& url,
      const StringVal& key, const StringVal& part);
  static void ParseUrlClose(FunctionContext*, FunctionContext::FunctionStateScope);

  static void SetRE2MemLimit(int64_t re2_mem_limit);

  /// Converts ASCII 'val' to corresponding character.
  static StringVal Chr(FunctionContext* context, const IntVal& val);

  static StringVal Base64Encode(FunctionContext* ctx, const StringVal& str);
  static StringVal Base64Decode(FunctionContext* ctx, const StringVal& str);

  static StringVal GetJsonObject(FunctionContext* ctx, const StringVal& json_str,
      const StringVal& path_str);
  /// Implementation of GetJsonObject, not cross-compiled since no significant benefits
  /// can gain.
  static StringVal GetJsonObjectImpl(FunctionContext* ctx, const StringVal& json_str,
      const StringVal& path_str);

  static IntVal Levenshtein(
      FunctionContext* context, const StringVal& s1, const StringVal& s2);

  static IntVal DamerauLevenshtein(
      FunctionContext* context, const StringVal& s1, const StringVal& s2);

  static DoubleVal JaroDistance(
      FunctionContext* ctx, const StringVal& s1, const StringVal& s2);

  static DoubleVal JaroSimilarity(
      FunctionContext* ctx, const StringVal& s1, const StringVal& s2);

  static DoubleVal JaroWinklerDistance(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2);

  static DoubleVal JaroWinklerDistance(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2, const DoubleVal& scaling_factor);

  static DoubleVal JaroWinklerDistance(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2, const DoubleVal& scaling_factor,
      const DoubleVal& boost_threshold);

  static DoubleVal JaroWinklerSimilarity(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2);

  static DoubleVal JaroWinklerSimilarity(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2, const DoubleVal& scaling_factor);

  static DoubleVal JaroWinklerSimilarity(FunctionContext* ctx, const StringVal& s1,
      const StringVal& s2, const DoubleVal& scaling_factor,
      const DoubleVal& boost_threshold);

  /// Converts bytes stored as an integer value into human readable memory measurements.
  /// For example, 123456789012 bytes is converted to "114.98 GB".
  static StringVal PrettyPrintMemory(FunctionContext*, const BigIntVal& bytes);
  static StringVal PrettyPrintMemory(FunctionContext*, const IntVal& bytes);
  static StringVal PrettyPrintMemory(FunctionContext*, const SmallIntVal& bytes);
  static StringVal PrettyPrintMemory(FunctionContext*, const TinyIntVal& bytes);

 private:
  static uint64_t re2_mem_limit_;

  static void DoTrimPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope, bool utf8_mode);

  /// Templatized implementation of the actual string trimming function.
  /// The first parameter, 'D', is one of StringFunctions::TrimPosition values.
  /// The second parameter, 'IS_IMPLICIT_WHITESPACE', is true when the set of characters
  /// to trim is implicitly set to ' ', as a result of calling the one-arg
  /// forms of trim/ltrim/rtrim.
  template <TrimPosition D, bool IS_IMPLICIT_WHITESPACE>
  static StringVal DoTrimString(FunctionContext* ctx, const StringVal& str,
      const StringVal& chars_to_trim);

  /// Templatized implementation of the actual string trimming function with UTF-8
  /// character handling.
  /// The first parameter, 'D', is one of the values of StringFunctions::TrimPosition.
  template <StringFunctions::TrimPosition D>
  static StringVal DoUtf8TrimString(const StringVal& str, const TrimContext& trim_ctx);
};
}
#endif
