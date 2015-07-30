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


#ifndef IMPALA_EXPRS_STRING_FUNCTIONS_H
#define IMPALA_EXPRS_STRING_FUNCTIONS_H

#include "runtime/string-value.h"
#include "runtime/string-search.h"

using namespace impala_udf;

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
 public:
  static StringVal Substring(FunctionContext*, const StringVal& str, const BigIntVal& pos,
                             const BigIntVal& len);
  static StringVal Substring(FunctionContext*, const StringVal& str, const BigIntVal& pos);
  static StringVal Left(FunctionContext*, const StringVal& str, const BigIntVal& len);
  static StringVal Right(FunctionContext*, const StringVal& str, const BigIntVal& len);
  static StringVal Space(FunctionContext*, const BigIntVal& len);
  static StringVal Repeat(FunctionContext*, const StringVal& str, const BigIntVal& n);
  static StringVal Lpad(FunctionContext*, const StringVal& str, const BigIntVal& len,
                        const StringVal& pad);
  static StringVal Rpad(FunctionContext*, const StringVal& str, const BigIntVal&,
                        const StringVal& pad);
  static IntVal Length(FunctionContext*, const StringVal& str);
  static IntVal CharLength(FunctionContext*, const StringVal& str);
  static StringVal Lower(FunctionContext*, const StringVal& str);
  static StringVal Upper(FunctionContext*, const StringVal& str);
  static StringVal InitCap(FunctionContext*, const StringVal& str);
  static StringVal Reverse(FunctionContext*, const StringVal& str);
  static StringVal Translate(FunctionContext*, const StringVal& str, const StringVal& src,
                             const StringVal& dst);
  static StringVal Trim(FunctionContext*, const StringVal& str);
  static StringVal Ltrim(FunctionContext*, const StringVal& str);
  static StringVal Rtrim(FunctionContext*, const StringVal& str);
  static IntVal Ascii(FunctionContext*, const StringVal& str);
  static IntVal Instr(FunctionContext*, const StringVal& str, const StringVal& substr);
  static IntVal Locate(FunctionContext*, const StringVal& substr, const StringVal& str);
  static IntVal LocatePos(FunctionContext*, const StringVal& substr, const StringVal& str,
                          const BigIntVal& start_pos);

  static void RegexpPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static void RegexpClose(FunctionContext*, FunctionContext::FunctionStateScope);
  static StringVal RegexpExtract(FunctionContext*, const StringVal& str,
                                 const StringVal& pattern, const BigIntVal& index);
  static StringVal RegexpReplace(FunctionContext*, const StringVal& str,
                                 const StringVal& pattern, const StringVal& replace);

  static StringVal Concat(FunctionContext*, int num_children, const StringVal* strs);
  static StringVal ConcatWs(FunctionContext*, const StringVal& sep, int num_children,
                            const StringVal* strs);
  static IntVal FindInSet(FunctionContext*, const StringVal& str,
                          const StringVal& str_set);

  static void ParseUrlPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static StringVal ParseUrl(FunctionContext*, const StringVal& url, const StringVal& part);
  static StringVal ParseUrlKey(FunctionContext*, const StringVal& url,
                               const StringVal& key, const StringVal& part);
  static void ParseUrlClose(FunctionContext*, FunctionContext::FunctionStateScope);

  /// Converts ASCII 'val' to corresponding character.
  static StringVal Chr(FunctionContext* context, const IntVal& val);

  static void BTrimPrepare(FunctionContext*, FunctionContext::FunctionStateScope);
  static void BTrimClose(FunctionContext*, FunctionContext::FunctionStateScope);

  /// Trims occurrences of the characters in 'chars_to_trim' string from
  /// both ends of string 'str'.
  static StringVal BTrimString(FunctionContext* ctx, const StringVal& str,
    const StringVal& chars_to_trim);
};
}
#endif
