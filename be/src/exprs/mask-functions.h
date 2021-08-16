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

#pragma once

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::IntVal;
using impala_udf::BooleanVal;
using impala_udf::BigIntVal;
using impala_udf::DateVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;

/// Data mask functions for Ranger column masking policies. In Hive, there're 6 builtin
/// GenericUDFs for column masking:
///   mask_show_first_n(value, charCount, upperChar, lowerChar, digitChar, otherChar,
///       numberChar)
///   mask_show_last_n(value, charCount, upperChar, lowerChar, digitChar, otherChar,
///       numberChar)
///   mask_first_n(value, charCount, upperChar, lowerChar, digitChar, otherChar,
///       numberChar)
///   mask_last_n(value, charCount, upperChar, lowerChar, digitChar, otherChar,
///       numberChar)
///   mask_hash(value)
///   mask(value, upperChar, lowerChar, digitChar, otherChar, numberChar, dayValue,
///       monthValue, yearValue)
///
/// Description of the parameters:
///   value      - value to mask. Supported types: TINYINT, SMALLINT, INT, BIGINT,
///                STRING, VARCHAR, CHAR, DATE(only for mask()).
///   charCount  - number of characters. Default value: 4
///   upperChar  - character to replace upper-case characters with. Specify -1 to retain
///                original character. Default value: 'X'
///   lowerChar  - character to replace lower-case characters with. Specify -1 to retain
///                original character. Default value: 'x'
///   digitChar  - character to replace digit characters with. Specify -1 to retain
///                original character. Default value: 'n'
///   otherChar  - character to replace all other characters with. Specify -1 to retain
///                original character. Default value: -1
///   numberChar - character to replace digits in a number with. Valid values: 0-9.
///                Default value: '1'
///   dayValue   - value to replace day field in a date with. The day must be valid for
///                the year and month, otherwise the results will be NULL. Specify -1 to
///                retain original value. Valid values: 1-31. Default value: 1
///   monthValue - value to replace month field in a date with. Specify -1 to retain
///                original value. Valid values: 0-11. Default value: 0
///   yearValue  - value to replace year field in a date with. Specify -1 to retain
///                original value. Default value: 1
///
/// In Hive, these functions accept variable length of arguments in non-restricted types:
///   mask_show_first_n(val)
///   mask_show_first_n(val, 8)
///   mask_show_first_n(val, 8, 'X', 'x', 'n')
///   mask_show_first_n(val, 8, 'x', 'x', 'x', 'x', -1)
///   mask_show_first_n(val, 8, 'x', -1, 'x', 'x', '9')
/// The arguments of upperChar, lowerChar, digitChar, otherChar and numberChar can be in
/// string or numeric types.
///
/// We currently don't have a corresponding framework for GenericUDF(IMPALA-9271), so we
/// implement these by overloads. However, it may requires hundreds of overloads to cover
/// all possible combinations. We just implement some important overloads, including
///   * those used by Ranger default masking policies,
///   * those with simple arguments and may be useful for users,
///   * an overload with all arguments in int type for full functionality. Char argument
///     need to be converted to their ASCII value.
class MaskFunctions {
 public:
  /// Declarations of mask_show_first_n()
  /// Overloads for masking a string value
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  // The default transformer of MASK_SHOW_FIRST_4 mask type is
  //   mask_show_first_n({col}, 4, 'x', 'x', 'x', -1, '1')
  // So we need this overload.
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  // Overload for only masking other chars. So we can support patterns like
  //   mask_show_first_n({col}, 4, -1, -1, -1, 'x')
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const StringVal& other_char);
  // Overload that all masked chars are given as integers.
  static StringVal MaskShowFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static BigIntVal MaskShowFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overload used by the default MASK_SHOW_FIRST_4 policy on date type
  static DateVal MaskShowFirstN(FunctionContext* ctx, const DateVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);

  /// Declarations of mask_show_last_n()
  /// Overloads for masking a string value
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  // The default transformer of MASK_SHOW_LAST_4 mask type is
  //   mask_show_last_n({col}, 4, 'x', 'x', 'x', -1, '1')
  // So we need this overload.
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  // Overload for only masking other chars. So we can support patterns like
  //   mask_show_last_n({col}, 4, -1, -1, -1, 'x')
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const StringVal& other_char);
  // Overload that all masked chars are given as integers.
  static StringVal MaskShowLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static BigIntVal MaskShowLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Declarations of mask_first_n()
  /// Overloads for masking a string value
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  // The default transformer of MASK_FIRST_4 mask type is
  //   mask_first_n({col}, 4, 'x', 'x', 'x', -1, '1')
  // So we need this overload.
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  // Overload for only masking other chars. So we can support patterns like
  //   mask_first_n({col}, 4, -1, -1, -1, 'x')
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const StringVal& other_char);
  // Overload that all masked chars are given as integers.
  static StringVal MaskFirstN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static BigIntVal MaskFirstN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Declarations of mask_first_n()
  /// Overloads for masking a string value
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  // The default transformer of MASK_SHOW_LAST_4 mask type is
  //   mask_show_last_n({col}, 4, 'x', 'x', 'x', -1, '1')
  // So we need this overload.
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  // Overload for only masking other chars. So we can support patterns like
  //   mask_first_n({col}, 4, -1, -1, -1, 'x')
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const StringVal& other_char);
  // Overload that all masked chars are given as integers.
  static StringVal MaskLastN(FunctionContext* ctx, const StringVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);
  /// Overloads for masking a numeric value
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static BigIntVal MaskLastN(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& char_count, const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const IntVal& other_char, const IntVal& number_char);

  /// Declarations of mask()
  /// Overloads for masking a string value
  static StringVal Mask(FunctionContext* ctx, const StringVal& val);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char);
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  // The default transformer of MASK_DATE_SHOW_YEAR mask type is
  //   mask({col}, 'x', 'x', 'x', -1, '1', 1, 0, -1)
  // So we need this overload.
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  // Overload for only masking other chars. So we can support patterns like
  //   mask({col}, -1, -1, -1, 'x')
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const IntVal& upper_char, const IntVal& lower_char,
      const IntVal& digit_char, const StringVal& other_char);
  // Overload that all masked chars are given as integers.
  static StringVal Mask(FunctionContext* ctx, const StringVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
      const IntVal& month_value, const IntVal& year_value);
  /// Overloads for masking a date value
  static DateVal Mask(FunctionContext* ctx, const DateVal& val);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value);
  // The default transformer of MASK_DATE_SHOW_YEAR mask type is
  //   mask({col}, 'x', 'x', 'x', -1, '1', 1, 0, -1)
  // So we need this overload.
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static DateVal Mask(FunctionContext* ctx, const DateVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
      const IntVal& month_value, const IntVal& year_value);
  /// Overloads for masking a numeric value
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const StringVal& other_char,
      const IntVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  // The default transformer of MASK_DATE_SHOW_YEAR mask type is
  //   mask({col}, 'x', 'x', 'x', -1, '1', 1, 0, -1)
  // So we need this overload.
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static BigIntVal Mask(FunctionContext* ctx, const BigIntVal& val,
      const IntVal& upper_char, const IntVal& lower_char, const IntVal& digit_char,
      const IntVal& other_char, const IntVal& number_char, const IntVal& day_value,
      const IntVal& month_value, const IntVal& year_value);
  /// Other overloads
  static BooleanVal Mask(FunctionContext* ctx, const BooleanVal& val);
  static BooleanVal Mask(FunctionContext* ctx, const BooleanVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static DoubleVal Mask(FunctionContext* ctx, const DoubleVal& val);
  static DoubleVal Mask(FunctionContext* ctx, const DoubleVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);
  static TimestampVal Mask(FunctionContext* ctx, const TimestampVal& val);
  static TimestampVal Mask(FunctionContext* ctx, const TimestampVal& val,
      const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char, const IntVal& day_value, const IntVal& month_value,
      const IntVal& year_value);

  /// Declarations of mask_hash()
  static StringVal MaskHash(FunctionContext* ctx, const StringVal& val);
  static BigIntVal MaskHash(FunctionContext* ctx, const BigIntVal& val);
  static DoubleVal MaskHash(FunctionContext* ctx, const DoubleVal& val);
  static BooleanVal MaskHash(FunctionContext* ctx, const BooleanVal& val);
  static TimestampVal MaskHash(FunctionContext* ctx, const TimestampVal& val);
  static DateVal MaskHash(FunctionContext* ctx, const DateVal& val);

  /// Nullify overloads
  static BooleanVal MaskNull(FunctionContext* ctx, const BooleanVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static DoubleVal MaskNull(FunctionContext* ctx, const DoubleVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
  static TimestampVal MaskNull(FunctionContext* ctx, const TimestampVal& val,
      const IntVal& char_count, const StringVal& upper_char, const StringVal& lower_char,
      const StringVal& digit_char, const IntVal& other_char,
      const StringVal& number_char);
};

} // namespace impala
