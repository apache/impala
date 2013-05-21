#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This is a list of all the functions that are not auto-generated.
# It contains all the meta data that describes the function.  The format is:
# <function name>, <return_type>, [<args>], <backend function name>, [<sql aliases>]
#
# 'function name' is the base of what the opcode enum will be generated from.  It does not
# have to be unique, the script will mangle the name with the signature if necessary.
#
# 'sql aliases' are the function names that can be used from sql.  They are
# optional and there can be multiple aliases for a function.
#
# This is combined with the list in generated_functions to code-gen the opcode
# registry in the FE and BE.

functions = [
  ['Compound_And', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::AndComputeFn', []],
  ['Compound_Or', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::OrComputeFn', []],
  ['Compound_Not', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::NotComputeFn', []],

  ['Constant_Regex', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'LikePredicate::ConstantRegexFn', []],
  ['Constant_Substring', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'LikePredicate::ConstantSubstringFn', []],
  ['Like', 'BOOLEAN', ['STRING', 'STRING'], 'LikePredicate::LikeFn', []],
  ['Regex', 'BOOLEAN', ['STRING', 'STRING'], 'LikePredicate::RegexFn', []],

  ['Math_Pi', 'DOUBLE', [], 'MathFunctions::Pi', ['pi']],
  ['Math_E', 'DOUBLE', [], 'MathFunctions::E', ['e']],
  ['Math_Abs', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Abs', ['abs']],
  ['Math_Sign', 'FLOAT', ['DOUBLE'], 'MathFunctions::Sign', ['sign']],
  ['Math_Sin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Sin', ['sin']],
  ['Math_Asin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Asin', ['asin']],
  ['Math_Cos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Cos', ['cos']],
  ['Math_Acos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Acos', ['acos']],
  ['Math_Tan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Tan', ['tan']],
  ['Math_Atan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Atan', ['atan']],
  ['Math_Radians', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Radians', ['radians']],
  ['Math_Degrees', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Degrees', ['degrees']],
  ['Math_Ceil', 'BIGINT', ['DOUBLE'], 'MathFunctions::Ceil', ['ceil', 'ceiling']],
  ['Math_Floor', 'BIGINT', ['DOUBLE'], 'MathFunctions::Floor', ['floor']],
  ['Math_Round', 'BIGINT', ['DOUBLE'], 'MathFunctions::Round', ['round']],
  ['Math_Round', 'DOUBLE', ['DOUBLE', 'INT'], 'MathFunctions::RoundUpTo', ['round']],
  ['Math_Exp', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Exp', ['exp']],
  ['Math_Ln', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Ln', ['ln']],
  ['Math_Log10', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Log10', ['log10']],
  ['Math_Log2', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Log2', ['log2']],
  ['Math_Log', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::Log', ['log']],
  ['Math_Pow', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::Pow', ['pow', 'power']],
  ['Math_Sqrt', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Sqrt', ['sqrt']],
  ['Math_Rand', 'DOUBLE', [], 'MathFunctions::Rand', ['rand']],
  ['Math_Rand', 'DOUBLE', ['INT'], 'MathFunctions::RandSeed', ['rand']],
  ['Math_Bin', 'STRING', ['BIGINT'], 'MathFunctions::Bin', ['bin']],
  ['Math_Hex', 'STRING', ['BIGINT'], 'MathFunctions::HexInt', ['hex']],
  ['Math_Hex', 'STRING', ['STRING'], 'MathFunctions::HexString', ['hex']],
  ['Math_Unhex', 'STRING', ['STRING'], 'MathFunctions::Unhex', ['unhex']],
  ['Math_Conv', 'STRING', ['BIGINT', 'TINYINT', 'TINYINT'], \
        'MathFunctions::ConvInt', ['conv']],
  ['Math_Conv', 'STRING', ['STRING', 'TINYINT', 'TINYINT'], \
        'MathFunctions::ConvString', ['conv']],
  ['Math_Pmod', 'BIGINT', ['BIGINT', 'BIGINT'], 'MathFunctions::PmodBigInt', ['pmod']],
  ['Math_Pmod', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::PmodDouble', ['pmod']],
  ['Math_Positive', 'BIGINT', ['BIGINT'], 'MathFunctions::PositiveBigInt', ['positive']],
  ['Math_Positive', 'DOUBLE', ['DOUBLE'], 'MathFunctions::PositiveDouble', ['positive']],
  ['Math_Negative', 'BIGINT', ['BIGINT'], 'MathFunctions::NegativeBigInt', ['negative']],
  ['Math_Negative', 'DOUBLE', ['DOUBLE'], 'MathFunctions::NegativeDouble', ['negative']],

  ['String_Substring', 'STRING', ['STRING', 'INT'], \
        'StringFunctions::Substring<int32_t>', ['substr', 'substring']], 
  ['String_Substring', 'STRING', ['STRING', 'BIGINT'], \
        'StringFunctions::Substring<int64_t>', ['substr', 'substring']],
  ['String_Substring', 'STRING', ['STRING', 'INT', 'INT'], \
        'StringFunctions::Substring<int32_t>', ['substr', 'substring']],
  ['String_Substring', 'STRING', ['STRING', 'BIGINT', 'BIGINT'], \
        'StringFunctions::Substring<int64_t>', ['substr', 'substring']],
# left and right are key words, leave them out for now.
  ['String_Left', 'STRING', ['STRING', 'INT'],
        'StringFunctions::Left<int32_t>', ['strleft']],
  ['String_Left', 'STRING', ['STRING', 'BIGINT'],
        'StringFunctions::Left<int64_t>', ['strleft']],
  ['String_Right', 'STRING', ['STRING', 'INT'],
        'StringFunctions::Right<int32_t>', ['strright']],
  ['String_Right', 'STRING', ['STRING', 'BIGINT'],
        'StringFunctions::Right<int64_t>', ['strright']],
  ['String_Space', 'STRING', ['INT'], 'StringFunctions::Space<int32_t>', ['space']],
  ['String_Space', 'STRING', ['BIGINT'], 'StringFunctions::Space<int32_t>', ['space']],
  ['String_Repeat', 'STRING', ['STRING', 'INT'],
        'StringFunctions::Repeat<int32_t>', ['repeat']],
  ['String_Repeat', 'STRING', ['STRING', 'BIGINT'],
        'StringFunctions::Repeat<int64_t>', ['repeat']],
  ['String_Lpad', 'STRING', ['STRING', 'INT', 'STRING'], \
        'StringFunctions::Lpad<int32_t>', ['lpad']],
  ['String_Lpad', 'STRING', ['STRING', 'BIGINT', 'STRING'], \
        'StringFunctions::Lpad<int64_t>', ['lpad']],
  ['String_Rpad', 'STRING', ['STRING', 'INT', 'STRING'], \
        'StringFunctions::Rpad<int32_t>', ['rpad']],
  ['String_Rpad', 'STRING', ['STRING', 'BIGINT', 'STRING'], \
        'StringFunctions::Rpad<int64_t>', ['rpad']],
  ['String_Length', 'INT', ['STRING'], 'StringFunctions::Length', ['length']],
  ['String_Lower', 'STRING', ['STRING'], 'StringFunctions::Lower', ['lower', 'lcase']],
  ['String_Upper', 'STRING', ['STRING'], 'StringFunctions::Upper', ['upper', 'ucase']],
  ['String_Reverse', 'STRING', ['STRING'], 'StringFunctions::Reverse', ['reverse']],
  ['String_Trim', 'STRING', ['STRING'], 'StringFunctions::Trim', ['trim']],
  ['String_Ltrim', 'STRING', ['STRING'], 'StringFunctions::Ltrim', ['ltrim']],
  ['String_Rtrim', 'STRING', ['STRING'], 'StringFunctions::Rtrim', ['rtrim']],
  ['String_Ascii', 'INT', ['STRING'], 'StringFunctions::Ascii', ['ascii']], 
  ['String_Instr', 'INT', ['STRING', 'STRING'], 'StringFunctions::Instr', ['instr']],
  ['String_Locate', 'INT', ['STRING', 'STRING'], 'StringFunctions::Locate', ['locate']],
  ['String_Locate', 'INT', ['STRING', 'STRING', 'INT'], \
        'StringFunctions::LocatePos<int32_t>', ['locate']],
  ['String_Locate', 'INT', ['STRING', 'STRING', 'BIGINT'], \
        'StringFunctions::LocatePos<int64_t>', ['locate']],
  ['String_Regexp_Extract', 'STRING', ['STRING', 'STRING', 'INT'], \
        'StringFunctions::RegexpExtract<int32_t>', ['regexp_extract']],
  ['String_Regexp_Extract', 'STRING', ['STRING', 'STRING', 'BIGINT'], \
        'StringFunctions::RegexpExtract<int64_t>', ['regexp_extract']],
  ['String_Regexp_Replace', 'STRING', ['STRING', 'STRING', 'STRING'], \
        'StringFunctions::RegexpReplace', ['regexp_replace']],
  ['String_Concat', 'STRING', ['STRING', '...'], 'StringFunctions::Concat', ['concat']],
  ['String_Concat_Ws', 'STRING', ['STRING', 'STRING', '...'], \
        'StringFunctions::ConcatWs', ['concat_ws']],
  ['String_Find_In_Set', 'INT', ['STRING', 'STRING'], \
        'StringFunctions::FindInSet', ['find_in_set']],
  ['String_Parse_Url', 'STRING', ['STRING', 'STRING'], \
        'StringFunctions::ParseUrl', ['parse_url']],
  ['String_Parse_Url', 'STRING', ['STRING', 'STRING', 'STRING'], \
        'StringFunctions::ParseUrlKey', ['parse_url']],
  ['Utility_Sleep', 'BOOLEAN', ['INT'], 'UtilityFunctions::Sleep', ['sleep']],
  ['Utility_Version', 'STRING', [], 'UtilityFunctions::Version', ['version']],

# Timestamp Functions
  ['Unix_Timestamp', 'INT', [], \
        'TimestampFunctions::Unix', ['unix_timestamp']],
  ['Unix_Timestamp', 'INT', ['TIMESTAMP'], \
        'TimestampFunctions::Unix', ['unix_timestamp']],
  ['Unix_Timestamp', 'INT', ['STRING', 'STRING'], \
        'TimestampFunctions::Unix', ['unix_timestamp']],
  ['From_UnixTime', 'STRING', ['INT'], \
        'TimestampFunctions::FromUnix', ['from_unixtime']],
  ['From_UnixTime', 'STRING', ['INT', 'STRING'], \
        'TimestampFunctions::FromUnix', ['from_unixtime']],
  ['Timestamp_year', 'INT', ['TIMESTAMP'], 'TimestampFunctions::Year', ['year']],
  ['Timestamp_month', 'INT', ['TIMESTAMP'], 'TimestampFunctions::Month', ['month']],
  ['Timestamp_dayofmonth', 'INT', ['TIMESTAMP'], \
        'TimestampFunctions::DayOfMonth', ['day', 'dayofmonth']],
  ['Timestamp_dayofyear', 'INT', ['TIMESTAMP'],
        'TimestampFunctions::DayOfYear', ['dayofyear']],
  ['Timestamp_weekofyear', 'INT', ['TIMESTAMP'], \
        'TimestampFunctions::WeekOfYear', ['weekofyear']],
  ['Timestamp_hour', 'INT', ['TIMESTAMP'], 'TimestampFunctions::Hour', ['hour']],
  ['Timestamp_minute', 'INT', ['TIMESTAMP'], 'TimestampFunctions::Minute', ['minute']],
  ['Timestamp_second', 'INT', ['TIMESTAMP'], 'TimestampFunctions::Second', ['second']],
  ['Timestamp_now', 'TIMESTAMP', [], \
        'TimestampFunctions::Now', ['now', 'current_timestamp']],
  ['Timestamp_to_date', 'STRING', ['TIMESTAMP'], \
        'TimestampFunctions::ToDate', ['to_date']],
  ['Timestamp_diff', 'INT', ['TIMESTAMP', 'TIMESTAMP'], \
        'TimestampFunctions::DateDiff', ['datediff']],
  ['From_utc_timestamp', 'TIMESTAMP', ['TIMESTAMP', 'STRING'], \
        'TimestampFunctions::FromUtc', ['from_utc_timestamp']],
  ['To_utc_timestamp', 'TIMESTAMP', ['TIMESTAMP', 'STRING'], \
        'TimestampFunctions::ToUtc', ['to_utc_timestamp']],

# Date and time add/sub functions.
  ['Timestamp_years_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<true, int32_t, years>', ['years_add']],
  ['Timestamp_years_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<true, int64_t, years>', ['years_add']],
  ['Timestamp_years_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<false, int32_t, years>', ['years_sub']],
  ['Timestamp_years_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<false, int64_t, years>', ['years_sub']],
  ['Timestamp_months_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<true, int32_t, months>', ['months_add']],
  ['Timestamp_months_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<true, int64_t, months>', ['months_add']],
  ['Timestamp_months_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<false, int32_t, months>', ['months_sub']],
  ['Timestamp_months_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<false, int64_t, months>', ['months_sub']],
  ['Timestamp_weeks_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<true, int32_t, weeks>', ['weeks_add']],
  ['Timestamp_weeks_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<true, int64_t, weeks>', ['weeks_add']],
  ['Timestamp_weeks_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<false, int32_t, weeks>', ['weeks_sub']],
  ['Timestamp_weeks_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<false, int64_t, weeks>', ['weeks_sub']],
  ['Timestamp_days_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<true, int32_t, days>',\
        ['days_add', 'date_add', 'adddate']],
  ['Timestamp_days_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<true, int64_t, days>',\
        ['days_add', 'date_add', 'adddate']],
  ['Timestamp_days_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::DateAddSub<false, int32_t, days>',\
        ['days_sub', 'date_sub', 'subdate']],
  ['Timestamp_days_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::DateAddSub<false, int64_t, days>',\
        ['days_sub', 'date_sub', 'subdate']],
  ['Timestamp_hours_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, hours>', ['hours_add']],
  ['Timestamp_hours_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, hours>', ['hours_add']],
  ['Timestamp_hours_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, hours>', ['hours_sub']],
  ['Timestamp_hours_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, hours>', ['hours_sub']],
  ['Timestamp_minutes_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, minutes>', ['minutes_add']],
  ['Timestamp_minutes_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, minutes>', ['minutes_add']],
  ['Timestamp_minutes_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, minutes>', ['minutes_sub']],
  ['Timestamp_minutes_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, minutes>', ['minutes_sub']],
  ['Timestamp_seconds_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, seconds>', ['seconds_add']],
  ['Timestamp_seconds_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, seconds>', ['seconds_add']],
  ['Timestamp_seconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, seconds>', ['seconds_sub']],
  ['Timestamp_seconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, seconds>', ['seconds_sub']],
  ['Timestamp_milliseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, milliseconds>',\
        ['milliseconds_add']],
  ['Timestamp_milliseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, milliseconds>',\
        ['milliseconds_add']],
  ['Timestamp_milliseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, milliseconds>',\
        ['milliseconds_sub']],
  ['Timestamp_milliseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, milliseconds>',\
        ['milliseconds_sub']],
  ['Timestamp_microseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, microseconds>',\
        ['microseconds_add']],
  ['Timestamp_microseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, microseconds>',\
        ['microseconds_add']],
  ['Timestamp_microseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, microseconds>',\
        ['microseconds_sub']],
  ['Timestamp_microseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, microseconds>',\
        ['microseconds_sub']],
  ['Timestamp_nanoseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<true, int32_t, nanoseconds>',\
        ['nanoseconds_add']],
  ['Timestamp_nanoseconds_add', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<true, int64_t, nanoseconds>',\
        ['nanoseconds_add']],
  ['Timestamp_nanoseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'INT'], \
        'TimestampFunctions::TimeAddSub<false, int32_t, nanoseconds>',\
        ['nanoseconds_sub']],
  ['Timestamp_nanoseconds_sub', 'TIMESTAMP', ['TIMESTAMP', 'BIGINT'], \
        'TimestampFunctions::TimeAddSub<false, int64_t, nanoseconds>',\
        ['nanoseconds_sub']],

# Conditional Functions
  ['Conditional_If', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], \
        'ConditionalFunctions::IfBool', ['if']],
  ['Conditional_If', 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], \
        'ConditionalFunctions::IfTinyint', ['if']],
  ['Conditional_If', 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], \
        'ConditionalFunctions::IfSmallint', ['if']],
  ['Conditional_If', 'INT', ['BOOLEAN', 'INT', 'INT'], \
        'ConditionalFunctions::IfInt', ['if']],
  ['Conditional_If', 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], \
        'ConditionalFunctions::IfBigint', ['if']],
  ['Conditional_If', 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], \
        'ConditionalFunctions::IfFloat', ['if']],
  ['Conditional_If', 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], \
        'ConditionalFunctions::IfDouble', ['if']],
  ['Conditional_If', 'STRING', ['BOOLEAN', 'STRING', 'STRING'], \
        'ConditionalFunctions::IfString', ['if']],
  ['Conditional_If', 'TIMESTAMP', ['BOOLEAN', 'TIMESTAMP', 'TIMESTAMP'], \
        'ConditionalFunctions::IfTimestamp', ['if']],

  ['Conditional_IsNull', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  ['Conditional_IsNull', 'INT', ['INT', 'INT'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  ['Conditional_IsNull', 'BIGINT', ['BIGINT', 'BIGINT'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  ['Conditional_IsNull', 'DOUBLE', ['DOUBLE', 'DOUBLE'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  ['Conditional_IsNull', 'STRING', ['STRING', 'STRING'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  ['Conditional_IsNull', 'TIMESTAMP', ['TIMESTAMP', 'TIMESTAMP'], \
        'ConditionalFunctions::IsNull', ['isnull']],
  
  ['Conditional_Coalesce', 'BOOLEAN', ['BOOLEAN', '...'], \
        'ConditionalFunctions::CoalesceBool', ['coalesce']],
  ['Conditional_Coalesce', 'BIGINT', ['BIGINT', '...'], \
        'ConditionalFunctions::CoalesceInt', ['coalesce']],
  ['Conditional_Coalesce', 'DOUBLE', ['DOUBLE', '...'], \
        'ConditionalFunctions::CoalesceFloat', ['coalesce']],
  ['Conditional_Coalesce', 'STRING', ['STRING', '...'], \
        'ConditionalFunctions::CoalesceString', ['coalesce']],
  ['Conditional_Coalesce', 'TIMESTAMP', ['TIMESTAMP', '...'], \
        'ConditionalFunctions::CoalesceTimestamp', ['coalesce']],
]
