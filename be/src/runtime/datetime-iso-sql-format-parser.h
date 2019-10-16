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

#include "runtime/datetime-parser-common.h"

#include "gutil/macros.h"

namespace impala {

namespace datetime_parse_util {

/// This class is responsible for parsing an input string into a timestamp using
/// ISO:SQL:2016 datetime format patterns. The list of tokens for the parsing comes as a
/// constructor parameter in 'dt_ctx' pre-processed by IsoSqlFomratTokenizer.
/// For more details of the parsing logic please see the design document attached to
/// IMPALA-4018.
class IsoSqlFormatParser {
public:
  /// Given a list of format tokens in 'dt_ctx' runs through 'input_str' and parses it
  /// into 'result'. Return value indicates if the parsing was successful.
  /// The caller has to make sure that 'dt_ctx.fmt' is a null-terminated string.
  static bool ParseDateTime(const char* input_str, int input_len,
      const DateTimeFormatContext& dt_ctx, DateTimeParseResult* result)
      WARN_UNUSED_RESULT;

private:
  /// 'input_str' points to a location in the input string where the parsing stands now.
  /// Given 'tok' as the next token in the list of tokens created by the tokenizer this
  /// function finds the end of the next token.
  /// 'input_len' is used for stopping when we reach the end of the input string.
  /// Returns a pointer pointing one position after the last character of the found token.
  /// If can't identify the next token then returns nullptr.
  /// If a MONTH_NAME token is not followed by a separator then the end of the month name
  /// in the input can't be found by this function. In this case MAX_MONTH_NAME_LENGTH is
  /// expected as the lenght of the month token and later on ParseMonthNameToken() will
  /// adjust the end of the token.
  static const char* FindEndOfToken(const char* input_str, int input_len,
      const DateTimeFormatToken& tok, bool fx_provided) WARN_UNUSED_RESULT;

  /// Has to call this function when 'input_str' points to the fist character of a
  /// meridiem indicator. Identifies the last position of a meridiem indicator and returns
  /// a pointer to the next position after this. If no meridiem indicator is found
  /// starting from 'input_str' then returns nullptr.
  static const char* ParseMeridiemIndicatorFromInput(const char* input_str,
      int input_len);

  /// If the year part of the input is shorter than 4 digits then prefixes the year with
  /// digits from the current year. Puts the result into 'result->year'.
  static void PrefixYearFromCurrentYear(int actual_token_len, const TimestampValue* now,
      DateTimeParseResult* result);

  /// Uses 'result->year' as an input. Can call this function for 2-digit years and it
  /// constructs a 4-digit year based on the year provided and the current date. Puts the
  /// result back to 'result->year'.
  static void GetRoundYear(const TimestampValue* now, DateTimeParseResult* result);

  /// Gets a pointer to the current char in the input string and an index to the current
  /// token being processed within 'dt_ctx->toks'. Advances these pointers to the end of
  /// the current separator sequence no matter how long these sequences are. It's expected
  /// that '**tok' is of type SEPARATOR. Returns false if '**current_pos' is not a
  /// separator or if either the input ends while having remaining items in 'dt_ctx->toks'
  /// or the other way around.
  static bool ProcessSeparatorSequence(const char** current_pos, const char* end_pos,
      const DateTimeFormatContext& dt_ctx, int* dt_ctx_it);

  // Gets the next character starting from '*format' that can be used for input
  // matching. Takes care of the escaping backslashes regardless if the text token inside
  // the format is itself double escped or not. Returns the next character in a form
  // expected in the input. If '*format' points at the beginning of an escape sequence,
  // '*format' is moved to the last character of the escape sequence. Otherwise,
  // '*format' is not changed. E.g. If the text token is "\"abc" then this returns '"'
  // after skipping the backslash and moves '*format' to '"'.
  static char GetNextCharFromTextToken(const char** format,
      const DateTimeFormatToken* tok);
};

}
}
