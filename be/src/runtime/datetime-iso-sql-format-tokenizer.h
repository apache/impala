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

#include <string>
#include <unordered_set>
#include <unordered_map>

#include "gutil/macros.h"

namespace impala {

namespace datetime_parse_util {

/// This class is responsible for splitting a datetime format string into tokens following
/// the ISO:SQL:2016 standard. For acceptable tokens please see the design document
/// attached to IMPALA-4018. You can also get a good impression of the available tokens by
/// checking VALID_TOKENS member and IsSeparator() function of this class.
class IsoSqlFormatTokenizer {
public:
  IsoSqlFormatTokenizer(DateTimeFormatContext* dt_ctx, CastDirection cast_mode,
      bool time_toks) : dt_ctx_(dt_ctx), cast_mode_(cast_mode),
      accept_time_toks_(time_toks), fm_modifier_active_(false) {}

  void Reset(DateTimeFormatContext* dt_ctx, CastDirection cast_mode, bool time_toks) {
    dt_ctx_ = dt_ctx;
    cast_mode_ = cast_mode;
    accept_time_toks_ = time_toks;
    used_tokens_.clear();
  }

  /// Performs parsing of 'dt_ctx_.fmt' format string. During the process this populates
  /// 'dt_ctx_' with metadata about the format string such as tokens found in the string.
  FormatTokenizationResult Tokenize();

  /// Returns true if '*current_pos' points to a valid separator. If
  /// 'read_escaped_single_quotes' is true then escaped single quotes are also taken as
  /// valid separators. In this case '*current_pos' is advanced from the escaping
  /// backslash to the single quote.
  static bool IsSeparator(const char** current_pos, const char* str_end,
      bool read_escaped_single_quotes = true);
private:
  /// Stores metadata about a specific token type.
  struct TokenItem {
    TokenItem(DateTimeFormatTokenType t, bool dt, bool tt) :
      type(t), date_token(dt), time_token(tt) {}

    DateTimeFormatTokenType type;
    bool date_token;
    bool time_token;
  };

  /// This holds all the valid tokens accepted by this parser. When a format string is
  /// being parsed the substrings are checked against this member to decide if they are
  /// valid tokens or not.
  /// Note, token matching is case-insensitive even though this member contains
  /// upper-case names only.
  static const std::unordered_map<std::string, TokenItem> VALID_TOKENS;

  /// Keeps the maximum token lengths for tokens where the token length doesn't equal to
  /// the length of the datetime pattern. E.g. "HH12" is 4 chars long, however expects
  /// maximum 2 digits as "10".
  static const std::unordered_map<std::string, int> SPECIAL_LENGTHS;

  /// This has to be in line with the longest string in VALID_TOKENS;
  static const unsigned MAX_TOKEN_SIZE;

  /// To be on the safe side this introduces a max length limit for the input format
  /// strings.
  static const int MAX_FORMAT_LENGTH;

  /// Maximum length of fractional second digits.
  static const int FRACTIONAL_MAX_LEN;

  /// When parsing is in progress this contains the format tokens that we have found in
  /// the input format string so far.
  std::unordered_set<std::string> used_tokens_;

  /// The context that is used for the input of the parsing. It is also populated during
  /// the parsing process. Not owned by this class.
  /// Note, that 'dt_ctx_->fmt' is a null-terminated string so it's safe to use string
  /// functions that make this assumption.
  DateTimeFormatContext* dt_ctx_;

  /// Decides whether this is a 'datetime to string' or a 'string to datetime' cast.
  CastDirection cast_mode_;

  bool accept_time_toks_;

  /// True when the FM modifier has to be applied to the following non-separator token.
  /// It is set back to false once applied on a token.
  bool fm_modifier_active_;

  /// Iterates through all the consecutive separator characters from a given pointer
  /// 'current' and saves them to 'dt_ctx_'.
  void ProcessSeparators(const char** current);

  /// Identifies the next token using VALID_TOKENS and saves it to 'dt_ctx_'. Finds the
  /// longest possible match.
  FormatTokenizationResult ProcessNextToken(const char** current_pos);

  /// Checks if the token has special length in 'SPECIAL_LENGTHS'. If finds a special
  /// length then returns that, otherwise returns the length of 'token'.
  int GetMaxTokenLength(const std::string& token) const;

  /// Checks if 'token' is present in 'used_tokens_';
  bool IsUsedToken(const std::string& token) const;

  /// Checks if any of the meridiem indicators are present in 'used_tokens_'.
  bool IsMeridiemIndicatorProvided() const;

  /// Checks if the end product of the parsing contains format tokens that collide with
  /// each other like YYYY and RR.
  FormatTokenizationResult CheckIncompatibilities() const;

  /// Checks if '*current_pos' points to an FX modifier and advances '*current_pos' after
  /// the FX modifier. Sets 'dt_ctx_->fx_modifier' to true if '*current_pos' points to an
  /// FX modifier. Call this when '*current_pos' points to the first character of the
  /// format string.
  void ProcessFXModifier(const char** current_pos);

  /// Returns true if 'current_pos' points to either a double quote or to a backslash
  /// that is followed by a double quote.
  static bool IsStartOfTextToken(const char* current_pos);

  /// Finds the end of the text token and saves metadata about the token into 'dt_ctx_'.
  /// This has to be called when '*current_pos' points to the beginning of a text token
  /// or in other words if IsStartOfTextToken(*current_pos) is true. As a side effect
  /// 'current_pos' is advanced right after the closing double qoute of the text token.
  FormatTokenizationResult ProcessTextToken(const char** current_pos,
      const char* str_begin, const char* str_end);

  // Starting from 'str_start' finds the closing quotation mark of the text token even
  // if it's escaped. 'str_start' has to point to the first character of the text token
  // right after the opening quote. Returns a pointer pointing right after the closing
  // double quote of the text token or nullptr if the text token is unclosed.
  static const char* FindEndOfTextToken(const char* str_start, const char* str_end,
      bool is_escaped);
};

}
}
