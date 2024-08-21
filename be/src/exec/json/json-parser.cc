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

#include "exec/json/json-parser.h"

#include "exec/json/hdfs-json-scanner.h"
#include "gutil/strings/ascii_ctype.h"

using namespace impala;
using namespace rapidjson;

using std::vector;
using std::string;

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

#define ERROR_IF_FALSE(x, err) \
  do { \
    if (UNLIKELY(!(x))) { code_ = err; return false; } \
  } while (false)

template <class Scanner>
JsonParser<Scanner>::JsonParser(const vector<string>& schema, Scanner* scanner)
  : num_fields_(schema.size()), scanner_(scanner), stream_(this) {
  field_found_.resize(num_fields_);
  int index = 0;
  for (const string& name : schema) {
    string lower_case_key(name.size(), 0);
    for (size_t i = 0; i < name.size(); ++i) {
      lower_case_key[i] = ascii_tolower(name[i]);
    }
    DCHECK(field_indexs_.find(lower_case_key) == field_indexs_.end());
    field_indexs_[lower_case_key] = index++;
  }
}

template <class Scanner>
void JsonParser<Scanner>::ResetParser() {
  row_initialized_ = false;
  array_depth_ = 0;
  object_depth_ = 0;
  current_field_idx_ = -1;
  memset(field_found_.data(), false, field_found_.size());
}

template <class Scanner>
bool JsonParser<Scanner>::IsTidy() {
  // Check if there are no unclosed arrays or objects.
  bool no_unclosed_elements = array_depth_ == 0 && object_depth_ == 0;

  // Check if there are no field or row in handling.
  bool no_handling_field_or_row = current_field_idx_ == -1 && !row_initialized_;

  // Check if there are no any fields found, i.e. all false in 'field_found_'.
  bool no_fields_found = field_found_.size() == 0 ||
      (field_found_[0] == false &&
      !memcmp(field_found_.data(), field_found_.data() + 1, field_found_.size() - 1));

  // Return true if all conditions are met, indicating that the parser is tidy
  return no_unclosed_elements && no_handling_field_or_row && no_fields_found;
}

template <class Scanner>
bool JsonParser<Scanner>::MoveToNextJson() {
  while (!stream_.Eos()) {
    if (stream_.Take() == '\n') {
      return true;
    }
  }
  return false;
}

template <class Scanner>
Status JsonParser<Scanner>::Parse(int max_rows, int* num_rows) {
  while (*num_rows < max_rows) {
    constexpr auto parse_flags =
        kParseNumbersAsStringsFlag | kParseStopWhenDoneFlag | kParseNanAndInfFlag;
    // Reads characters from the stream, parses them and publishes events to this
    // handler (JsonParser).
    reader_.Parse<parse_flags>(stream_, *this);

    if (UNLIKELY(reader_.HasParseError())) {
      if (reader_.GetParseErrorCode() == kParseErrorDocumentEmpty) {
        // When the parser encounters the first non-empty character as '\0' during once
        // parsing, it assumes the end of the stream and throws the error code
        // "kParseErrorDocumentEmpty". If the stream has indeed reached its end, we can
        // return normally. However, if a file corruption causes a '\0' to be inserted
        // between JSON objects, the stream hasn't actually ended, and we should
        // continue scanning.
        if (UNLIKELY(!stream_.Eos())) {
          DCHECK_EQ(stream_.Peek(), '\0');
          stream_.Take();
          continue;
        }
        DCHECK(IsTidy());
        return Status::OK();
      }
      // Call the scanner to handling error. If the error is successfully handled,
      // continue parsing. Since parsing have been interrupted and we may be stopped in
      // the middle of a JSON object, we need to move to the starting position of the
      // next object and reset the parser status before starting the next parse.
      // But there is a special case where the error code reported by the parser is
      // kParseErrorObjectMissCommaOrCurlyBracket, indicating that the current JSON
      // object is missing a closing curly bracket. In this case, we should already be
      // stopped at the end of this JSON object and there is no need to move to the
      // starting position of the next object. If MoveToNextJson() is still called in
      // this case, it is highly likely to cause us to miss a complete row.
      RETURN_IF_ERROR(scanner_->HandleError(reader_.GetParseErrorCode(),
          reader_.GetErrorOffset()));
      if (row_initialized_) FinishRow();
      if (reader_.GetParseErrorCode() != kParseErrorObjectMissCommaOrCurlyBracket) {
        MoveToNextJson();
      }
      ResetParser();
    }

    ++(*num_rows);
    if (UNLIKELY(scanner_->BreakParse())) break;
  }

  return Status::OK();
}

template <class Scanner>
Status JsonParser<Scanner>::CountJsonObjects(int max_rows, int* num_rows) {
  JsonSkipper<CharStream> skipper(stream_);
  while (*num_rows < max_rows) {
    skipper.SkipNextObject();

    if (UNLIKELY(skipper.HasError())) {
      if (skipper.GetErrorCode() == kParseErrorDocumentEmpty) {
        // See the comments at the corresponding location of the Parse().
        if (UNLIKELY(!stream_.Eos())) {
          DCHECK_EQ(stream_.Peek(), '\0');
          stream_.Take();
          continue;
        }
        return Status::OK();
      }
      RETURN_IF_ERROR(scanner_->HandleError(skipper.GetErrorCode(), stream_.Tell()));

      // See the comments at the corresponding location of the Parse().
      if (reader_.GetParseErrorCode() != kParseErrorObjectMissCommaOrCurlyBracket) {
        MoveToNextJson();
      }
    }

    ++(*num_rows);
    if (UNLIKELY(scanner_->BreakParse())) break;
  }

  return Status::OK();
}

template <class Scanner>
bool JsonParser<Scanner>::Key(const char* str, uint32_t len, bool copy) {
  if (object_depth_ == 1 && array_depth_ == 0) {
    DCHECK_EQ(current_field_idx_, -1);
    string lower_case_key(len, 0);
    for (uint32_t i = 0; i < len; ++i) {
      lower_case_key[i] = ascii_tolower(str[i]);
    }
    auto iter = field_indexs_.find(lower_case_key);
    current_field_idx_ = (iter == field_indexs_.end()) ? -1 : iter->second;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::StartObject() {
  ++object_depth_;
  if (object_depth_ == 1 && array_depth_ == 0) {
    scanner_->InitRow();
    row_initialized_ = true;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::EndObject(uint32_t mem_count) {
  --object_depth_;
  if (UNLIKELY(IsRequiredField())) {
    // Don't support complex type yet, treated as null for now.
    // TODO: support complex type.
    scanner_->AddNull(current_field_idx_);
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  if (object_depth_ == 0 && array_depth_ == 0) {
    FinishRow();
    row_initialized_ = false;
    memset(field_found_.data(), false, field_found_.size());
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::StartArray() {
  ++array_depth_;
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::EndArray(uint32_t mem_count) {
  --array_depth_;
  if (UNLIKELY(IsRequiredField())) {
    // Don't support complex type yet, treated as null for now.
    // TODO: support complex type.
    scanner_->AddNull(current_field_idx_);
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::Null() {
  if (IsRequiredField()) {
    scanner_->AddNull(current_field_idx_);
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::Bool(bool boolean) {
  if (IsRequiredField()) {
    RETURN_IF_FALSE(scanner_->AddBool(current_field_idx_, boolean));
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::RawNumber(const char* str, uint32_t len, bool copy) {
  if (IsRequiredField()) {
    RETURN_IF_FALSE(scanner_->AddNumber(current_field_idx_, str, len));
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  return true;
}

template <class Scanner>
bool JsonParser<Scanner>::String(const char* str, uint32_t len, bool copy) {
  if (IsRequiredField()) {
    RETURN_IF_FALSE(scanner_->AddString(current_field_idx_, str, len));
    field_found_[current_field_idx_] = true;
    current_field_idx_ = -1;
  }
  return true;
}

template<class Stream>
bool JsonSkipper<Stream>::SkipNextObject() {
  code_ = kParseErrorNone;
  while (true) {
    SkipWhitespace();
    ERROR_IF_FALSE(s_.Peek() != '\0', kParseErrorDocumentEmpty);
    bool is_object = (s_.Peek() == '{');
    RETURN_IF_FALSE(SkipValue());
    if (LIKELY(is_object)) return true;
  }
}

template<class Stream>
bool JsonSkipper<Stream>::SkipNull() {
  DCHECK(s_.Peek() == 'n');
  s_.Take();
  ERROR_IF_FALSE(Consume('u'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('l'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('l'), kParseErrorValueInvalid);
  return true;
}

template<class Stream>
bool JsonSkipper<Stream>::SkipTrue() {
  DCHECK(s_.Peek() == 't');
  s_.Take();
  ERROR_IF_FALSE(Consume('r'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('u'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('e'), kParseErrorValueInvalid);
  return true;
}

template<class Stream>
bool JsonSkipper<Stream>::SkipFalse() {
  DCHECK(s_.Peek() == 'f');
  s_.Take();
  ERROR_IF_FALSE(Consume('a'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('l'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('s'), kParseErrorValueInvalid);
  ERROR_IF_FALSE(Consume('e'), kParseErrorValueInvalid);
  return true;
}

template<class Stream>
bool JsonSkipper<Stream>::SkipString() {
  DCHECK(s_.Peek() == '"');
  s_.Take();
  char c;
  bool escape = false;
  while ((c = s_.Peek()) != '\0') {
    if (escape) {
      escape = false;
    } else if (c == '\\') {
      escape = true;
    } else if (c == '"') {
      s_.Take();
      return true;
    }
    s_.Take();
  }
  ERROR_IF_FALSE(false, kParseErrorStringMissQuotationMark);
}

template<class Stream>
bool JsonSkipper<Stream>::SkipNumber() {
  // Please note that in standard JSON, number literals must start with a digit or a
  // minus sign (in the case of negative numbers). Positive numbers should be written
  // directly without a '+', and '0.123' should not be abbreviated as '.123'.
  // Numbers starting with '.' or '+' in JSON are considered invalid values, which is
  // consistent with the behavior of rapidjson.
  // Despite the fact that special values such as Inf and NaN are not supported in
  // standard JSON (they are considered invalid values), rapidjson does support them.
  // We have already enabled the parsing flag kParseNanAndInfFlag in the
  // JsonParser::Parse() to support parsing Inf and NaN, so this function also supports
  // them accordingly.
  Consume('-');
  if (UNLIKELY(s_.Peek() == '0')) {
    s_.Take();
  } else if (LIKELY(s_.Peek() >= '1' && s_.Peek() <= '9')) {
    while (LIKELY(s_.Peek() >= '0' && s_.Peek() <= '9')) s_.Take();
  } else if (LIKELY(s_.Peek() == 'N')) {
    s_.Take();
    ERROR_IF_FALSE(Consume('a'), kParseErrorValueInvalid);
    ERROR_IF_FALSE(Consume('N'), kParseErrorValueInvalid);
    return true;
  } else if (LIKELY(s_.Peek() == 'I')) {
    s_.Take();
    ERROR_IF_FALSE(Consume('n'), kParseErrorValueInvalid);
    ERROR_IF_FALSE(Consume('f'), kParseErrorValueInvalid);
    if (UNLIKELY(s_.Peek() == 'i')) {
      s_.Take();
      ERROR_IF_FALSE(Consume('n'), kParseErrorValueInvalid);
      ERROR_IF_FALSE(Consume('i'), kParseErrorValueInvalid);
      ERROR_IF_FALSE(Consume('t'), kParseErrorValueInvalid);
      ERROR_IF_FALSE(Consume('y'), kParseErrorValueInvalid);
    }
    return true;
  } else ERROR_IF_FALSE(false, kParseErrorValueInvalid);

  if (Consume('.')) {
    ERROR_IF_FALSE(s_.Peek() >= '0' && s_.Peek() <= '9', kParseErrorNumberMissFraction);
    while (LIKELY(s_.Peek() >= '0' && s_.Peek() <= '9')) s_.Take();
  }

  if (Consume('e') || Consume('E')) {
    if (!Consume('+')) Consume('-');
    ERROR_IF_FALSE(s_.Peek() >= '0' && s_.Peek() <= '9', kParseErrorNumberMissExponent);
    while (LIKELY(s_.Peek() >= '0' && s_.Peek() <= '9')) s_.Take();
  }
  return true;
}

template<class Stream>
bool JsonSkipper<Stream>::SkipObject() {
  DCHECK(s_.Peek() == '{');
  s_.Take();
  SkipWhitespace();
  if (Consume('}')) return true;
  while (true) {
    ERROR_IF_FALSE(s_.Peek() == '"', kParseErrorObjectMissName);
    RETURN_IF_FALSE(SkipString());
    SkipWhitespace();
    ERROR_IF_FALSE(Consume(':'), kParseErrorObjectMissColon);
    SkipWhitespace();
    RETURN_IF_FALSE(SkipValue());
    SkipWhitespace();
    if (Consume(',')) SkipWhitespace();
    else if (Consume('}')) return true;
    else ERROR_IF_FALSE(false, kParseErrorObjectMissCommaOrCurlyBracket);
  }
}

template<class Stream>
bool JsonSkipper<Stream>::SkipArray() {
  DCHECK(s_.Peek() == '[');
  s_.Take();
  SkipWhitespace();
  if (Consume(']')) return true;
  while (true) {
    RETURN_IF_FALSE(SkipValue());
    SkipWhitespace();
    if (Consume(',')) SkipWhitespace();
    else if (Consume(']')) return true;
    else ERROR_IF_FALSE(false, kParseErrorArrayMissCommaOrSquareBracket);
  }
}

template<class Stream>
bool JsonSkipper<Stream>::SkipValue() {
  // Please note that in standard JSON, the special values null, true, and false must all
  // be in lowercase form. Any other cases will be considered invalid values, which is
  // consistent with the behavior of rapidjson.
  switch (s_.Peek()) {
    case 'n': RETURN_IF_FALSE(SkipNull()); break;
    case 't': RETURN_IF_FALSE(SkipTrue()); break;
    case 'f': RETURN_IF_FALSE(SkipFalse()); break;
    case '"': RETURN_IF_FALSE(SkipString()); break;
    case '{': RETURN_IF_FALSE(SkipObject()); break;
    case '[': RETURN_IF_FALSE(SkipArray()); break;
    default: RETURN_IF_FALSE(SkipNumber()); break;
  }
  return true;
}

template class impala::JsonParser<SimpleJsonScanner>;
template class impala::JsonParser<HdfsJsonScanner>;
template class impala::JsonSkipper<SimpleStream>;
