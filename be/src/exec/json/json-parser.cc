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
    constexpr auto parse_flags = kParseNumbersAsStringsFlag | kParseStopWhenDoneFlag;
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

template class impala::JsonParser<SimpleJsonScanner>;
template class impala::JsonParser<HdfsJsonScanner>;
