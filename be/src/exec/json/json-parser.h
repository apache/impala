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

#include <functional>
#include <sstream>
#include <vector>
#include <unordered_map>

#include "common/compiler-util.h"
#include "common/status.h"
#include "rapidjson/error/en.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"

namespace impala {

/// A template class to assist in parsing JSON, using the member function Parse() to parse
/// the JSON text and convert it to a row format defined by the schema. The JSON text
/// consists of multiple JSON objects, each of which is parsed into one row of data.
///
/// Scanner is a class defined for input and output for this class, and it needs to
/// implement the following member functions:
///
/// Returns a boolean value to indicate whether Parse() should end parsing and return.
/// It is called once after parsing each object.
///   bool BreakParse();
///
/// Provides input for the parser, with 'begin' and 'end' being the start and end
/// positions of the next buffer to be parsed.
///   void GetNextBuffer(const char** begin, const char** end);
///
/// Handles errors. This function is called when the following functions encounter an
/// error (return false). If this function returns OK, parsing continues; otherwise, the
/// error status is returned by the Parse().
///   Status HandleError(rapidjson::ParseErrorCode error, size_t offset);
///
/// The following functions materialize output tuples. Functions with void return type
/// must succeed. Functions with bool return type return true on succeed, and return false
/// to stop parsing the whole scan range.
///
/// Called when starting to parse a new object, initializes a new row of data.
///   void InitRow();
///
/// Called when finishing parsing an object, submits a row of data.
///   void SubmitRow();
///
/// Called when encountering a null value during parsing. Index is the index of the key
/// for this value in the schema, and so on for the other following functions.
///   void AddNull(int index);
///   bool AddBool(int index, bool value);
///   bool AddString(int index, const char* str, uint32_t len);
///   bool AddNumber(int index, const char* str, uint32_t len);
///
/// This parser is implemented based on the SAX-style API provided by Rapidjson.
/// This class provides event handler functions for the rapidjson::Reader to achieve JSON
/// parsing. See more details in:
///   https://rapidjson.org/md_doc_sax.html
///   https://rapidjson.org/classrapidjson_1_1_handler.html
template <class Scanner>
class JsonParser {
public:
  /// A stream of characters used to wrap the buffer, wrapping the buffer into a format
  /// acceptable by RapidJson.
  class CharStream {
  public:
    typedef char Ch;

    CharStream(JsonParser* parser) : parser_(parser) { }

    /// Determines whether the stream has ended. After the current buffer is parsed,
    /// GetNextBuffer is called to request more data from Scanner. It is only considered
    /// the end of the stream when Scanner cannot provide more data.
    ALWAYS_INLINE bool Eos() {
      if (LIKELY(current_ != end_)) return false;
      tell_ = Tell();
      parser_->GetNextBuffer(&current_, &end_);
      begin_ = current_;
      return current_ == end_;
    }

    ALWAYS_INLINE Ch Peek() {
      return UNLIKELY(Eos()) ? '\0' : *current_;
    }

    ALWAYS_INLINE Ch Take() {
      return UNLIKELY(Eos()) ? '\0' : *current_++;
    }

    ALWAYS_INLINE size_t Tell() const {
      return static_cast<size_t>(current_ - begin_) + tell_;
    }

    /// The following functions are only required for the output stream, so we don't need
    /// to implement them here. However, to avoid compilation errors, we must explicitly
    /// indicate them as not available.
    Ch* PutBegin() { CHECK(false); return 0; }
    void Put(Ch) { CHECK(false); }
    size_t PutEnd(Ch*) { CHECK(false); return 0; }
    void Flush() { CHECK(false); }

  private:
    JsonParser* parser_;
    const Ch* current_ = nullptr;
    const Ch* begin_ = nullptr;
    const Ch* end_ = nullptr;
    size_t tell_ = 0;
  };

  JsonParser(const std::vector<std::string>& schema, Scanner* scanner);

  void ResetParser();

  /// A debug function that checks whether the parser is tidy. If it is not, it means that
  /// unexpected errors may have occurred in the parser.
  bool IsTidy();

  /// Consume char stream to find the start of the first tuple in this scan range. Return
  /// true if found, This function works under the premise that there is only one JSON
  /// object per line in the JSON file and there are no newline in the JSON object.
  bool MoveToNextJson();

  /// Using callback provided by Scanner to parses JSON data and converts it to row
  /// format. Returns in the following cases:
  /// 1. Maximum parsing row limit max_rows is reached.
  /// 2. No more data needs to be parsed (end of stream is reached).
  /// 3. An error is encountered when converting to a row, or a parsing error (caused by
  ///    invalid JSON format, etc.), and Scanner returns an error status after handling
  ///    the error.
  /// 4. Scanner's BreakParse() indicates the need to end parsing.
  /// Please note that 'max_rows' and 'num_rows' actually represent the number of
  /// top-level JSON values processed by the parser, meaning that if there are top-level
  /// arrays, strings, or other JSON values in the JSON data, they will also be included
  /// in the count.
  Status Parse(int max_rows, int* num_rows);

  /// Parse the JSON data and directly count how many top-level JSON objects (excluding
  /// nested ones) there are without performing specific data copying and conversion. It
  /// behaves similarly to Parse() but is faster, suitable for zero slots scans such as
  /// count(*). Different from Parse(), here 'max_rows' and 'num_rows' only count the
  /// top-level JSON objects and do not include other top-level JSON values.
  Status CountJsonObjects(int max_rows, int* num_rows);

private:
  friend class rapidjson::GenericReader<rapidjson::UTF8<>, rapidjson::UTF8<>>;

  inline void FinishRow() {
    DCHECK(row_initialized_);
    for (int i = 0; i < num_fields_; ++i) {
      if (UNLIKELY(!field_found_[i])) {
        scanner_->AddNull(i);
      }
    }
    scanner_->SubmitRow();
  }

  inline void GetNextBuffer(const char** begin, const char** end) {
    scanner_->GetNextBuffer(begin, end);
  }

  inline bool IsRequiredField() {
    return current_field_idx_ != -1 && object_depth_ == 1 && array_depth_ == 0;
  }

  /// The following functions are event handlers provided for Rapidjson SAX. When parsing
  /// a JSON, the corresponding handlers will be called upon encountering the
  /// corresponding element. The main processing flow for a row of data is as follows:
  /// 1. Call StartObject() at the beginning of the JSON object to initialize a new row.
  /// 2. Call Key() upon encountering a key to find its index of the row in the schema and
  ///    update current_field_idx_.
  /// 3. Call the corresponding type processing function upon encountering a value to add
  ///    the value to the position pointed to by current_field_idx_ in the row.
  /// 4. Call EndObject() upon reaching the end of the JSON object. Add null values for
  ///    fields not found in the schema, and submit this row.
  bool Key(const char* str, uint32_t len, bool copy);
  bool StartObject();
  bool EndObject(uint32_t mem_count);
  bool StartArray();
  bool EndArray(uint32_t mem_count);
  bool Null();
  bool Bool(bool boolean);
  bool RawNumber(const char* str, uint32_t len, bool copy);
  bool String(const char* str, uint32_t len, bool copy);

  /// We used the kParseNumbersAsStringsFlag flag for parsing, which output numerical type
  /// values as strings (by calling RawNumber). Therefore, the following handler functions
  /// will never be called and no need to be implemented. However, to avoid compilation
  /// errors, we still need to explicitly indicate them as not available.
  bool Int(int i) { CHECK(false); return false; }
  bool Uint(unsigned i) { CHECK(false); return false; }
  bool Int64(int64_t i) { CHECK(false); return false; }
  bool Uint64(uint64_t i) { CHECK(false); return false; }
  bool Double(double d) { CHECK(false); return false; }

  /// The number of fields in the schema.
  const size_t num_fields_;

  /// Scanner pointer used for invoking callback functions.
  Scanner* scanner_;

  /// Character stream that wraps the JSON data buffer.
  CharStream stream_;

  /// Mapping of field names to field positions, generated based on the schema and used to
  /// locate field positions when assembling rows.
  std::unordered_map<std::string, int> field_indexs_;

  /// RapidJson's SAX-style JSON parser.
  rapidjson::Reader reader_;

  /// This is mainly used to determine if we have an unfinished row when an error occurs.
  bool row_initialized_;

  /// Counter used to record the nesting depth of the current JSON array or object during
  /// parsing.
  int array_depth_;
  int object_depth_;

  /// Used to record the current field's position in the row during parsing.
  /// Updated by Key() based on 'field_indexs_', consumed and reset by other processors.
  /// -1 indicates a not required field (not in the schema).
  int current_field_idx_;

  /// Used to record which fields have been found in the current row during parsing.
  std::vector<char> field_found_;
};

/// A util class used to assist in parsing JSON. When conducting zero slots scans, no
/// actual data from the JSON is needed, only the number of JSON objects. This class is
/// essentially a simplified version of a rapidjson parser (rapidjson::GenericReader),
/// removing specific data parsing and copying operations, allowing for faster parsing of
/// the number of JSON objects.
/// The class retains the ability to recognize malformed JSON and provide specific error
/// codes like rapidjson's parser. However, as it skips specific data parsing, it cannot
/// identify string encoding errors or numeric overflow errors. Nonetheless, these data
/// errors do not affect the counting of JSON objects, and ignoring them is acceptable.
/// Please refer to the following link for code about rapidjson::GenericReader:
///   https://github.com/Tencent/rapidjson/blob/5ec44fb/include/rapidjson/reader.h#L539
template<class Stream>
class JsonSkipper {
 public:
  JsonSkipper(Stream& stream) : s_(stream) { }

  /// Consume the stream until skipping a complete outermost JSON object, return false and
  /// log the corresponding error code if an error occurs.
  bool SkipNextObject();

  bool HasError() { return code_ != rapidjson::kParseErrorNone; }
  rapidjson::ParseErrorCode GetErrorCode() { return code_; }

 private:
  friend class JsonParserTest;

  /// This function attempts to consume a character from the stream, if the next character
  /// matches the 'expect', take out it and return true, otherwise return false.
  inline bool Consume(char expect) {
    if (LIKELY(s_.Peek() == expect)) {
      s_.Take();
      return true;
    }
    return false;
  }

  inline void SkipWhitespace() {
    char c;
    while ((c = s_.Peek()) == ' ' || c == '\n' || c == '\r' || c == '\t') s_.Take();
  }

  /// The following function is used to skip a specific JSON value. It maintains logic
  /// consistent with rapidjson, consuming the stream and returning true upon successfully
  /// skipping the specified value, or returning false and setting the respective error
  /// code if an error is encountered.
  /// See more details about valid JSON values in: https://rapidjson.org/md_doc_sax.html
  bool SkipNull();
  bool SkipTrue();
  bool SkipFalse();
  bool SkipString();
  bool SkipNumber();
  bool SkipObject();
  bool SkipArray();
  bool SkipValue();

  Stream& s_;
  rapidjson::ParseErrorCode code_ = rapidjson::kParseErrorNone;
};

/// A simple c_str wrapper for testing JsonSkipper.
class SimpleStream {
public:
  SimpleStream(const char* str) : current_(str) { }

  bool Eos() { return *current_ == '\0'; }

  char Peek() { return *current_; }

  char Take() { return Eos() ? '\0' : *current_++; }

private:
  const char* current_ = nullptr;
};

/// A simple class for testing JsonParser.
class SimpleJsonScanner {
public:
  using GetBufferFunc = std::function<void(const char**, const char**)>;

  SimpleJsonScanner(const std::vector<std::string>& schema, GetBufferFunc get_buffer)
    : row_count_(0), parser_(schema, this), get_buffer_(std::move(get_buffer)) {
    parser_.ResetParser();
    current_row_.resize(schema.size());
  }

  Status Scan(int max_row, int* num_rows) {
    *num_rows = 0;
    if (!parser_.IsTidy()) return Status("Parser is not tidy");
    RETURN_IF_ERROR(parser_.Parse(max_row, num_rows));
    return Status::OK();
  }

  Status Count(int max_row, int* num_rows) {
    *num_rows = 0;
    RETURN_IF_ERROR(parser_.CountJsonObjects(max_row, num_rows));
    return Status::OK();
  }

  std::string result() const { return result_.str(); }

  size_t row_count() const { return row_count_; }

private:
  friend class JsonParser<SimpleJsonScanner>;

  Status HandleError(rapidjson::ParseErrorCode error, size_t offset) {
    return Status::OK();
  }

  bool BreakParse() {
    return false;
  }

  void GetNextBuffer(const char** begin, const char** end) {
    get_buffer_(begin, end);
  }

  void InitRow() { }

  void SubmitRow() {
    for (const auto& s : current_row_) result_ << s << ", ";
    result_ << '\n';
    ++row_count_;
  }

  void AddNull(int index) {
    current_row_[index] = "null";
  }

  bool AddBool(int index, bool b) {
    current_row_[index] = (b ? "true" : "false");
    return true;
  }

  bool AddString(int index, const char* b, uint32_t len) {
    current_row_[index] = string(b, len);
    return true;
  }

  bool AddNumber(int index, const char* b, uint32_t len) {
    current_row_[index] = string(b, len);
    return true;
  }

  std::vector<std::string> current_row_;
  std::stringstream result_;
  size_t row_count_;
  JsonParser<SimpleJsonScanner> parser_;
  GetBufferFunc get_buffer_;
};

} // namespace impala
