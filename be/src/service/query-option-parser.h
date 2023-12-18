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

#ifndef IMPALA_SERVICE_QUERY_OPTION_PARSER_H
#define IMPALA_SERVICE_QUERY_OPTION_PARSER_H

#include <functional>
#include <sstream>
#include <string>
#include <type_traits>

#include "common/status.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gutil/strings/strip.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "util/string-parser.h"

namespace impala {

template <typename T>
class QueryOptionValidator {
  static_assert(std::is_arithmetic<T>::value || std::is_same<T, MemSpec>::value,
      "Not supported type");

 public:
  static inline Status InclusiveRange(
      TImpalaQueryOptions::type option, const T value, const T lower, const T upper) {
    if (value < lower || value > upper) {
      std::stringstream ss;
      ss << value << " is not in range [" << lower << ", " << upper << "]";
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status ExclusiveRange(
      TImpalaQueryOptions::type option, const T value, const T lower, const T upper) {
    if (value <= lower || value >= upper) {
      std::stringstream ss;
      ss << value << " is not in range (" << lower << ", " << upper << ")";
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status InclusiveLowerBound(
      TImpalaQueryOptions::type option, const T value, const T lower) {
    if (value < lower) {
      std::stringstream ss;
      ss << "Value must be greater than or equal to " << lower
         << ", actual value: " << value;
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status ExclusiveLowerBound(
      TImpalaQueryOptions::type option, const T value, const T lower) {
    if (value <= lower) {
      std::stringstream ss;
      ss << "Value must be greater than " << lower << ", actual value: " << value;
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status NotEquals(
      TImpalaQueryOptions::type option, const T value, const T other) {
    if (value == other) {
      std::stringstream ss;
      ss << "Value can't be " << other << ", actual value: " << value;
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status NonNegative(TImpalaQueryOptions::type option, const T value) {
    if (value < T{0}) {
      std::stringstream ss;
      ss << "Value must be non-negative: " << value;
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

  static inline Status PowerOf2(TImpalaQueryOptions::type option, const T value) {
    // Converting accepted types to int64_t to make this function applicable to MemSpec
    int64_t int_value = ConvertToInt(value);
    if (!impala::BitUtil::IsPowerOf2(int_value)) {
      std::stringstream ss;
      ss << "Value must be a power of two: " << value;
      return CreateValidationErrorStatus(option, ss.str());
    }
    return Status::OK();
  }

 private:
  // Converter to make PowerOf2 usable to all accepted types, this identity function
  // maps all arithmetic values to themselves
  static int64_t ConvertToInt(int64_t value) { return value; }
  // This overload extracts out the numeric value from MemSpec
  static int64_t ConvertToInt(const MemSpec& mem_spec) { return mem_spec.value; }

  static inline Status CreateValidationErrorStatus(
      TImpalaQueryOptions::type option, const std::string& error_message) {
    return Status(strings::Substitute("Invalid value for query option $0: $1",
        _TImpalaQueryOptions_VALUES_TO_NAMES.at(option), error_message));
  }
};

template <typename T>
struct ParseResult {
  const T value;
  bool is_parse_ok;
};

class QueryOptionParser {
 public:
  template <typename T>
  static Status Parse(
      TImpalaQueryOptions::type option, const std::string& value, T* result) {
    ParseResult<T> parse_result = ParseInternal<T>(value);
    if (!parse_result.is_parse_ok) {
      return CreateParseErrorStatus(option, value);
    }
    *result = parse_result.value;
    return Status::OK();
  }

  template <typename T>
  static Status ParseAndCheckInclusiveRange(TImpalaQueryOptions::type option,
      const std::string& value, const T lower, const T upper, T* result) {
    Status status = Parse(option, value, result);
    RETURN_IF_ERROR(status);
    return QueryOptionValidator<T>::InclusiveRange(option, *result, lower, upper);
  }

  template <typename T>
  static Status ParseAndCheckExclusiveRange(TImpalaQueryOptions::type option,
      const std::string& value, const T lower, const T upper, T* result) {
    Status status = Parse(option, value, result);
    RETURN_IF_ERROR(status);
    return QueryOptionValidator<T>::ExclusiveRange(option, *result, lower, upper);
  }

  template <typename T>
  static Status ParseAndCheckInclusiveLowerBound(TImpalaQueryOptions::type option,
      const std::string& value, const T lower, T* result) {
    Status status = Parse(option, value, result);
    RETURN_IF_ERROR(status);
    return QueryOptionValidator<T>::InclusiveLowerBound(option, *result, lower);
  }

  template <typename T>
  static Status ParseAndCheckExclusiveLowerBound(TImpalaQueryOptions::type option,
      const std::string& value, const T lower, T* result) {
    Status status = Parse(option, value, result);
    RETURN_IF_ERROR(status);
    return QueryOptionValidator<T>::ExclusiveLowerBound(option, *result, lower);
  }

  template <typename T>
  static Status ParseAndCheckNonNegative(
      TImpalaQueryOptions::type option, const std::string& value, T* result) {
    Status status = Parse(option, value, result);
    RETURN_IF_ERROR(status);
    return QueryOptionValidator<T>::NonNegative(option, *result);
  }

 private:
  static Status CreateParseErrorStatus(
      TImpalaQueryOptions::type option, const std::string& value) {
    return Status(TErrorCode::QUERY_OPTION_PARSE_FAILED,
        _TImpalaQueryOptions_VALUES_TO_NAMES.at(option), value);
  }

  template <typename Integral,
      std::enable_if_t<std::is_integral<Integral>::value, Integral>* = nullptr>
  static ParseResult<Integral> ParseInternal(const std::string& value) {
    StringParser::ParseResult parse_result;
    const auto result =
        StringParser::StringToInt<Integral>(value.c_str(), value.size(), &parse_result);

    return {result, parse_result == StringParser::PARSE_SUCCESS};
  }

  template <typename Float,
      std::enable_if_t<std::is_floating_point<Float>::value, Float>* = nullptr>
  static ParseResult<Float> ParseInternal(const std::string& value) {
    StringParser::ParseResult parse_result;
    const auto result =
        StringParser::StringToFloat<Float>(value.c_str(), value.size(), &parse_result);

    return {result, parse_result == StringParser::PARSE_SUCCESS};
  }

  static ParseResult<MemSpec> ParseMemSpecInternal(const std::string& value) {
    bool is_percent = false;
    const int64_t mem_spec =
        ParseUtil::ParseMemSpec(value, &is_percent, MemInfo::physical_mem());

    bool is_parse_ok = mem_spec >= 0;

    // Percent values are not allowed for query options
    if (is_percent) {
      is_parse_ok = false;
    }

    return {{mem_spec, true}, is_parse_ok};
  }
};

template <>
Status QueryOptionParser::Parse(
    TImpalaQueryOptions::type option, const std::string& value, MemSpec* result) {
  ParseResult<MemSpec> parse_result = ParseMemSpecInternal(value);
  if (!parse_result.is_parse_ok) {
    return CreateParseErrorStatus(option, value);
  }
  *result = parse_result.value;
  return Status::OK();
}

} // namespace impala

#endif
