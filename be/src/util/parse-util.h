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

#include <cstdint>
#include <map>
#include <string>

#include <boost/algorithm/string/predicate.hpp>

#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h" // for THdfsCompression
#include "gutil/strings/substitute.h"

namespace impala {

struct MemSpec {
  int64_t value = {};
  // MemSpec parsing can't distinguish 0 and infinity/unspecified (-1 parsed as 0)
  // values, for printing ranges we have to make 0 available
  bool could_be_inf_or_unspecified = false;

  bool operator<(const MemSpec& other) const { return this->value < other.value; }
  bool operator<=(const MemSpec& other) const { return this->value <= other.value; }
  bool operator>(const MemSpec& other) const { return this->value > other.value; }
  bool operator>=(const MemSpec& other) const { return this->value >= other.value; }

  friend std::ostream& operator<<(std::ostream& os, const MemSpec& mem_spec) {
    if (mem_spec.could_be_inf_or_unspecified && mem_spec.value == 0) {
      os << "Inf/Not specified";
    } else {
      os << mem_spec.value;
    }
    return os;
  }
};

/// Utility class for parsing information from strings.
class ParseUtil {
 public:
  /// Parses mem_spec_str and returns the memory size in bytes.
  /// Sets *is_percent to indicate whether the given spec is in percent.
  /// Accepted formats:
  /// '<int>[bB]?'  -> bytes (default if no unit given)
  /// '<float>[kK(bB)]' -> kilobytes
  /// '<float>[mM(bB)]' -> megabytes
  /// '<float>[gG(bB)]' -> in gigabytes
  /// '<int>%'      -> in percent of relative_reference
  /// 'relative_reference' -> value used to compute the percentage value,
  ///     typically MemInfo::physical_mem()
  /// Requires MemInfo to be initialized for the '%' spec to work.
  /// Returns 0 if mem_spec_str is empty or '-1'.
  /// Returns -1 if parsing failed.
  static int64_t ParseMemSpec(const std::string& mem_spec_str,
      bool* is_percent, int64_t relative_reference);

  static Status ParseCompressionCodec(
      const std::string& compression_codec, THdfsCompression::type* type, int* level);
};

std::string GetThriftEnumValues(const std::map<int, const char*>& enum_values_to_names);

/// Parses a string into a value of 'ENUM_TYPE' - if the string matches the enum name
/// (case-insensitive), that value is returned.
/// Return an error for an invalid Thrift enum value.
template <typename ENUM_TYPE>
static inline Status GetThriftEnum(const std::string& value, const std::string& desc,
    const std::map<int, const char*>& enum_values_to_names, ENUM_TYPE* enum_value) {
  for (const auto& e : enum_values_to_names) {
    if (boost::algorithm::iequals(value, std::to_string(e.first))
        || boost::algorithm::iequals(value, e.second)) {
      *enum_value = static_cast<ENUM_TYPE>(e.first);
      return Status::OK();
    }
  }
  return Status(strings::Substitute("Invalid $0: '$1'. Valid values are $2.", desc, value,
      GetThriftEnumValues(enum_values_to_names)));
}
}
