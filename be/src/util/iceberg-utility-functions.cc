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

#include "util/iceberg-utility-functions.h"

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"

#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gutil/strings/substitute.h"
#include "util/string-parser.h"

#include "common/names.h"

namespace impala {
namespace iceberg {

static constexpr int32_t ICEBERG_EPOCH_YEAR = 1970;

bool IsTimeBasedPartition(TIcebergPartitionTransformType::type transform_type) {
  if (transform_type == TIcebergPartitionTransformType::YEAR ||
      transform_type == TIcebergPartitionTransformType::MONTH ||
      transform_type == TIcebergPartitionTransformType::DAY ||
      transform_type == TIcebergPartitionTransformType::HOUR) {
    return true;
  }
  return false;
}

template <typename T>
string GetFormattedTimePoint(int32_t offset, const string& format) {
  static const T epoch;
  static const cctz::time_zone utc;
  T datetime = epoch + offset;
  auto time_point = cctz::convert(datetime, utc);
  return cctz::format(format, time_point, utc);
}

string HumanReadableYear(int32_t part_value) {
  return std::to_string(part_value + ICEBERG_EPOCH_YEAR);
}

string HumanReadableMonth(int32_t part_value) {
  return GetFormattedTimePoint<cctz::civil_month>(part_value, "%Y-%m");
}

string HumanReadableDay(int32_t part_value) {
  return GetFormattedTimePoint<cctz::civil_day>(part_value, "%Y-%m-%d");
}

string HumanReadableHour(int32_t part_value) {
  return GetFormattedTimePoint<cctz::civil_hour>(part_value, "%Y-%m-%d-%H");
}

string HumanReadableTime(TIcebergPartitionTransformType::type transform_type,
    const string& part_value, Status* status) {
  DCHECK(status != nullptr);
  StringParser::ParseResult parse_result;
  int32_t int_part_value = StringParser::StringToInt<int32_t>(
      part_value.c_str(), part_value.size(), &parse_result);
  if (parse_result != StringParser::ParseResult::PARSE_SUCCESS) {
    *status = Status(Substitute("Failed to parse time partition value '$0' as int.",
        part_value));
    return "";
  }
  *status = Status::OK();
  switch (transform_type) {
    case TIcebergPartitionTransformType::YEAR:  return HumanReadableYear(int_part_value);
    case TIcebergPartitionTransformType::MONTH: return HumanReadableMonth(int_part_value);
    case TIcebergPartitionTransformType::DAY:   return HumanReadableDay(int_part_value);
    case TIcebergPartitionTransformType::HOUR:  return HumanReadableHour(int_part_value);
    default:
        DCHECK(false);
        *status = Status(Substitute("Unknown transform type: $0", transform_type));
  }
  return "";
}

}
}
