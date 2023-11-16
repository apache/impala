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

#include "gen-cpp/CatalogObjects_types.h"

namespace impala {

class Status;

namespace iceberg {

/// Returns True if 'transform_type' is transformation based on a time value.
/// I.e. YEAR, MONTH, DAY, HOUR
bool IsTimeBasedPartition(TIcebergPartitionTransformType::type transform_type);

/// Returns a human readable year.
/// Based on Iceberg's TransformUtil.humanYear()
/// E.g.:
/// 0  => 1970
/// 53 => 2023
std::string HumanReadableYear(int32_t part_value);

/// Returns a human readable month.
/// Based on Iceberg's TransformUtil.humanMonth()
/// E.g.:
/// 0  => 1970-01
/// 36 => 1973-01
std::string HumanReadableMonth(int32_t part_value);

/// Returns a human readable day.
/// Based on Iceberg's TransformUtil.humanDay()
/// E.g.:
/// 0  => 1970-01-01
/// 10 => 1970-01-11
std::string HumanReadableDay(int32_t part_value);

/// Returns a human readable hour.
/// Based on Iceberg's TransformUtil.humanHour()
/// E.g.:
/// 0  => 1970-01-01-00
/// 10 => 1970-01-01-10
std::string HumanReadableHour(int32_t part_value);

/// @param transform_type is the Iceberg partition transform (YEAR, MONTH, etc.).
/// @param part_value is the transformed value (offset since unix epoch).
/// @param transform_result set to ERROR in case of failure.
/// @return Returns a human readable time value.
std::string HumanReadableTime(
    TIcebergPartitionTransformType::type transform_type, const std::string& part_value,
    Status* transform_result);

}
}
