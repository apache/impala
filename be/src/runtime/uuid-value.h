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

#include <array>
#include <cstring>

#include "common/logging.h"

namespace impala {

/// Represents a UUID as 16 raw bytes (Iceberg/Parquet UUID logical type).
class UuidValue {
 public:
  static constexpr int BYTE_SIZE = 16;

  void Assign(const void* src, int len) {
    DCHECK_EQ(len, BYTE_SIZE);
    memcpy(bytes_.data(), src, BYTE_SIZE);
  }

 private:
  std::array<uint8_t, BYTE_SIZE> bytes_;
};

static_assert(sizeof(UuidValue) == UuidValue::BYTE_SIZE);

} // namespace impala
