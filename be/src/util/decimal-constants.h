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

// Common constants shared between decimal-related modules.

#pragma once

#include <cstdint>

namespace impala {

/// Maximum absolute value of a valid Decimal4Value. This is 9 digits of 9's.
constexpr int32_t MAX_UNSCALED_DECIMAL4 = 999999999;

/// Maximum absolute value of a valid Decimal8Value. This is 18 digits of 9's.
constexpr int64_t MAX_UNSCALED_DECIMAL8 = 999999999999999999;

/// Maximum absolute value a valid Decimal16Value. This is 38 digits of 9's.
constexpr __int128_t MAX_UNSCALED_DECIMAL16 = 99 + 100 *
    (MAX_UNSCALED_DECIMAL8 + (1 + MAX_UNSCALED_DECIMAL8) *
     static_cast<__int128_t>(MAX_UNSCALED_DECIMAL8));

} // namespace impala

