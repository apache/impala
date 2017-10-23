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

#include "runtime/runtime-filter.inline.h"

#include "util/time.h"

#include "common/names.h"

using namespace impala;

const int RuntimeFilter::SLEEP_PERIOD_MS = 20;

const char* RuntimeFilter::LLVM_CLASS_NAME = "class.impala::RuntimeFilter";

bool RuntimeFilter::WaitForArrival(int32_t timeout_ms) const {
  do {
    if (HasFilter()) return true;
    SleepForMs(SLEEP_PERIOD_MS);
  } while ((MonotonicMillis() - registration_time_) < timeout_ms);

  return HasFilter();
}
