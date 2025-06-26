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

#include <glog/logging.h>

// GFlag validator that asserts the provided value is greater than or equal to 0.
template<typename T>
bool ge_zero(const char* flagname, const T value) {
  if (value >= 0){
    return true;
  }

  LOG(ERROR) << "Flag '" << flagname << "' must be greater than or equal to 0.";
  return false;
};

// GFlag validator that asserts the provided value is greater than or equal to 1.
// Double values greater than 0 but less than 1 will fail validation.
template<typename T>
bool ge_one(const char* flagname, const T value) {
  if (value < 1) {
    LOG(ERROR) << "Flag '" << flagname << "' must be greater than or equal to 1.";
    return false;
  }

  return true;
}

// GFlag validator that asserts the provided value is greater than 0.
// Double values greater than 0 but less than 1 will pass validation.
template<typename T>
bool gt_zero(const char* flagname, const T value) {
  if (value <= 0) {
    LOG(ERROR) << "Flag '" << flagname << "' must be greater than 0.";
    return false;
  }

  return true;
}
