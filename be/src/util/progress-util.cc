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

#include "util/progress-util.h"

#include <iomanip>
#include <sstream>

#include "common/names.h"

using namespace impala;

string impala::ProgressToString(int64_t num_completed, int64_t total) {
  stringstream ss;
  ss << num_completed << " / " << total << " (" << setw(4);
  if (num_completed == 0 || total == 0) {
    ss << "0" << "%)";
  } else {
    ss << (100.0 * num_completed / static_cast<double>(total)) << "%)";
  }
  return ss.str();
}
