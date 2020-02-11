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

#include <string>

#include "util/metrics-fwd.h"

namespace impala {

/// This class stores the metrics that are common for the Impalad
/// Statestored and Catalogd processes.
/// Also responsible for registering and initializing these metrics.
class CommonMetrics {
public:
  static StringProperty* PROCESS_START_TIME;
  static StringProperty* KUDU_CLIENT_VERSION;

  /// Registers and initializes the commnon metrics
  static void InitCommonMetrics(MetricGroup* metric_group);

private:
 static std::string PROCESS_START_TIME_METRIC_NAME;
 static std::string KUDU_CLIENT_VERSION_METRIC_NAME;
};

}
