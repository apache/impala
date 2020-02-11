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

#include "util/common-metrics.h"

#include <kudu/client/client.h>

#include "util/metrics.h"
#include "util/time.h"

namespace impala {

StringProperty* CommonMetrics::PROCESS_START_TIME = nullptr;
StringProperty* CommonMetrics::KUDU_CLIENT_VERSION = nullptr;

string CommonMetrics::PROCESS_START_TIME_METRIC_NAME = "process-start-time";
string CommonMetrics::KUDU_CLIENT_VERSION_METRIC_NAME = "kudu-client.version";

void CommonMetrics::InitCommonMetrics(MetricGroup* metric_group) {
  PROCESS_START_TIME = metric_group->AddProperty<string>(
      PROCESS_START_TIME_METRIC_NAME, "");
  KUDU_CLIENT_VERSION = metric_group->AddProperty<string>(
      KUDU_CLIENT_VERSION_METRIC_NAME, kudu::client::GetShortVersionString());

  PROCESS_START_TIME->SetValue(CurrentTimeString());
}

}
