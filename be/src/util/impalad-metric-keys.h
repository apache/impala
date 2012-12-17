// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_IMPALA_METRICS_H
#define IMPALA_UTIL_IMPALA_METRICS_H

namespace impala {

// Contains the keys (strings) for impala metrics.
class ImpaladMetricKeys {
 public:
  // Number of queries processed
  static const char* NUM_QUERIES;
  // Number of scan ranges processed
  static const char* TOTAL_SCAN_RANGES_PROCESSED;
  // Number of scan ranges with missing volume id metadata
  static const char* NUM_SCAN_RANGES_MISSING_VOLUME_ID;
};

};

#endif
