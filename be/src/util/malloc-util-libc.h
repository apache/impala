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

#include <sstream>

#include <glog/logging.h>

#include "malloc-util.h"

#include "util/metrics.h"
#include "util/process-state-info.h"

namespace impala {

/// Libc doesn't have a simple API for tracking memory use from malloc, so we use
/// resident set size.
class LibcMallocMetric : public IntGauge {
 public:
  LibcMallocMetric(const TMetricDef& def) : IntGauge(def, 0) {}

  static LibcMallocMetric* RESIDENT_SET_SIZE;

  int64_t GetValue() override {
    // There may be more efficient ways to get this from the OS
    ProcessStateInfo proc_state(false);
    return proc_state.GetRss();
  }
};

LibcMallocMetric* LibcMallocMetric::RESIDENT_SET_SIZE = nullptr;

class LibcMallocUtil : public MallocUtil {
 public:
  Status Init() override { return Status::OK(); }

  std::string GetName() const override {
    return "Libc";
  }

  std::string GetTextDescription() const override {
    std::stringstream ss;
    ss << GetName() << " doesn't support a text description.";
    return ss.str();
  }

  Status RegisterMemoryMetrics(MetricGroup* metrics) override {
    LibcMallocMetric::RESIDENT_SET_SIZE = metrics->RegisterMetric(
        new LibcMallocMetric(MetricDefs::Get("libc-resident-set-size")));
    return Status::OK();
  }

  IntGauge* GetUsedBytesMetric(bool include_overhead) const override {
    DCHECK(LibcMallocMetric::RESIDENT_SET_SIZE != nullptr);
    return LibcMallocMetric::RESIDENT_SET_SIZE;
  }

  IntGauge* GetOverheadBytesMetric() const override {
    return nullptr;
  }

  HugePageSupport GetHugePageSupport() const override {
    // libc doesn't retain large chunks of memory, so this is compatible with using
    // madvise to get huge pages.
    return HugePageSupport::MADVISE_COMPATIBLE;
  }
};

} // namespace impala
