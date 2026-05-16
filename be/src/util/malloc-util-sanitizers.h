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
#include <sanitizer/allocator_interface.h>

#include "malloc-util.h"

#include "util/metrics.h"
#include "util/pretty-printer.h"

namespace impala {

/// The sanitizers replace malloc() and track memory usage themselves.
class SanitizerMallocMetric : public IntGauge {
 public:
  SanitizerMallocMetric(const TMetricDef& def) : IntGauge(def, 0) {}

  static SanitizerMallocMetric* BYTES_ALLOCATED;

  int64_t GetValue() override {
    return __sanitizer_get_current_allocated_bytes();
  }
};

SanitizerMallocMetric* SanitizerMallocMetric::BYTES_ALLOCATED = nullptr;

class SanitizerMallocUtil : public MallocUtil {
 public:
  Status Init(int64_t process_mem_limit) override { return Status::OK(); }

  std::string GetName() const override {
#if defined(ADDRESS_SANITIZER)
    return "AddressSanitizer";
#else
    return "ThreadSanitizer";
#endif
  }

  std::string GetTextDescription() const override {
    std::stringstream ss;
    ss << GetName() << " Total Bytes Allocated: "
       << PrettyPrinter::Print(
              SanitizerMallocMetric::BYTES_ALLOCATED->GetValue(), TUnit::BYTES);
    return ss.str();
  }

  Status RegisterMemoryMetrics(MetricGroup* metrics) override {
    SanitizerMallocMetric::BYTES_ALLOCATED = metrics->RegisterMetric(
        new SanitizerMallocMetric(MetricDefs::Get("sanitizer-total-bytes-allocated")));
    return Status::OK();
  }

  IntGauge* GetUsedBytesMetric(bool include_overhead) const override {
    DCHECK(SanitizerMallocMetric::BYTES_ALLOCATED != nullptr);
    return SanitizerMallocMetric::BYTES_ALLOCATED;
  }

  IntGauge* GetOverheadBytesMetric() const override {
    return nullptr;
  }

  HugePageSupport GetHugePageSupport() const override {
    // Sanitizers don't retain large chunks of memory, so this is compatible
    // with using madvise to get huge pages
    return HugePageSupport::MADVISE_COMPATIBLE;
  }
};

} // namespace impala
