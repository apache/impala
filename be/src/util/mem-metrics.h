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

#ifndef IMPALA_UTIL_MEM_METRICS_H
#define IMPALA_UTIL_MEM_METRICS_H

#include "util/metrics.h"

#include <google/malloc_extension.h>

#include "util/debug-util.h"

namespace impala {

// Specialised metric which exposes numeric properties from tcmalloc.
class TcmallocMetric : public Metrics::PrimitiveMetric<uint64_t> {
 public:
  TcmallocMetric(const std::string& key, const std::string& tcmalloc_var)
      : Metrics::PrimitiveMetric<uint64_t>(key, 0), tcmalloc_var_(tcmalloc_var) { }

 private:
  // Name of the tcmalloc property this metric should fetch.
  const std::string tcmalloc_var_;

  virtual void CalculateValue() {
    MallocExtension::instance()->GetNumericProperty(tcmalloc_var_.c_str(), &value_);
  }

  virtual void PrintValue(std::stringstream* out) {
    (*out) << PrettyPrinter::Print(value_, TCounterType::BYTES);
  }
};

// Registers common tcmalloc metrics.
void RegisterTcmallocMetrics(Metrics* metrics);

}

#endif
