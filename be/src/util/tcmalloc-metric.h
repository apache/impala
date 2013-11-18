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
  // Number of bytes allocated by tcmalloc, currently used by the application.
  static TcmallocMetric* BYTES_IN_USE;

  // Number of bytes of system memory reserved by tcmalloc, including that in use by the
  // application. Does not include what tcmalloc accounts for as 'malloc metadata' in
  // /memz. That is, this is memory reserved by tcmalloc that the application can use.
  // Includes unmapped virtual memory.
  static TcmallocMetric* TOTAL_BYTES_RESERVED;

  // Number of bytes reserved and still mapped by tcmalloc that are not allocated to the
  // application.
  static TcmallocMetric* PAGEHEAP_FREE_BYTES;

  // Number of bytes once reserved by tcmalloc, but released back to the operating system
  // so that their use incurs a pagefault. Contributes to the total amount of virtual
  // address space used, but not to the physical memory usage.
  static TcmallocMetric* PAGEHEAP_UNMAPPED_BYTES;

  // Derived metric computing the amount of physical memory (in bytes) used by the
  // process, including that actually in use and free bytes reserved by tcmalloc. Does not
  // include the tcmalloc metadata.
  class PhysicalBytesMetric : public Metrics::PrimitiveMetric<uint64_t> {
   public:
    PhysicalBytesMetric(const std::string& key)
        : Metrics::PrimitiveMetric<uint64_t>(key, 0) { }

   private:
    virtual void CalculateValue() {
      value_ = TOTAL_BYTES_RESERVED->value() - PAGEHEAP_UNMAPPED_BYTES->value();
    }

    virtual void PrintValue(std::stringstream* out) {
      (*out) << PrettyPrinter::Print(value_, TCounterType::BYTES);
    }
  };

  static PhysicalBytesMetric* PHYSICAL_BYTES_RESERVED;

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
