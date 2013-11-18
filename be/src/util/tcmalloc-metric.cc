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

#include "util/tcmalloc-metric.h"

using namespace impala;

TcmallocMetric* TcmallocMetric::BYTES_IN_USE = NULL;
TcmallocMetric* TcmallocMetric::PAGEHEAP_FREE_BYTES = NULL;
TcmallocMetric* TcmallocMetric::TOTAL_BYTES_RESERVED = NULL;
TcmallocMetric* TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = NULL;
TcmallocMetric::PhysicalBytesMetric* TcmallocMetric::PHYSICAL_BYTES_RESERVED = NULL;

void impala::RegisterTcmallocMetrics(Metrics* metrics) {
#ifndef ADDRESS_SANITIZER
  TcmallocMetric::BYTES_IN_USE = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.bytes-in-use", "generic.current_allocated_bytes"));

  TcmallocMetric::TOTAL_BYTES_RESERVED = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.total-bytes-reserved", "generic.heap_size"));

  TcmallocMetric::PAGEHEAP_FREE_BYTES = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-free-bytes", "tcmalloc.pageheap_free_bytes"));

  TcmallocMetric::PAGEHEAP_UNMAPPED_BYTES = metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes"));

  TcmallocMetric::PHYSICAL_BYTES_RESERVED = metrics->RegisterMetric(
      new TcmallocMetric::PhysicalBytesMetric("tcmalloc.physical-bytes-reserved"));
#endif
}
