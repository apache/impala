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

#include "util/mem-metrics.h"

using namespace impala;

void impala::RegisterTcmallocMetrics(Metrics* metrics) {
#ifndef ADDRESS_SANITIZER
  // Number of bytes allocated by tcmalloc, currently used by the application.
  metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.bytes-in-use", "generic.current_allocated_bytes"));

  // Number of bytes of system memory reserved by tcmalloc, including that in use by the
  // application. Does not include what tcmalloc accounts for as 'malloc metadata' in
  // /memz. That is, this is memory reserved by tcmalloc that the application can use.
  metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.total-bytes-reserved", "generic.heap_size"));

  // Number of bytes reserved and still mapped by tcmalloc that are not allocated to the
  // application.
  metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-free-bytes", "tcmalloc.pageheap_free_bytes"));

  // Number of bytes once reserved by tcmalloc, but released back to the operating system
  // so that their use incurs a pagefault. This value is not included in
  // total-bytes-reserved. It does contribute to the total amount of virtual address space
  // used.
  metrics->RegisterMetric(new TcmallocMetric(
      "tcmalloc.pageheap-unmapped-bytes", "tcmalloc.pageheap_unmapped_bytes"));
#endif
}
