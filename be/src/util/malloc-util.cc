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

#include <glog/logging.h>

#include "util/malloc-util.h"

#if defined(MALLOC_UTIL_SANITIZER_IMPL)
#include "util/malloc-util-sanitizers.h"
#elif defined(MALLOC_UTIL_GPERFTOOLS_IMPL)
#include "util/malloc-util-gperftools.h"
#elif defined(MALLOC_UTIL_LIBC_IMPL)
#include "util/malloc-util-libc.h"
#else
#error "Unknown implementation"
#endif

namespace impala {

MallocUtil* MallocUtil::GetInstance() {
  // With C++11, static variables are initialized the first time a function is called,
  // and initialization is thread safe. These all have trivial constructors.
#if defined(MALLOC_UTIL_SANITIZER_IMPL)
  static MallocUtil* malloc_util_instance = new SanitizerMallocUtil();
#elif defined(MALLOC_UTIL_GPERFTOOLS_IMPL)
  static MallocUtil* malloc_util_instance = new GperftoolsMallocUtil();
#elif defined(MALLOC_UTIL_LIBC_IMPL)
  static MallocUtil* malloc_util_instance = new LibcMallocUtil();
#endif
  return malloc_util_instance;
}

std::ostream& operator<<(std::ostream& out, const MallocUtil::HugePageSupport& h) {
  switch (h) {
  case MallocUtil::HugePageSupport::MADVISE_COMPATIBLE:
    out << "MADVISE_COMPATIBLE";
    break;
  case MallocUtil::HugePageSupport::MADVISE_INCOMPATIBLE:
    out << "MADVISE_INCOMPATIBLE";
    break;
  }
  return out;
}

} // namespace impala
