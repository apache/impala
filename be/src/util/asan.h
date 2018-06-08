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

#ifndef UTIL_ASAN_H_
#define UTIL_ASAN_H_

// Compatibility with non-clang compilers.
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(address_sanitizer)
#define ASAN_NO_INSTRUMENTATION __attribute__((no_sanitize("address")))
#else
#define ASAN_NO_INSTRUMENTATION
#endif

#if defined(ADDRESS_SANITIZER)
#include <sanitizer/lsan_interface.h>
#define IGNORE_LEAKING_OBJECT(p) __lsan_ignore_object(p)
#else
#define IGNORE_LEAKING_OBJECT(p)
#endif


#endif //UTIL_ASAN_H_
