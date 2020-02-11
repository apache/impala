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

#include "util/dynamic-util.h"

#include <dlfcn.h>
#include <sstream>
#include "util/test-info.h"

#include "common/names.h"

namespace impala {

Status DynamicLookup(void* handle, const char* symbol, void** fn_ptr, bool quiet) {
  *(void **) (fn_ptr) = dlsym(handle, symbol);
  char* error = dlerror();
  if (error != NULL) {
    stringstream ss;
    ss << "Unable to find " << symbol << "\ndlerror: " << error;
    return quiet ? Status::Expected(ss.str()) : Status(ss.str());
  }
  return Status::OK();
}

Status DynamicOpen(const char* library, void** handle) {
  int flags = RTLD_NOW;
  // If we are loading shared libraries from the FE tests, where the Java
  // side loads the initial impala binary (libfesupport.so), we are unable
  // to load other libraries and have the symbols resolve. We'll load the
  // secondary libraries with RTLD_LAZY, which means the symbols don't need
  // to resolve at load time but will fail at dlsym(). This is generally
  // undesirable (we want to fail early) and also not the best solution. This
  // will prevent the FE tests from running the functions that cannot resolve
  // the symbols (e.g. planner tests with some UDFs).
  // TODO: this is to work around some build breaks. We need to fix this better.
  if (TestInfo::is_fe_test()) flags = RTLD_LAZY;
  *handle = dlopen(library, flags);
  if (*handle == NULL) {
    stringstream ss;
    ss << "Unable to load " << library << "\ndlerror: " << dlerror();
    return Status(ss.str());
  }
  return Status::OK();
}

void DynamicClose(void* handle) {
  dlclose(handle);
}

}
