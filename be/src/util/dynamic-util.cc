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

#include "util/dynamic-util.h"
#include <dlfcn.h>
#include <sstream>

using namespace std;

namespace impala {

Status DynamicLookup(void* handle, const char* symbol, void** fn_ptr) {
  *(void **) (fn_ptr) = dlsym(handle, symbol);
  char* error = dlerror();
  if (error != NULL) {
    stringstream ss;
    ss << "Unable to find " << symbol << " dlerror: " << error;
    return Status(ss.str());
  }
  return Status::OK;
}

Status DynamicOpen(const string& library, void** handle) {
  *handle = dlopen(library.c_str(), RTLD_NOW);
  if (*handle == NULL) {
    stringstream ss;
    ss << "Unable to load " << library << " dlerror: " << dlerror();
    return Status(ss.str());
  }
  return Status::OK;
}

}
