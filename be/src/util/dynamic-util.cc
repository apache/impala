// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#include <dlfcn.h>
#include <sstream>
#include "util/dynamic-util.h"

using namespace std;
namespace impala {
Status DynamicLookup(RuntimeState* state,
                     void* handle, const char* symbol, void** fn_ptr) {
  *(void **) (fn_ptr) = dlsym(handle, symbol);
  char* error = dlerror();
  if (error != NULL) {
    stringstream ss;
    ss << "Unable to find " << symbol << " dlerror: " << error;
    if (state->LogHasSpace()) state->LogError(ss.str());
    return Status(ss.str());
  }
  return Status::OK;
}

Status DynamicOpen(RuntimeState* state,
                   const string& library, const int flags, void** handle) {
  *handle = dlopen(library.c_str(), flags);
  if (*handle == NULL) {
    stringstream ss;
    ss << "Unable to load " << library << " dlerror: " << dlerror();
    if (state->LogHasSpace()) state->LogError(ss.str());
    return Status(ss.str());
  }
  return Status::OK;
}
}
