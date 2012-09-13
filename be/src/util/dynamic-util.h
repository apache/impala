// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#ifndef IMPALA_UTIL_DYNAMIC_UTIL_H
#define IMPALA_UTIL_DYNAMIC_UTIL_H

#include "common/status.h"
#include "runtime/runtime-state.h"

namespace impala {

// Look up smybols in a dynamically linked library.
// state -- runtime state for errors.
// handle -- handle to the library.
// symbol -- symbol to lookup.
// fn_ptr -- pointer tor retun addres of function.
Status DynamicLookup(RuntimeState* state,
                     void* handle, const char* symbol, void** fn_ptr);

// Open a dynamicly loaded library.
// state -- runtime state for returnning errors.
// library -- name of the library.  The default paths will be searched.
// flags -- loader flags.
// handle -- returned handle to the library.
Status DynamicOpen(RuntimeState* state,
                   const std::string& library, const int flags, void** handle);
}
#endif
