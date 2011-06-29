// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

namespace impala {

// A collection of items that are part of the global state of a 
// query and potentially shared across execution nodes.
// Example: system time returned by SQL function now()
struct RuntimeState {
};

}

#endif
