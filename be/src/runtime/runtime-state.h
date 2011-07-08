// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

namespace impala {

class ObjectPool;

// A collection of items that are part of the global state of a 
// query and potentially shared across execution nodes.
class RuntimeState {
 public:
  RuntimeState();

  ObjectPool* obj_pool() const { return obj_pool_; }

 private:
  ObjectPool* obj_pool_;
};

}

#endif
