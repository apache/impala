// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

#include <boost/scoped_ptr.hpp>

namespace impala {

class DescriptorTbl;
class ObjectPool;

// A collection of items that are part of the global state of a 
// query and potentially shared across execution nodes.
class RuntimeState {
 public:
  RuntimeState(const DescriptorTbl& descs);

  ObjectPool* obj_pool() const { return obj_pool_.get(); }
  const DescriptorTbl& descs() const { return descs_; }
  int batch_size() const { return batch_size_; }
  int file_buf_size() const { return file_buf_size_; }

 private:
  const DescriptorTbl& descs_;
  boost::scoped_ptr<ObjectPool> obj_pool_;
  int batch_size_;
  int file_buf_size_;

  static const int DEFAULT_BATCH_SIZE = 1024;
  static const int DEFAULT_FILE_BUF_SIZE = 131072; // 128K

  // prohibit copies
  RuntimeState(const RuntimeState&);
};

}

#endif
