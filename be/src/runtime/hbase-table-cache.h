// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_HBASE_TABLE_CACHE_H
#define IMPALA_RUNTIME_HBASE_TABLE_CACHE_H

#include <jni.h>
#include "common/status.h"
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

namespace impala {

// A (process-wide) cache of HTable java objects.
// These objects are shared across all threads and kept until the process terminates.
class HBaseTableCache {
 public:
  ~HBaseTableCache();

  // JNI setup. Create global references to classes,
  // and find method ids.
  static Status Init();

  // Return the HTable java object for the given table name. If the HTable does not exist
  // in the cache, it'll be constructed and added to the cache.
  jobject GetHBaseTable(const std::string& table_name);

 private:
  boost::mutex lock_;  // protects table_map
  typedef boost::unordered_map<std::string, jobject> HTableMap;
  HTableMap table_map_;

  static jclass htable_cl_;
  static jmethodID htable_ctor_;
  static jmethodID htable_close_id_;

  // HBaseConfiguration jobject. Initialized in Init().
  static void* hbase_conf_;
};

}

#endif
