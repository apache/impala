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

#ifndef IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H
#define IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H

#include <jni.h>
#include <mutex>
#include <string>

#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "runtime/hbase-table.h"

namespace impala {

/// A (process-wide) factory of Table java objects.
/// This object keeps a Connection object around to ease creation of Tables
/// that share a pool of threads and connections.
/// TODO: Consider writing factory in Java to save JNI logic (and to avoid
/// having to mark objects as global refs)
/// TODO: Implement a cache to avoid creating more HTables than necessary
/// TODO: Add metrics to track the number of tables outstanding
class HBaseTableFactory {
 public:
  HBaseTableFactory();
  virtual ~HBaseTableFactory();

  /// create an HTable java object for the given table name.
  /// It is the caller's responsibility to close the HBaseTable using
  /// HBaseTable#Close().
  Status GetTable(const std::string& table_name,
                  boost::scoped_ptr<HBaseTable>* hbase_table);

 private:
  /// Connection jobject. Initialized in GetConnection(). The connection_ pointer value
  /// is protected by connection_lock_: the Connection object can be shared between
  /// threads.
  std::mutex connection_lock_;
  jobject connection_;

  /// Connection class and methods.
  jclass connection_cl_;
  jmethodID connection_close_id_;

  /// Opens the connections to HBase and Zookeeper on the first call.
  /// Returns the existing connection on subsequent calls.
  Status GetConnection(jobject* connection);
};

}  // namespace impala

#endif  // IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H
