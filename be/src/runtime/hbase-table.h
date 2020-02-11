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

#ifndef IMPALA_RUNTIME_HBASE_HTABLE_H_
#define IMPALA_RUNTIME_HBASE_HTABLE_H_

#include <jni.h>
#include <string>

#include "common/status.h"

namespace impala {

class RuntimeState;

/// Class to wrap JNI calls into Table.
class HBaseTable {
 public:
  HBaseTable(const std::string& table_name, jobject connection);
  ~HBaseTable();

  /// Close and release the Table wrapped by this class.
  void Close(RuntimeState* state);

  /// Create all needed java side objects.
  Status Init();

  /// From a java Scan object get a result scanner that will iterate over
  /// KeyValues from HBase.
  Status GetResultScanner(const jobject& scan, jobject* result_scanner);

  /// Send an list of puts to hbase through a Table.
  Status Put(const jobject& puts_list);

  /// Call this to initialize the HBase Table jni references
  static Status InitJNI();

 private:
  std::string table_name_;
  jobject connection_;
  jobject table_;

  /// org.apache.hadoop.hbase.client.Table
  static jclass table_cl_;

  /// org.apache.hadoop.hbase.client.Connection
  static jclass connection_cl_;

  /// connection.getTable(TableName)
  static jmethodID connection_get_table_id_;

  /// table.close()
  static jmethodID table_close_id_;

  /// table.getScannerId(Scan)
  static jmethodID table_get_scanner_id_;

  /// table.put(List<Put> puts
  static jmethodID table_put_id_;

  /// TableName class and static methods
  static jclass table_name_cl_;

  /// TableName.valueOf(String)
  static jmethodID table_name_value_of_id_;
};

}  // namespace impala
#endif /* IMPALA_RUNTIME_HBASE_HTABLE_H_ */
