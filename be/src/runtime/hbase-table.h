// Copyright 2013 Cloudera Inc.
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

#ifndef IMPALA_RUNTIME_HBASE_HTABLE_H_
#define IMPALA_RUNTIME_HBASE_HTABLE_H_

#include <boost/thread.hpp>
#include <jni.h>
#include <string>

#include "common/status.h"

namespace impala {

class RuntimeState;

// Class to wrap JNI calls into HTable.
class HBaseTable {
 public:
  HBaseTable(const std::string& table_name, jobject& conf, jobject& executor);
  ~HBaseTable();

  // Close and release the HTable wrapped by this class.
  void Close(RuntimeState* state);

  // Create all needed java side objects.
  // This call can cause connections to HBase and Zookeeper to be created.
  Status Init();

  // From a java Scan object get a result scanner that will iterate over
  // KeyValues from HBase.
  Status GetResultScanner(const jobject& scan, jobject* result_scanner);

  // Send an list of puts to hbase through an HTable.
  Status Put(const jobject& puts_list);

  // Call this to initialize the HBase HTable jni references
  static Status InitJNI();

 private:
  std::string table_name_;
  jobject conf_;
  jobject executor_;
  jobject htable_;

  // org.apache.hadoop.hbase.client.HTable
  static jclass htable_cl_;

  // new HTable(Configuration, ExecutorService)
  static jmethodID htable_ctor_;

  // htable.close()
  static jmethodID htable_close_id_;

  // htable.getScannerId(Scan)
  static jmethodID htable_get_scanner_id_;

  // htable.put(List<Put> puts
  static jmethodID htable_put_id_;

  // Bytes class and static methods
  static jclass bytes_cl_;

  // Bytes.toBytes(String)
  static jmethodID bytes_to_bytes_id_;
};

}  // namespace impala
#endif /* IMPALA_RUNTIME_HBASE_HTABLE_H_ */
