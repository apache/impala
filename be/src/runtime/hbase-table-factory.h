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


#ifndef IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H
#define IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H

#include <jni.h>
#include <string>

#include "common/status.h"

namespace impala {

// A (process-wide) factory of HTable java objects.
// This object keeps java objects around to ease creation of HTables
// that share a pool of threads and connections.
// TODO: Consider writing factory in Java to save JNI logic (and to avoid
// having to mark objects as global refs)
// TODO: Implement a cache to avoid creating more HTables than necessary
// TODO: Add metrics to track the number of tables outstanding
// TODO: Consider merging JNI handles with HBaseTableScanner where possible to
// avoid duplicating JNI work (maybe by writing a C++-side HTable class)
class HBaseTableFactory {
 public:
  ~HBaseTableFactory();

  // JNI setup. Create global references to classes,
  // and find method ids.
  static Status Init();

  // create an HTable java object for the given table name.
  // The new htable is returned in global_htable
  // It is the caller's responsibility to close the HTable by calling
  // CloseHTable when the HTable is no longer needed.
  Status GetHBaseTable(const std::string& table_name, jobject* global_htable);

  // Close the java HTable.  This should allow java to spin
  // down unneeded resources.
  Status CloseHTable(const jobject& htable);

 private:
  static jclass htable_cl_;
  static jmethodID htable_ctor_;
  static jmethodID htable_close_id_;

  static jclass bytes_cl_;
  static jmethodID bytes_to_bytes_id_;

  static jclass executor_cl_;
  static jmethodID executor_shutdown_id_;

  // Configuration and executor jobject's. Initialized in Init().
  static jobject conf_;
  static jobject executor_;
};

}  // namespace impala

#endif  // IMPALA_RUNTIME_HBASE_TABLE_FACTORY_H
