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

#pragma once

#include "common/status.h"
#include "runtime/descriptors.h"

#include <jni.h>
#include <string>
#include <arrow/c/bridge.h>

namespace impala {

class RuntimeState;

/// Adapter class of the FE PaimonJniScanner, wraps the JNI calls as C++ methods.
class PaimonJniScanner {
 public:
  PaimonJniScanner(const std::string& scan_param, const TupleDescriptor* tuple_desc,
      const std::string& table_name);

  /// JNI setup. Creates global references for Java classes and finds method ids.
  /// Initializes static members, should be called once per process lifecycle.
  static Status InitJNI() WARN_UNUSED_RESULT;

  // Initializes this object, creates the java metadata scanner object.
  Status Init(JNIEnv* env) WARN_UNUSED_RESULT;

  /// Executes an Paimon scan through JNI.
  Status ScanTable(JNIEnv* env) WARN_UNUSED_RESULT;

  /// Gets the next arrow batch from 'org.apache.impala.util.paimon.PaimonJniScanner'.
  Status GetNextBatchDirect(JNIEnv* env, struct ArrowArray** array,
      struct ArrowSchema** schema, long* rows, long* offheap_used) WARN_UNUSED_RESULT;

  /// Removes global references.
  void Close(RuntimeState* state);

 private:
  /// Global class references created with JniUtil.
  inline static jclass paimon_jni_scanner_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID paimon_jni_scanner_ctor_ = nullptr;
  inline static jmethodID paimon_jni_scanner_scan_table_ = nullptr;
  inline static jmethodID paimon_jni_scanner_get_next_ = nullptr;
  inline static jmethodID paimon_jni_scanner_close_ = nullptr;

  /// The Paimon table scan parameters.
  const std::string& paimon_scan_param_;
  /// Top level TupleDescriptor.
  const TupleDescriptor* tuple_desc_;
  /// metastore table name
  const std::string& table_name_;
  /// Paimon scanner Java object, it helps preparing the  table and
  /// executes an Paimon table scan. Allows the ScanNode to fetch the row batch from
  /// the Java Off Heap.
  jobject j_jni_scanner_;

  std::string DebugString();
};
} // namespace impala
