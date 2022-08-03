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

#ifndef IMPALA_EXEC_HBASE_TABLE_WRITER_H
#define IMPALA_EXEC_HBASE_TABLE_WRITER_H

#include <jni.h>
#include <boost/scoped_ptr.hpp>
#include <vector>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/descriptors.h"
#include "runtime/hbase-table.h"

namespace impala {

class RowBatch;

/// Class to write RowBatches to an HBase table using the java HTable client.
/// This class should only be called from a single sink and should not be
/// shared.
/// Sample usage (Must happen in order):
///    HBaseTableWriter::InitJni();
///    writer = new HBaseTableWriter(state, table_desc_, output_exprs_);
///    writer.Init(state);
///    writer.AppendRows(batch);
class HBaseTableWriter {
 public:
  HBaseTableWriter(HBaseTableDescriptor* table_desc,
                   const std::vector<ScalarExprEvaluator*>& output_expr_evals,
                   RuntimeProfile* profile);
  Status AppendRows(RowBatch* batch);

  /// Calls to Close release the HBaseTable.
  void Close(RuntimeState* state);

  /// Create all needed java side objects.
  /// This call may cause connections to HBase and Zookeeper to be created.
  Status Init(RuntimeState* state);

  /// Grab all of the Java classes needed to get data into and out of HBase.
  static Status InitJNI();

 private:
  /// Methods used to create JNI objects.
  /// Create a Put using the supplied row key
  Status CreatePut(JNIEnv* env, const void* rk, int rk_len, jobject* put);

  /// Create a byte array containing the string's chars.
  Status CreateByteArray(JNIEnv* env, const std::string& s,
                         jbyteArray* j_array);

  /// Create a byte array containing the input bytes.
  Status CreateByteArray(JNIEnv* env, const void* data, int data_len,
                         jbyteArray* j_array);

  /// Create an ArrayList<Put> to be passed to put.put(list);
  /// Should be used like:
  ///    CreatePutList(env, limit);
  Status CreatePutList(JNIEnv* env, int num_puts);

  /// Clean up the jni global ref in put_list_, allowing the jni to garbage
  /// collect all of the puts that are created by a writer.
  Status CleanUpJni();

  /// Owned by RuntimeState not by this object
  HBaseTableDescriptor* table_desc_;

  /// The wrapper around a Java HTable.
  /// This instance is owned by this object and must be cleaned
  /// up using close before the table can be discarded.
  boost::scoped_ptr<HBaseTable> table_;

  /// Contains the byte size of output_expr_evals_[i]->root()'s type.
  std::vector<int> output_exprs_byte_sizes_;

  /// Reference to the evaluators of expressions which generate the output value.
  /// The evaluators are owned by the sink which owns this table writer.
  const std::vector<ScalarExprEvaluator*>& output_expr_evals_;

  /// jni ArrayList<Put>
  jobject put_list_;

  /// org.apache.hadoop.hbase.client.Put
  static jclass put_cl_;

  /// new Put(byte[])
  static jmethodID put_ctor_;

  /// Put#addColumn(byte[], byte[], byte[])
  static jmethodID put_addcolumn_id_;

  /// java.util.ArrayList
  static jclass list_cl_;

  /// new ArrayList<V>(starting_capacity)
  static jmethodID list_ctor_;

  /// ArrayList#add(V);
  static jmethodID list_add_id_;

  /// cf_arrays_[i-1] is the column family jbyteArray for column i.
  std::vector<jbyteArray> cf_arrays_;

  /// qual_arrays_[i-1] is the column family qualifier jbyteArray for column i.
  std::vector<jbyteArray> qual_arrays_;

  /// Parent table sink's profile
  RuntimeProfile* runtime_profile_;
  RuntimeProfile::Counter* encoding_timer_;
  RuntimeProfile::Counter* htable_put_timer_;
};

}  // namespace impala
#endif
