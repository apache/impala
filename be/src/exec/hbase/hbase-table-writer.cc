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

#include "exec/hbase/hbase-table-writer.h"

#include <boost/scoped_array.hpp>
#include <sstream>

#include "common/logging.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/exec-env.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"
#include "util/coding-util.h"
#include "util/jni-util.h"

#include "common/names.h"

namespace impala {

jclass HBaseTableWriter::put_cl_ = NULL;
jclass HBaseTableWriter::list_cl_ = NULL;

jmethodID HBaseTableWriter::put_ctor_ = NULL;
jmethodID HBaseTableWriter::list_ctor_ = NULL;
jmethodID HBaseTableWriter::list_add_id_ = NULL;

jmethodID HBaseTableWriter::put_addcolumn_id_ = NULL;

HBaseTableWriter::HBaseTableWriter(HBaseTableDescriptor* table_desc,
    const vector<ScalarExprEvaluator*>& output_expr_evals, RuntimeProfile* profile)
  : table_desc_(table_desc),
    table_(NULL),
    output_expr_evals_(output_expr_evals),
    put_list_(NULL),
    runtime_profile_(profile) { }

Status HBaseTableWriter::Init(RuntimeState* state) {
  RETURN_IF_ERROR(ExecEnv::GetInstance()->htable_factory()->GetTable(
      table_desc_->table_name(), &table_));
  encoding_timer_ = ADD_TIMER(runtime_profile_, "EncodingTimer");
  htable_put_timer_ = ADD_TIMER(runtime_profile_, "HTablePutTimer");

  int num_col = table_desc_->num_cols();
  if (num_col < 2) {
    return Status("HBase tables must contain at least"
        " one column in addition to the row key.");
  }

  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error getting JNIEnv.");
  output_exprs_byte_sizes_.resize(num_col);
  cf_arrays_.reserve(num_col - 1);
  qual_arrays_.reserve(num_col - 1);
  for (int i = 0; i < num_col; ++i) {
    output_exprs_byte_sizes_[i] =
        output_expr_evals_[i]->root().type().GetByteSize();

    if (i == 0) continue;

    // Setup column family and qualifier byte array for non-rowkey column
    const HBaseTableDescriptor::HBaseColumnDescriptor& col = table_desc_->cols()[i];
    jbyteArray byte_array;
    jbyteArray global_ref;
    RETURN_IF_ERROR(CreateByteArray(env, col.family, &byte_array));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, byte_array, &global_ref));
    cf_arrays_.push_back(global_ref);
    RETURN_IF_ERROR(CreateByteArray(env, col.qualifier, &byte_array));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, byte_array, &global_ref));
    qual_arrays_.push_back(global_ref);
  }

  return Status::OK();
}

Status HBaseTableWriter::InitJNI() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error getting JNIEnv.");

  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(
          env, "org/apache/hadoop/hbase/client/Put", &put_cl_));
  RETURN_ERROR_IF_EXC(env);
  put_ctor_ = env->GetMethodID(put_cl_, "<init>", "([B)V");
  RETURN_ERROR_IF_EXC(env);
  put_addcolumn_id_ = env->GetMethodID(put_cl_, "addColumn",
    "([B[B[B)Lorg/apache/hadoop/hbase/client/Put;");
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "java/util/ArrayList", &list_cl_));
  list_ctor_ = env->GetMethodID(list_cl_, "<init>", "(I)V");
  RETURN_ERROR_IF_EXC(env);
  list_add_id_ = env->GetMethodID(list_cl_, "add", "(Ljava/lang/Object;)Z");
  RETURN_ERROR_IF_EXC(env);

  return Status::OK();
}

Status HBaseTableWriter::AppendRows(RowBatch* batch) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error getting JNIEnv.");

  int limit = batch->num_rows();
  if (limit == 0) return Status::OK();
  int num_cols = table_desc_->num_cols();
  DCHECK_GE(num_cols, 2);

  // Create the array list.
  RETURN_IF_ERROR(CreatePutList(env, limit));

  // For every TupleRow in the row batch create a put, assign the row key,
  // and add all of the values generated from the expressions.
  string string_value; // text encoded value
  string base64_encoded_value; // needed for BINARY columns
  char binary_value[8]; // binary encoded value; at most 8 bytes
  const void* data; // pointer to the column value in bytes
  int data_len; // length of the column value in bytes
  {
    SCOPED_TIMER(encoding_timer_);
    for (int idx_batch = 0; idx_batch < limit; idx_batch++) {
      TupleRow* current_row = batch->GetRow(idx_batch);
      jobject put = NULL;

      if (output_expr_evals_[0]->GetValue(current_row) == NULL) {
        // HBase row key must not be null.
        return Status("Cannot insert into HBase with a null row key.");
      }

      for (int j = 0; j < num_cols; j++) {
        const HBaseTableDescriptor::HBaseColumnDescriptor& col = table_desc_->cols()[j];
        void* value = output_expr_evals_[j]->GetValue(current_row);

        if (value != NULL) {
          if (!col.binary_encoded) {
            // Text encoded
            string_value.clear();
            output_expr_evals_[j]->PrintValue(value, &string_value);
            const ColumnDescriptor& col_desc = table_desc_->col_descs()[j];
            if (col_desc.type().IsBinaryType()) {
              Base64Encode(string_value , &base64_encoded_value);
              data = base64_encoded_value.data();
              data_len = base64_encoded_value.length();
            } else {
              data = string_value.data();
              data_len = string_value.length();
            }
          } else {
            // Binary encoded
            // Only bool, tinyint, smallint, int, bigint, float and double can be binary
            // encoded. Convert the value to big-endian.
            data = binary_value;
            data_len = output_exprs_byte_sizes_[j];
            DCHECK(data_len == 1 || data_len == 2 || data_len == 4 || data_len == 8)
              << data_len;
            BitUtil::ByteSwap(binary_value, value, data_len);
          }

          if (j == 0) {
            DCHECK(put == NULL);
            RETURN_IF_ERROR(CreatePut(env, data, data_len, &put));
          } else {
            DCHECK(put != NULL) << "Put shouldn't be NULL for non-key cols.";
            jbyteArray val_array;
            RETURN_IF_ERROR(CreateByteArray(env, data, data_len, &val_array));
            env->CallObjectMethod(put, put_addcolumn_id_, cf_arrays_[j-1],
                qual_arrays_[j-1], val_array);
            RETURN_ERROR_IF_EXC(env);

            // Clean up the local references.
            env->DeleteLocalRef(val_array);
            RETURN_ERROR_IF_EXC(env);
          }
        }
      }

      DCHECK(put != NULL);
      env->DeleteLocalRef(put);
      RETURN_ERROR_IF_EXC(env);
    }
  }

  // Send the array list to HTable.
  {
    SCOPED_TIMER(htable_put_timer_);
    RETURN_IF_ERROR(table_->Put(put_list_));
  }
  // Now clean put_list_.
  env->DeleteGlobalRef(put_list_);
  put_list_ = NULL;
  return Status::OK();
}

Status HBaseTableWriter::CleanUpJni() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error getting JNIEnv.");

  if (put_list_ != NULL) {
    env->DeleteGlobalRef(put_list_);
    put_list_ = NULL;
  }

  for (jbyteArray ref: cf_arrays_) env->DeleteGlobalRef(ref);
  for (jbyteArray ref: qual_arrays_) env->DeleteGlobalRef(ref);
  return Status::OK();
}

Status HBaseTableWriter::CreatePutList(JNIEnv* env, int num_puts) {
  DCHECK(put_list_ == NULL);
  jobject local_put_list = env->NewObject(list_cl_, list_ctor_, num_puts);
  RETURN_ERROR_IF_EXC(env);

  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_put_list, &put_list_));

  return Status::OK();
}

Status HBaseTableWriter::CreatePut(JNIEnv* env, const void* rk, int rk_len,
                                   jobject* put) {
  // Create the row key byte array.
  jbyteArray rk_array;
  RETURN_IF_ERROR(CreateByteArray(env, rk, rk_len, &rk_array));

  (*put) = env->NewObject(put_cl_, put_ctor_, rk_array);
  RETURN_ERROR_IF_EXC(env);

  // Add the put to the list.
  env->CallObjectMethod(put_list_, list_add_id_, *put);
  RETURN_ERROR_IF_EXC(env);

  env->DeleteLocalRef(rk_array);
  RETURN_ERROR_IF_EXC(env);

  return Status::OK();
}

Status HBaseTableWriter::CreateByteArray(JNIEnv* env, const string& s,
                                         jbyteArray* j_array) {
  int s_len = s.size();
  return CreateByteArray(env, s.data(), s_len, j_array);
}

Status HBaseTableWriter::CreateByteArray(JNIEnv* env, const void* data, int data_len,
    jbyteArray* j_array) {
  (*j_array) = env->NewByteArray(data_len);
  RETURN_ERROR_IF_EXC(env);
  env->SetByteArrayRegion((*j_array), 0, data_len, reinterpret_cast<const jbyte*>(data));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

void HBaseTableWriter::Close(RuntimeState* state) {
  // Guard against double closing.
  if (table_.get() != NULL) {
    table_->Close(state);
    table_.reset();
  }

  // The jni should already have everything cleaned at this point but try again just in
  // case there was an error that caused AppendRows() to exit out before calling
  // CleanUpJni.
  Status status = CleanUpJni();
  if (!status.ok()) {
    stringstream ss;
    ss << "HBaseTableWriter::Close ran into an issue: " << status.GetDetail();
    state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
  }
}

}  // namespace impala
