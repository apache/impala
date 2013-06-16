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

#include "exec/hbase-table-writer.h"

#include <boost/scoped_array.hpp>
#include <sstream>

#include "runtime/hbase-table-factory.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "exprs/expr.h"
#include "runtime/raw-value.h"

using namespace boost;
using namespace std;

namespace impala {

jclass HBaseTableWriter::put_cl_ = NULL;
jclass HBaseTableWriter::list_cl_ = NULL;

jmethodID HBaseTableWriter::put_ctor_ = NULL;
jmethodID HBaseTableWriter::list_ctor_ = NULL;
jmethodID HBaseTableWriter::list_add_id_ = NULL;

jmethodID HBaseTableWriter::put_add_id_ = NULL;

HBaseTableWriter::HBaseTableWriter(HBaseTableDescriptor* table_desc,
                                   const vector<Expr*>& output_exprs)
    : table_desc_(table_desc),
      table_(NULL),
      output_exprs_(output_exprs),
      put_list_(NULL) {
};

Status HBaseTableWriter::Init(RuntimeState* state) {
  RETURN_IF_ERROR(state->htable_factory()->GetTable(table_desc_->name(),
      &table_));
  return Status::OK;
}

Status HBaseTableWriter::InitJNI() {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Error getting JNIEnv.");
  }

  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(
          env, "org/apache/hadoop/hbase/client/Put", &put_cl_));
  RETURN_ERROR_IF_EXC(env);
  put_ctor_ = env->GetMethodID(put_cl_, "<init>", "([B)V");
  RETURN_ERROR_IF_EXC(env);
  put_add_id_ = env->GetMethodID(put_cl_, "add",
    "([B[B[B)Lorg/apache/hadoop/hbase/client/Put;");
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "java/util/ArrayList", &list_cl_));
  list_ctor_ = env->GetMethodID(list_cl_, "<init>", "(I)V");
  RETURN_ERROR_IF_EXC(env);
  list_add_id_ = env->GetMethodID(list_cl_, "add", "(Ljava/lang/Object;)Z");
  RETURN_ERROR_IF_EXC(env);

  return Status::OK;
}

Status HBaseTableWriter::AppendRowBatch(RowBatch* batch) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Error getting JNIEnv.");
  }

  int limit = batch->num_rows();
  int num_cols = table_desc_->num_cols();
  if (num_cols < 2) {
    return Status("HBase tables must contain at least"
        " one column in addition to the row key.");
  }
  if (limit == 0) return Status::OK;

  // Create the array list.
  RETURN_IF_ERROR(CreatePutList(env, limit));

  scoped_array<jobject> puts(new jobject[limit]);
  // For every TupleRow in the row batch create a put, assign the row key,
  // and add all of the values generated from the expressions.
  for (int idx_batch = 0; idx_batch < limit; idx_batch++) {
    TupleRow* current_row = batch->GetRow(idx_batch);
    jobject put = NULL;

    if (output_exprs_[0]->GetValue(current_row) == NULL) {
      // HBase row key must not be null.
      return Status("Cannot insert into HBase with a null row key.");
    }

    for (int j = 0; j < num_cols; j++) {
      void* value = output_exprs_[j]->GetValue(current_row);
      if (value != NULL) {
        stringstream col_stringstream;
        output_exprs_[j]->PrintValue(value, &col_stringstream);
        if (j == 0) {
          DCHECK(put == NULL);
          RETURN_IF_ERROR(CreatePut(env, col_stringstream.str(), &put));

        } else {
          DCHECK(put != NULL) << "Put shouldn't be NULL for non-key cols.";

          const string value = col_stringstream.str();
          const pair<string, string>& col = table_desc_->cols()[j];

          jbyteArray cf_array;
          RETURN_IF_ERROR(CreateByteArray(env, col.first, &cf_array));

          jbyteArray qual_array;
          RETURN_IF_ERROR(CreateByteArray(env, col.second, &qual_array));

          jbyteArray val_array;
          RETURN_IF_ERROR(CreateByteArray(env, value, &val_array));

          env->CallObjectMethod(put, put_add_id_, cf_array, qual_array,
              val_array);
          RETURN_ERROR_IF_EXC(env);

          // Clean up the local references.
          env->DeleteLocalRef(cf_array);
          RETURN_ERROR_IF_EXC(env);
          env->DeleteLocalRef(qual_array);
          RETURN_ERROR_IF_EXC(env);
          env->DeleteLocalRef(val_array);
          RETURN_ERROR_IF_EXC(env);
        }
      }
    }

    DCHECK(put != NULL);
    env->DeleteLocalRef(put);
    RETURN_ERROR_IF_EXC(env);
  }

  // Send the array list to HTable.
  RETURN_IF_ERROR(table_->Put(put_list_));

  // Now clean things up.
  RETURN_IF_ERROR(CleanJNI());
  return Status::OK;
}

Status HBaseTableWriter::CleanJNI() {
  if (put_list_ == NULL) return Status::OK;

  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Error getting JNIEnv.");
  }

  env->DeleteGlobalRef(put_list_);
  RETURN_ERROR_IF_EXC(env);
  put_list_ = NULL;
  return Status::OK;
}

Status HBaseTableWriter::CreatePutList(JNIEnv* env, int num_puts) {
  jobject local_put_list = env->NewObject(list_cl_, list_ctor_, num_puts);
  RETURN_ERROR_IF_EXC(env);

  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_put_list, &put_list_));

  return Status::OK;
}

Status HBaseTableWriter::CreatePut(JNIEnv* env, const string& rk,
                                   jobject* put) {
  // Create the row key byte array.
  jbyteArray rk_array;
  RETURN_IF_ERROR(CreateByteArray(env, rk, &rk_array));

  (*put) = env->NewObject(put_cl_, put_ctor_, rk_array);
  RETURN_ERROR_IF_EXC(env);

  // Add the put to the list.
  env->CallObjectMethod(put_list_, list_add_id_, *put);
  RETURN_ERROR_IF_EXC(env);

  env->DeleteLocalRef(rk_array);
  RETURN_ERROR_IF_EXC(env);

  return Status::OK;
}

Status HBaseTableWriter::CreateByteArray(JNIEnv* env, const string& s,
                                         jbyteArray* j_array) {
  int s_len = static_cast<jsize>(s.size());
  // Create the byte array.
  (*j_array) = env->NewByteArray(s_len);
  RETURN_ERROR_IF_EXC(env);
  // Copy in the chars backing the std::string
  env->SetByteArrayRegion((*j_array), 0, s_len,
      reinterpret_cast<const jbyte*>(s.data()));
  RETURN_ERROR_IF_EXC(env);

  return Status::OK;
}

Status HBaseTableWriter::Close(RuntimeState* state) {
  // Guard against double closing.
  if (table_.get() != NULL) {
    RETURN_IF_ERROR(table_->Close());
    table_.reset(NULL);
  }

  // The jni should already have everything cleaned at this point
  // but try again just in case there was an error that caused
  // AppendRowBatch to exit out before calling CleanJNI.
  RETURN_IF_ERROR(CleanJNI());
  return Status::OK;
}

}  // namespace impala
