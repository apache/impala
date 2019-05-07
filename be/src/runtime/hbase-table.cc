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

#include "runtime/hbase-table.h"

#include <string>

#include "runtime/runtime-state.h"
#include "util/jni-util.h"

#include "common/names.h"

namespace impala {

jclass HBaseTable::table_cl_ = NULL;
jmethodID HBaseTable::table_close_id_ = NULL;
jmethodID HBaseTable::table_get_scanner_id_ = NULL;
jmethodID HBaseTable::table_put_id_ = NULL;

jclass HBaseTable::connection_cl_ = NULL;
jmethodID HBaseTable::connection_get_table_id_ = NULL;

jclass HBaseTable::table_name_cl_ = NULL;
jmethodID HBaseTable::table_name_value_of_id_ = NULL;

HBaseTable::HBaseTable(const string& table_name, jobject connection)
    : table_name_(table_name),
      connection_(connection),
      table_(NULL) {
}

HBaseTable::~HBaseTable() {
  DCHECK(table_ == NULL) << "Must call Close()";
}

void HBaseTable::Close(RuntimeState* state) {
  // If this has already been closed then return out early.
  if (table_ == NULL) return;

  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    state->LogError(ErrorMsg(
        TErrorCode::GENERAL, "HBaseTable::Close(): Error creating JNIEnv"));
  } else {
    env->CallObjectMethod(table_, table_close_id_);
    Status s = JniUtil::GetJniExceptionMsg(env, true, "HBaseTable::Close(): ");
    if (!s.ok()) state->LogError(s.msg());
    env->DeleteGlobalRef(table_);
  }

  table_ = NULL;
}

Status HBaseTable::Init() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  if (env == NULL) return Status("Error creating JNIEnv");

  // Get a TableName object from the table name
  jstring jtable_name_string = env->NewStringUTF(table_name_.c_str());
  RETURN_ERROR_IF_EXC(env);

  // Convert into a TableName object
  jobject jtable_name = env->CallStaticObjectMethod(table_name_cl_,
      table_name_value_of_id_, jtable_name_string);
  RETURN_ERROR_IF_EXC(env);

  // Get a Table from the Connection.
  jobject local_table = env->CallObjectMethod(connection_, connection_get_table_id_,
      jtable_name);
  RETURN_ERROR_IF_EXC(env);

  // Make sure the GC doesn't remove the Table until told to. All local refs
  // will be deleted when the JniLocalFrame goes out of scope.
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_table, &table_));
  return Status::OK();
}

Status HBaseTable::InitJNI() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // TableName
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/TableName",
          &table_name_cl_));

  table_name_value_of_id_ = env->GetStaticMethodID(table_name_cl_, "valueOf",
      "(Ljava/lang/String;)Lorg/apache/hadoop/hbase/TableName;");
  RETURN_ERROR_IF_EXC(env);

  // Table
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Table",
          &table_cl_));

  table_close_id_ = env->GetMethodID(table_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env);

  table_get_scanner_id_ = env->GetMethodID(table_cl_, "getScanner",
      "(Lorg/apache/hadoop/hbase/client/Scan;)"
      "Lorg/apache/hadoop/hbase/client/ResultScanner;");
  RETURN_ERROR_IF_EXC(env);

  table_put_id_ = env->GetMethodID(table_cl_, "put", "(Ljava/util/List;)V");
  RETURN_ERROR_IF_EXC(env);

  // Connection
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env,
        "org/apache/hadoop/hbase/client/Connection", &connection_cl_));

  connection_get_table_id_= env->GetMethodID(connection_cl_, "getTable",
      "(Lorg/apache/hadoop/hbase/TableName;)"
      "Lorg/apache/hadoop/hbase/client/Table;");
  RETURN_ERROR_IF_EXC(env);

  return Status::OK();
}

Status HBaseTable::GetResultScanner(const jobject& scan,
                                    jobject* result_scanner) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");

  (*result_scanner) = env->CallObjectMethod(table_,
      table_get_scanner_id_, scan);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status HBaseTable::Put(const jobject& puts_list) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");

  env->CallObjectMethod(table_, table_put_id_, puts_list);
  RETURN_ERROR_IF_EXC(env);

  // TODO(eclark): FlushCommits
  return Status::OK();
}

}  // namespace impala
