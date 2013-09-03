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

#include "runtime/hbase-table.h"

#include <string>

#include "runtime/runtime-state.h"
#include "util/jni-util.h"

using namespace boost;
using namespace std;

namespace impala {

jclass HBaseTable::htable_cl_ = NULL;
jmethodID HBaseTable::htable_ctor_ = NULL;
jmethodID HBaseTable::htable_close_id_ = NULL;
jmethodID HBaseTable::htable_get_scanner_id_ = NULL;
jmethodID HBaseTable::htable_put_id_ = NULL;

jclass HBaseTable::bytes_cl_ = NULL;
jmethodID HBaseTable::bytes_to_bytes_id_ = NULL;

HBaseTable::HBaseTable(const string& table_name,
                       jobject& conf, jobject& executor )
    : table_name_(table_name),
      conf_(conf),
      executor_(executor),
      htable_(NULL) {
}

HBaseTable::~HBaseTable() {
  DCHECK(htable_ == NULL) << "Must call Close()";
}

void HBaseTable::Close(RuntimeState* state) {
  // If this has already been closed then return out early.
  if (htable_ == NULL) return;

  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    state->LogError("HBaseTable::Close(): Error creating JNIEnv");
  } else {
    env->CallObjectMethod(htable_, htable_close_id_);
    state->LogError(JniUtil::GetJniExceptionMsg(env, "HBaseTable::Close(): "));
    env->DeleteGlobalRef(htable_);
    state->LogError(JniUtil::GetJniExceptionMsg(env, "HBaseTable::Close(): "));
  }

  htable_ = NULL;
}

Status HBaseTable::Init() {
  JNIEnv* env = getJNIEnv();
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  if (env == NULL) return Status("Error creating JNIEnv");

  // Get the Java string for the table name
  jstring jtable_name_string = env->NewStringUTF(table_name_.c_str());
  RETURN_ERROR_IF_EXC(env);
  // Use o.a.h.hbase.util.Bytes.toBytes to convert into a byte array.
  jobject jtable_name = env->CallStaticObjectMethod(bytes_cl_,
      bytes_to_bytes_id_, jtable_name_string);
  RETURN_ERROR_IF_EXC(env);

  // Create the HTable.
  jobject local_htable = env->NewObject(htable_cl_,
      htable_ctor_, conf_, jtable_name, executor_);
  RETURN_ERROR_IF_EXC(env);

  // Make sure the GC doesn't remove the HTable until told to.
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_htable, &htable_));
  return Status::OK;
}

Status HBaseTable::InitJNI() {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // Bytes used to get String -> Java Byte Array the same way HBase will.
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/util/Bytes",
          &bytes_cl_));

  bytes_to_bytes_id_ = env->GetStaticMethodID(bytes_cl_, "toBytes",
      "(Ljava/lang/String;)[B");
  RETURN_ERROR_IF_EXC(env);

  // HTable
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/HTable",
          &htable_cl_));

  htable_ctor_ = env->GetMethodID(htable_cl_, "<init>",
      "(Lorg/apache/hadoop/conf/Configuration;"
      "[BLjava/util/concurrent/ExecutorService;)V");
  RETURN_ERROR_IF_EXC(env);

  htable_close_id_ = env->GetMethodID(htable_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env);

  htable_get_scanner_id_ = env->GetMethodID(htable_cl_, "getScanner",
      "(Lorg/apache/hadoop/hbase/client/Scan;)"
      "Lorg/apache/hadoop/hbase/client/ResultScanner;");
  RETURN_ERROR_IF_EXC(env);

  htable_put_id_ = env->GetMethodID(htable_cl_, "put",
      "(Ljava/util/List;)V");
  RETURN_ERROR_IF_EXC(env);

  return Status::OK;
}

Status HBaseTable::GetResultScanner(const jobject& scan,
                                    jobject* result_scanner) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");

  (*result_scanner) = env->CallObjectMethod(htable_,
      htable_get_scanner_id_, scan);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK;
}

Status HBaseTable::Put(const jobject& puts_list) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");

  env->CallObjectMethod(htable_, htable_put_id_, puts_list);
  RETURN_ERROR_IF_EXC(env);

  // TODO(eclark): FlushCommits
  return Status::OK;
}

}  // namespace impala
