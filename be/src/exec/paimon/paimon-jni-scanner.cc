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

#include "exec/paimon/paimon-jni-scanner.h"
#include <jni.h>
#include "util/jni-util.h"

namespace impala {

PaimonJniScanner::PaimonJniScanner(const std::string& scan_param,
    const TupleDescriptor* tuple_desc, const std::string& table_name)
  : paimon_scan_param_(scan_param), tuple_desc_(tuple_desc), table_name_(table_name) {}

Status PaimonJniScanner::InitJNI() {
  DCHECK(paimon_jni_scanner_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");

  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(
      env, "org/apache/impala/util/paimon/PaimonJniScanner", &paimon_jni_scanner_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(
      env, paimon_jni_scanner_cl_, "<init>", "([B)V", &paimon_jni_scanner_ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(
      env, paimon_jni_scanner_cl_, "ScanTable", "()V", &paimon_jni_scanner_scan_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, paimon_jni_scanner_cl_, "GetNextBatch",
      "([J)J", &paimon_jni_scanner_get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(
      env, paimon_jni_scanner_cl_, "close", "()V", &paimon_jni_scanner_close_));

  return Status::OK();
}

Status PaimonJniScanner::Init(JNIEnv* env) {
  jbyteArray jbytes_scan_param = env->NewByteArray(paimon_scan_param_.size());
  RETURN_ERROR_IF_EXC(env);
  env->SetByteArrayRegion(jbytes_scan_param, 0, paimon_scan_param_.size(),
      reinterpret_cast<const jbyte*>(paimon_scan_param_.data()));
  RETURN_ERROR_IF_EXC(env);
  jobject j_jni_scanner =
      env->NewObject(paimon_jni_scanner_cl_, paimon_jni_scanner_ctor_, jbytes_scan_param);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, j_jni_scanner, &j_jni_scanner_));
  return Status::OK();
}

Status PaimonJniScanner::ScanTable(JNIEnv* env) {
  env->CallObjectMethod(j_jni_scanner_, paimon_jni_scanner_scan_table_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status PaimonJniScanner::GetNextBatchDirect(JNIEnv* env, struct ArrowArray** array,
    struct ArrowSchema** schema, long* rows, long* offheap_used) {
  /// Will pass a long array to java method, and the returned are two memory address.
  /// 1st is the schema memory address, the second is the memory address of arrow
  /// array vector. the two memory address are in the offheap region of JVM,
  /// and the offheap usage in bytes.
  jlongArray address_array = env->NewLongArray(3);
  RETURN_ERROR_IF_EXC(env);
  jlong result =
      env->CallLongMethod(j_jni_scanner_, paimon_jni_scanner_get_next_, address_array);
  RETURN_ERROR_IF_EXC(env);
  jlong values[3];
  env->GetLongArrayRegion(address_array, 0, 3, &values[0]);
  *schema = (struct ArrowSchema*)values[0];
  *array = (struct ArrowArray*)values[1];
  *offheap_used = values[2];
  *rows = result;
  env->DeleteLocalRef(address_array);
  return Status::OK();
}

void PaimonJniScanner::Close(RuntimeState* state) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env != nullptr) {
    if (j_jni_scanner_ != nullptr) {
      /// Call close method to free resources of java PaimonJniScanner.
      env->CallObjectMethod(j_jni_scanner_, paimon_jni_scanner_close_);
      env->DeleteGlobalRef(j_jni_scanner_);
    }
  }
}

string PaimonJniScanner::DebugString() {
  std::stringstream out;
  out << "PaimonJniScanner: [ Paimon table name: " << table_name_ << "; ";
  out << tuple_desc_->DebugString() << "]";
  return out.str();
}

} // namespace impala
