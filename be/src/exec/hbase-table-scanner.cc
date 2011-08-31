// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "hbase-table-scanner.h"
#include <cstring>
#include "util/jni-util.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"

using namespace std;
using namespace impala;

jclass HBaseTableScanner::htable_cl_ = NULL;
jclass HBaseTableScanner::scan_cl_ = NULL;
jclass HBaseTableScanner::resultscanner_cl_ = NULL;
jclass HBaseTableScanner::result_cl_ = NULL;
jclass HBaseTableScanner::immutable_bytes_writable_cl_ = NULL;
jclass HBaseTableScanner::keyvalue_cl_ = NULL;
jclass HBaseTableScanner::hconstants_cl_ = NULL;
jmethodID HBaseTableScanner::htable_ctor_ = NULL;
jmethodID HBaseTableScanner::htable_get_scanner_id_ = NULL;
jmethodID HBaseTableScanner::htable_close_id_ = NULL;
jmethodID HBaseTableScanner::scan_ctor_ = NULL;
jmethodID HBaseTableScanner::scan_set_max_versions_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_caching_id_ = NULL;
jmethodID HBaseTableScanner::scan_add_column_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_next_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_close_id_ = NULL;
jmethodID HBaseTableScanner::result_get_bytes_id_ = NULL;
jmethodID HBaseTableScanner::result_raw_id_ = NULL;
jmethodID HBaseTableScanner::immutable_bytes_writable_get_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_buffer_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_family_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_family_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_qualifier_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_qualifier_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_row_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_row_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_value_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_value_length_id_ = NULL;
jobject HBaseTableScanner::empty_row_ = NULL;

HBaseTableScanner::HBaseTableScanner(JNIEnv* env)
  : env_(env),
    hbase_conf_(NULL),
    htable_(NULL),
    scan_(NULL),
    resultscanner_(NULL),
    result_(NULL),
    keyvalues_(NULL),
    keyvalue_(NULL),
    byte_array_(NULL),
    buffer_(NULL),
    keyvalue_index_(0),
    num_requested_keyvalues_(0),
    num_keyvalues_(0),
    all_keyvalues_present_(false),
    value_pool_(new MemPool()),
    rows_cached_(DEFAULT_ROWS_CACHED) {
}

Status HBaseTableScanner::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // Global class references:
  // HTable, Scan, ResultScanner, Result, ImmutableBytesWritable, KeyValue, HConstants.
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/HTable", &htable_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Scan", &scan_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/ResultScanner",
          &resultscanner_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Result", &result_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/io/ImmutableBytesWritable",
          &immutable_bytes_writable_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/KeyValue", &keyvalue_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/HConstants", &hconstants_cl_));

  // HTable method ids.
  htable_ctor_ = env->GetMethodID(htable_cl_, "<init>",
      "(Lorg/apache/hadoop/conf/Configuration;Ljava/lang/String;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  htable_get_scanner_id_ = env->GetMethodID(htable_cl_, "getScanner",
      "(Lorg/apache/hadoop/hbase/client/Scan;)Lorg/apache/hadoop/hbase/client/ResultScanner;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  htable_close_id_ = env->GetMethodID(htable_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Scan method ids.
  scan_ctor_ = env->GetMethodID(scan_cl_, "<init>", "([B[B)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_max_versions_id_ = env->GetMethodID(scan_cl_, "setMaxVersions",
      "(I)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_caching_id_ = env->GetMethodID(scan_cl_, "setCaching", "(I)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_add_column_id_ = env->GetMethodID(scan_cl_, "addColumn",
      "([B[B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // ResultScanner method ids.
  resultscanner_next_id_ = env->GetMethodID(resultscanner_cl_, "next",
      "()Lorg/apache/hadoop/hbase/client/Result;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  resultscanner_close_id_ = env->GetMethodID(resultscanner_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Result method ids.
  result_raw_id_ = env->GetMethodID(result_cl_, "raw", "()[Lorg/apache/hadoop/hbase/KeyValue;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  result_get_bytes_id_ = env->GetMethodID(result_cl_, "getBytes",
      "()Lorg/apache/hadoop/hbase/io/ImmutableBytesWritable;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // ImmutableBytesWritable
  immutable_bytes_writable_get_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "get", "()[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // KeyValue method ids.
  keyvalue_get_buffer_id_ = env->GetMethodID(keyvalue_cl_, "getBuffer", "()[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_family_offset_id_ = env->GetMethodID(keyvalue_cl_, "getFamilyOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_family_length_id_ = env->GetMethodID(keyvalue_cl_, "getFamilyLength", "()B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_qualifier_offset_id_ = env->GetMethodID(keyvalue_cl_, "getQualifierOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_qualifier_length_id_ = env->GetMethodID(keyvalue_cl_, "getQualifierLength", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_row_offset_id_ = env->GetMethodID(keyvalue_cl_, "getRowOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_row_length_id_ = env->GetMethodID(keyvalue_cl_, "getRowLength", "()S");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_value_offset_id_ = env->GetMethodID(keyvalue_cl_, "getValueOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_value_length_id_ = env->GetMethodID(keyvalue_cl_, "getValueLength", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // HConstants fields
  jfieldID empty_start_row_id =
      env->GetStaticFieldID(hconstants_cl_, "EMPTY_START_ROW", "[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  empty_row_ = env->GetStaticObjectField(hconstants_cl_, empty_start_row_id);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, empty_row_, &empty_row_));

  return Status::OK;
}

Status HBaseTableScanner::StartScan(
    const TupleDescriptor* tuple_desc, const string& start_key, const string& end_key) {
  const HBaseTableDescriptor* hbase_table =
      static_cast<const HBaseTableDescriptor*>(tuple_desc->table_desc());
  // htable_ = new HTable(hbase_conf_, hbase_table->table_name());
  // TODO: Create and use global cache of HTables.
  jstring jtable_name = env_->NewStringUTF(hbase_table->table_name().c_str());
  htable_ = env_->NewObject(htable_cl_, htable_ctor_, hbase_conf_, jtable_name);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_ = new Scan(start_key, stop_key);
  jbyteArray start_bytes;
  CreateRowKey(start_key, &start_bytes);
  jbyteArray end_bytes;
  CreateRowKey(end_key, &end_bytes);
  scan_ = env_->NewObject(scan_cl_, scan_ctor_, start_bytes, end_bytes);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_.setMaxVersions(1);
  scan_ = env_->CallObjectMethod(scan_, scan_set_max_versions_id_, 1);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_.setCaching(rows_cached_);
  env_->CallObjectMethod(scan_, scan_set_caching_id_, rows_cached_);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  const vector<SlotDescriptor*>& slots = tuple_desc->slots();
  // Restrict scan to requested families/qualifiers.
  for (int i = 0; i < slots.size(); ++i) {
    const string& family = hbase_table->cols()[slots[i]->col_pos()].first;
    const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].second;
    // The row key has an empty qualifier.
    if (qualifier.empty()) continue;
    jbyteArray family_bytes = env_->NewByteArray(family.size());
    env_->SetByteArrayRegion(family_bytes, 0, family.size(),
        reinterpret_cast<const jbyte*>(family.data()));
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
    jbyteArray qualifier_bytes = env_->NewByteArray(qualifier.size());
    env_->SetByteArrayRegion(qualifier_bytes, 0, qualifier.size(),
        reinterpret_cast<const jbyte*>(qualifier.data()));
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
    // scan_.addColumn(family_bytes, qualifier_bytes);
    env_->CallObjectMethod(scan_, scan_add_column_id_, family_bytes, qualifier_bytes);
    env_->DeleteLocalRef(family_bytes);
    env_->DeleteLocalRef(qualifier_bytes);
  }
  resultscanner_ = env_->CallObjectMethod(htable_, htable_get_scanner_id_, scan_);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
  return Status::OK;
}

Status HBaseTableScanner::CreateRowKey(const std::string& key, jbyteArray* bytes) {
  if (!key.empty()) {
    *bytes = env_->NewByteArray(key.size());
    if (*bytes == NULL) {
      return Status("Couldn't construct java byte array for key " + key);
    }
    jboolean is_copy;
    jbyte* elements = env_->GetByteArrayElements(*bytes, &is_copy);
    if (elements == NULL) {
      return Status("Couldn't get java byte array elements for key " + key);
    }
    memcpy(elements, key.data(), key.size());
    env_->ReleaseByteArrayElements(*bytes, elements, 0);
  } else {
    *bytes = reinterpret_cast<jbyteArray>(empty_row_);
  }
  return Status::OK;
}

Status HBaseTableScanner::Next(bool* has_next) {
  // result_ = resultscanner_.next();
  result_ = env_->CallObjectMethod(resultscanner_, resultscanner_next_id_);
  if (result_ == NULL) {
    *has_next = false;
    return Status::OK;
  }
  // keyvalues_ = result_.raw();
  keyvalues_ = reinterpret_cast<jobjectArray>(env_->CallObjectMethod(result_, result_raw_id_));
  num_keyvalues_ = env_->GetArrayLength(keyvalues_);
  // Check that raw() didn't return more keyvalues than expected.
  // If num_requested_keyvalues_ is 0 then only row key is asked for and this check should pass.
  if (num_keyvalues_ > num_requested_keyvalues_ && num_requested_keyvalues_ != 0) {
    *has_next = false;
    return Status("Encountered more keyvalues than expected.");
  }
  // If all requested columns are present, we avoid family-/qualifier comparisons in NextValue().
  if (num_keyvalues_ == num_requested_keyvalues_) {
    all_keyvalues_present_ = true;
  } else {
    all_keyvalues_present_ = false;
  }
  keyvalue_index_ = 0;
  // All KeyValues are backed by the same buffer. Place it into the C byffer_.
  jobject immutable_bytes_writable = env_->CallObjectMethod(result_, result_get_bytes_id_);
  byte_array_ = (jbyteArray)
      env_->CallObjectMethod(immutable_bytes_writable, immutable_bytes_writable_get_id_);
  jboolean is_copy = false;
  buffer_ = env_->GetByteArrayElements(byte_array_, &is_copy);
  value_pool_->Clear();
  *has_next = true;
  return Status::OK;
}

void HBaseTableScanner::ReleaseBuffer() {
  env_->ReleaseByteArrayElements(byte_array_, reinterpret_cast<jbyte*>(buffer_), 0);
}

void HBaseTableScanner::GetRowKey(void** key, int* key_length) {
  keyvalue_ = env_->GetObjectArrayElement(keyvalues_, 0);
  *key_length = env_->CallShortMethod(keyvalue_, keyvalue_get_row_length_id_);
  int key_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_row_offset_id_);
  // Allocate one extra byte for null-terminator.
  *key = value_pool_->Allocate(*key_length + 1);
  memcpy(*key, reinterpret_cast<char*>(buffer_) + key_offset, *key_length);
  reinterpret_cast<char*>(*key)[*key_length] = '\0';
}

void HBaseTableScanner::GetValue(const string& family, const string& qualifier,
    void** value, int* value_length) {
  // Current row doesn't have any more keyvalues. All remaining values are NULL.
  if (keyvalue_index_ >= num_keyvalues_) {
    *value = NULL;
    *value_length = 0;
    return;
  }
  keyvalue_ = env_->GetObjectArrayElement(keyvalues_, keyvalue_index_);
  if (!all_keyvalues_present_) {
    // Check family. If it doesn't match, we have a NULL value.
    int family_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_family_offset_id_);
    int family_length = env_->CallByteMethod(keyvalue_, keyvalue_get_family_length_id_);
    if (CompareStrings(family, family_offset, family_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
    // Check qualifier. If it doesn't match, we have a NULL value.
    int qualifier_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_qualifier_offset_id_);
    int qualifier_length = env_->CallIntMethod(keyvalue_, keyvalue_get_qualifier_length_id_);
    if (CompareStrings(qualifier, qualifier_offset, qualifier_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
  }
  // The requested family/qualifier matches the keyvalue at keyvalue_index_. Copy the cell.
  *value_length = env_->CallIntMethod(keyvalue_, keyvalue_get_value_length_id_);
  int value_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_value_offset_id_);
  // Allocate one extra byte for null-terminator.
  *value = value_pool_->Allocate(*value_length + 1);
  memcpy(*value, reinterpret_cast<char*>(buffer_) + value_offset, *value_length);
  reinterpret_cast<char*>(*value)[*value_length] = '\0';
  ++keyvalue_index_;
}

int HBaseTableScanner::CompareStrings(const string& s, int offset, int length) {
  int slength = static_cast<int>(s.length());
  if (slength == 0 && length == 0) return 0;
  if (length == 0) return 1;
  if (slength == 0) return -1;
  int result = memcmp(s.data(), reinterpret_cast<char*>(buffer_) + offset,
      std::min(slength, length));
  if (result == 0 && slength != length) {
    return (slength < length ? -1 : 1);
  } else {
    return result;
  }
}

void HBaseTableScanner::Close() {
  if (resultscanner_ != NULL) {
    // resultscanner_.close();
    env_->CallObjectMethod(resultscanner_, resultscanner_close_id_);
  }
  if (htable_ != NULL) {
    // htable_.close();
    env_->CallObjectMethod(htable_, htable_close_id_);
  }
}
