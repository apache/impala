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

#include "exec/hbase-table-scanner.h"

#include <cstring>
#include <algorithm>

#include "util/jni-util.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"

using namespace std;
using namespace impala;

jclass HBaseTableScanner::scan_cl_ = NULL;
jclass HBaseTableScanner::resultscanner_cl_ = NULL;
jclass HBaseTableScanner::result_cl_ = NULL;
jclass HBaseTableScanner::immutable_bytes_writable_cl_ = NULL;
jclass HBaseTableScanner::keyvalue_cl_ = NULL;
jclass HBaseTableScanner::hconstants_cl_ = NULL;
jclass HBaseTableScanner::filter_list_cl_ = NULL;
jclass HBaseTableScanner::filter_list_op_cl_ = NULL;
jclass HBaseTableScanner::single_column_value_filter_cl_ = NULL;
jclass HBaseTableScanner::compare_op_cl_ = NULL;
jmethodID HBaseTableScanner::scan_ctor_ = NULL;
jmethodID HBaseTableScanner::scan_set_max_versions_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_caching_id_ = NULL;
jmethodID HBaseTableScanner::scan_add_column_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_filter_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_start_row_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_stop_row_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_next_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_close_id_ = NULL;
jmethodID HBaseTableScanner::result_get_bytes_id_ = NULL;
jmethodID HBaseTableScanner::result_raw_id_ = NULL;
jmethodID HBaseTableScanner::immutable_bytes_writable_get_id_ = NULL;
jmethodID HBaseTableScanner::immutable_bytes_writable_get_length_id_ = NULL;
jmethodID HBaseTableScanner::immutable_bytes_writable_get_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_buffer_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_family_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_family_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_qualifier_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_qualifier_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_row_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_row_length_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_value_offset_id_ = NULL;
jmethodID HBaseTableScanner::keyvalue_get_value_length_id_ = NULL;
jmethodID HBaseTableScanner::filter_list_ctor_ = NULL;
jmethodID HBaseTableScanner::filter_list_add_filter_id_ = NULL;
jmethodID HBaseTableScanner::single_column_value_filter_ctor_ = NULL;
jobject HBaseTableScanner::empty_row_ = NULL;
jobject HBaseTableScanner::must_pass_all_op_ = NULL;
jobjectArray HBaseTableScanner::compare_ops_ = NULL;

void HBaseTableScanner::ScanRange::DebugString(int indentation_level,
    stringstream* out) {
  *out << string(indentation_level * 2, ' ');
  if (!start_key_.empty()) {
    *out << " start_key=" << start_key_;
  }
  if (!stop_key_.empty()) {
    *out << " stop_key=" << stop_key_;
  }
}

HBaseTableScanner::HBaseTableScanner(
    ScanNode* scan_node, HBaseTableFactory* htable_factory, RuntimeState* state)
  : scan_node_(scan_node),
    htable_factory_(htable_factory),
    htable_(NULL),
    scan_(NULL),
    resultscanner_(NULL),
    result_(NULL),
    keyvalues_(NULL),
    keyvalue_(NULL),
    byte_array_(NULL),
    buffer_(NULL),
    buffer_length_(0),
    keyvalue_index_(0),
    num_requested_keyvalues_(0),
    num_addl_requested_cols_(0),
    num_keyvalues_(0),
    all_keyvalues_present_(false),
    value_pool_(new MemPool()),
    buffer_pool_(new MemPool()),
    rows_cached_(DEFAULT_ROWS_CACHED),
    scan_setup_timer_(ADD_TIMER(scan_node_->runtime_profile(),
      "HBaseTableScanner.ScanSetup")) {
  value_pool_->set_limits(*state->mem_limits());
  buffer_pool_->set_limits(*state->mem_limits());
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
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Scan", &scan_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/ResultScanner",
          &resultscanner_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Result",
          &result_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/io/ImmutableBytesWritable",
          &immutable_bytes_writable_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/KeyValue", &keyvalue_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/HConstants",
          &hconstants_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/filter/FilterList",
          &filter_list_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env,
          "org/apache/hadoop/hbase/filter/FilterList$Operator", &filter_list_op_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env,
          "org/apache/hadoop/hbase/filter/SingleColumnValueFilter",
          &single_column_value_filter_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env,
          "org/apache/hadoop/hbase/filter/CompareFilter$CompareOp",
          &compare_op_cl_));

  // HTable method ids.

  // Scan method ids.
  scan_ctor_ = env->GetMethodID(scan_cl_, "<init>", "()V");
  RETURN_ERROR_IF_EXC(env);
  scan_set_max_versions_id_ = env->GetMethodID(scan_cl_, "setMaxVersions",
      "(I)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  scan_set_caching_id_ = env->GetMethodID(scan_cl_, "setCaching", "(I)V");
  RETURN_ERROR_IF_EXC(env);
  scan_add_column_id_ = env->GetMethodID(scan_cl_, "addColumn",
      "([B[B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  scan_set_filter_id_ = env->GetMethodID(scan_cl_, "setFilter",
      "(Lorg/apache/hadoop/hbase/filter/Filter;)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  scan_set_start_row_id_ = env->GetMethodID(scan_cl_, "setStartRow",
      "([B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  scan_set_stop_row_id_ = env->GetMethodID(scan_cl_, "setStopRow",
      "([B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);

  // ResultScanner method ids.
  resultscanner_next_id_ = env->GetMethodID(resultscanner_cl_, "next",
      "()Lorg/apache/hadoop/hbase/client/Result;");
  RETURN_ERROR_IF_EXC(env);
  resultscanner_close_id_ = env->GetMethodID(resultscanner_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env);

  // Result method ids.
  result_raw_id_ = env->GetMethodID(result_cl_, "raw",
      "()[Lorg/apache/hadoop/hbase/KeyValue;");
  RETURN_ERROR_IF_EXC(env);
  result_get_bytes_id_ = env->GetMethodID(result_cl_, "getBytes",
      "()Lorg/apache/hadoop/hbase/io/ImmutableBytesWritable;");
  RETURN_ERROR_IF_EXC(env);

  // ImmutableBytesWritable method ids.
  immutable_bytes_writable_get_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "get", "()[B");
  RETURN_ERROR_IF_EXC(env);
  immutable_bytes_writable_get_length_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "getLength", "()I");
  RETURN_ERROR_IF_EXC(env);
  immutable_bytes_writable_get_offset_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "getOffset", "()I");
  RETURN_ERROR_IF_EXC(env);

  // KeyValue method ids.
  keyvalue_get_buffer_id_ = env->GetMethodID(keyvalue_cl_, "getBuffer", "()[B");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_family_offset_id_ =
      env->GetMethodID(keyvalue_cl_, "getFamilyOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_family_length_id_ =
      env->GetMethodID(keyvalue_cl_, "getFamilyLength", "()B");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_qualifier_offset_id_ =
      env->GetMethodID(keyvalue_cl_, "getQualifierOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_qualifier_length_id_ =
      env->GetMethodID(keyvalue_cl_, "getQualifierLength", "()I");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_row_offset_id_ = env->GetMethodID(keyvalue_cl_, "getRowOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_row_length_id_ = env->GetMethodID(keyvalue_cl_, "getRowLength", "()S");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_value_offset_id_ = env->GetMethodID(keyvalue_cl_, "getValueOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  keyvalue_get_value_length_id_ = env->GetMethodID(keyvalue_cl_, "getValueLength", "()I");
  RETURN_ERROR_IF_EXC(env);

  // HConstants fields.
  jfieldID empty_start_row_id =
      env->GetStaticFieldID(hconstants_cl_, "EMPTY_START_ROW", "[B");
  RETURN_ERROR_IF_EXC(env);
  empty_row_ = env->GetStaticObjectField(hconstants_cl_, empty_start_row_id);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, empty_row_, &empty_row_));

  // FilterList method ids.
  filter_list_ctor_ = env->GetMethodID(filter_list_cl_, "<init>",
      "(Lorg/apache/hadoop/hbase/filter/FilterList$Operator;)V");
  RETURN_ERROR_IF_EXC(env);
  filter_list_add_filter_id_ = env->GetMethodID(filter_list_cl_, "addFilter",
      "(Lorg/apache/hadoop/hbase/filter/Filter;)V");
  RETURN_ERROR_IF_EXC(env);

  // FilterList.Operator fields.
  jfieldID must_pass_all_id = env->GetStaticFieldID(filter_list_op_cl_, "MUST_PASS_ALL",
      "Lorg/apache/hadoop/hbase/filter/FilterList$Operator;");
  RETURN_ERROR_IF_EXC(env);
  must_pass_all_op_ = env->GetStaticObjectField(filter_list_op_cl_, must_pass_all_id);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, must_pass_all_op_, &must_pass_all_op_));

  // SingleColumnValueFilter method ids.
  single_column_value_filter_ctor_ =
      env->GetMethodID(single_column_value_filter_cl_, "<init>",
          "([B[BLorg/apache/hadoop/hbase/filter/CompareFilter$CompareOp;[B)V");
  RETURN_ERROR_IF_EXC(env);

  // Get op array from CompareFilter.CompareOp.
  jmethodID compare_op_values = env->GetStaticMethodID(compare_op_cl_, "values",
      "()[Lorg/apache/hadoop/hbase/filter/CompareFilter$CompareOp;");
  RETURN_ERROR_IF_EXC(env);
  compare_ops_ =
      (jobjectArray) env->CallStaticObjectMethod(compare_op_cl_, compare_op_values);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, reinterpret_cast<jobject>(compare_ops_),
      reinterpret_cast<jobject*>(&compare_ops_)));

  return Status::OK;
}

Status HBaseTableScanner::ScanSetup(JNIEnv* env, const TupleDescriptor* tuple_desc,
    const vector<THBaseFilter>& filters) {
  SCOPED_TIMER(scan_setup_timer_);

  const HBaseTableDescriptor* hbase_table =
      static_cast<const HBaseTableDescriptor*>(tuple_desc->table_desc());
  // Use global cache of HTables.
  RETURN_IF_ERROR(htable_factory_->GetTable(hbase_table->table_name(),
      &htable_));

  // Setup an Scan object without the range
  // scan_ = new Scan();
  DCHECK(scan_ == NULL);
  scan_ = env->NewObject(scan_cl_, scan_ctor_);
  RETURN_ERROR_IF_EXC(env);
  scan_ = env->NewGlobalRef(scan_);

  // scan_.setMaxVersions(1);
  env->CallObjectMethod(scan_, scan_set_max_versions_id_, 1);
  RETURN_ERROR_IF_EXC(env);

  // scan_.setCaching(rows_cached_);
  env->CallObjectMethod(scan_, scan_set_caching_id_, rows_cached_);
  RETURN_ERROR_IF_EXC(env);

  const vector<SlotDescriptor*>& slots = tuple_desc->slots();
  // Restrict scan to materialized families/qualifiers.
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    const string& family = hbase_table->cols()[slots[i]->col_pos()].first;
    const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].second;
    // The row key has an empty qualifier.
    if (qualifier.empty()) continue;
    jbyteArray family_bytes;
    RETURN_IF_ERROR(CreateByteArray(env, family, &family_bytes));
    jbyteArray qualifier_bytes;
    RETURN_IF_ERROR(CreateByteArray(env, qualifier, &qualifier_bytes));
    // scan_.addColumn(family_bytes, qualifier_bytes);
    env->CallObjectMethod(scan_, scan_add_column_id_, family_bytes, qualifier_bytes);
    RETURN_ERROR_IF_EXC(env);
  }

  // circumvent hbase bug: make sure to select all cols that have filters,
  // otherwise the filter may not get applied;
  // see HBASE-4364 (https://issues.apache.org/jira/browse/HBASE-4364)
  num_addl_requested_cols_ = 0;
  for (vector<THBaseFilter>::const_iterator it = filters.begin(); it != filters.end();
       ++it) {
    bool requested = false;
    for (int i = 0; i < slots.size(); ++i) {
      if (!slots[i]->is_materialized()) continue;
      const string& family = hbase_table->cols()[slots[i]->col_pos()].first;
      const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].second;
      if (family == it->family && qualifier == it->qualifier) {
        requested = true;
        break;
      }
    }
    if (requested) continue;
    jbyteArray family_bytes;
    RETURN_IF_ERROR(CreateByteArray(env, it->family, &family_bytes));
    jbyteArray qualifier_bytes;
    RETURN_IF_ERROR(CreateByteArray(env, it->qualifier, &qualifier_bytes));
    // scan_.addColumn(family_bytes, qualifier_bytes);
    env->CallObjectMethod(scan_, scan_add_column_id_, family_bytes, qualifier_bytes);
    RETURN_ERROR_IF_EXC(env);
    ++num_addl_requested_cols_;
  }

  // Add HBase Filters.
  if (!filters.empty()) {
    // filter_list = new FilterList(Operator.MUST_PASS_ALL);
    jobject filter_list =
        env->NewObject(filter_list_cl_, filter_list_ctor_, must_pass_all_op_);
    RETURN_ERROR_IF_EXC(env);
    vector<THBaseFilter>::const_iterator it;
    for (it = filters.begin(); it != filters.end(); ++it) {
      // hbase_op = CompareFilter.CompareOp.values()[it->op_ordinal];
      jobject hbase_op = env->GetObjectArrayElement(compare_ops_, it->op_ordinal);
      RETURN_ERROR_IF_EXC(env);
      jbyteArray family_bytes;
      RETURN_IF_ERROR(CreateByteArray(env, it->family, &family_bytes));
      jbyteArray qualifier_bytes;
      RETURN_IF_ERROR(CreateByteArray(env, it->qualifier, &qualifier_bytes));
      jbyteArray value_bytes;
      RETURN_IF_ERROR(CreateByteArray(env, it->filter_constant, &value_bytes));
      // filter = new SingleColumnValueFilter(family_bytes, qualifier_bytes, hbase_op,
      //     value_bytes);
      jobject filter = env->NewObject(single_column_value_filter_cl_,
          single_column_value_filter_ctor_, family_bytes, qualifier_bytes, hbase_op,
          value_bytes);
      RETURN_ERROR_IF_EXC(env);
      // filter_list.add(filter);
      env->CallBooleanMethod(filter_list, filter_list_add_filter_id_, filter);
      RETURN_ERROR_IF_EXC(env);
    }
    // scan.setFilter(filter_list);
    env->CallObjectMethod(scan_, scan_set_filter_id_, filter_list);
    RETURN_ERROR_IF_EXC(env);
  }

  return Status::OK;
}

Status HBaseTableScanner::InitScanRange(JNIEnv* env, const ScanRange& scan_range) {
  jbyteArray start_bytes;
  CreateByteArray(env, scan_range.start_key(), &start_bytes);
  jbyteArray end_bytes;
  CreateByteArray(env, scan_range.stop_key(), &end_bytes);

  // scan_.setStartRow(start_bytes);
  env->CallObjectMethod(scan_, scan_set_start_row_id_, start_bytes);
  RETURN_ERROR_IF_EXC(env);

  // scan_.setStopRow(end_bytes);
  env->CallObjectMethod(scan_, scan_set_stop_row_id_, end_bytes);
  RETURN_ERROR_IF_EXC(env);

  // resultscanner_ = htable_.getScanner(scan_);
  if (resultscanner_ != NULL) env->DeleteGlobalRef(resultscanner_);
  RETURN_IF_ERROR(htable_->GetResultScanner(scan_, &resultscanner_));

  resultscanner_ = env->NewGlobalRef(resultscanner_);

  RETURN_ERROR_IF_EXC(env);
  return Status::OK;
}

Status HBaseTableScanner::StartScan(JNIEnv* env, const TupleDescriptor* tuple_desc,
    const ScanRangeVector& scan_range_vector, const vector<THBaseFilter>& filters) {
  // Setup the scan without ranges first
  RETURN_IF_ERROR(ScanSetup(env, tuple_desc, filters));

  // Record the ranges
  scan_range_vector_ = &scan_range_vector;
  current_scan_range_idx_ = 0;

  // Now, scan the first range (we should have at least one range.)
  return InitScanRange(env, scan_range_vector_->at(current_scan_range_idx_));
}

Status HBaseTableScanner::CreateByteArray(JNIEnv* env, const std::string& s,
    jbyteArray* bytes) {
  if (!s.empty()) {
    *bytes = env->NewByteArray(s.size());
    if (*bytes == NULL) {
      return Status("Couldn't construct java byte array for key " + s);
    }
    env->SetByteArrayRegion(*bytes, 0, s.size(),
        reinterpret_cast<const jbyte*>(s.data()));
  } else {
    *bytes = reinterpret_cast<jbyteArray>(empty_row_);
  }
  return Status::OK;
}

Status HBaseTableScanner::Next(JNIEnv* env, bool* has_next) {
  {
    SCOPED_TIMER(scan_node_->read_timer());
    while (true) {
      if (result_ != NULL) env->DeleteLocalRef(result_);
      // result_ = resultscanner_.next();
      result_ = env->CallObjectMethod(resultscanner_, resultscanner_next_id_);

      // jump to the next region when finished with the current region.
      if (result_ == NULL &&
          current_scan_range_idx_ + 1 < scan_range_vector_->size()) {
        ++current_scan_range_idx_;
        RETURN_IF_ERROR(InitScanRange(env,
            scan_range_vector_->at(current_scan_range_idx_)));
        continue;
      }
      break;
    }
  }

  if (result_ == NULL) {
    *has_next = false;
    return Status::OK;
  }

  if (keyvalues_ != NULL) env->DeleteLocalRef(keyvalues_);
  // keyvalues_ = result_.raw();
  keyvalues_ =
      reinterpret_cast<jobjectArray>(env->CallObjectMethod(result_, result_raw_id_));
  num_keyvalues_ = env->GetArrayLength(keyvalues_);
  // Check that raw() didn't return more keyvalues than expected.
  // If num_requested_keyvalues_ is 0 then only row key is asked for and this check
  // should pass.
  if (num_keyvalues_ > num_requested_keyvalues_ + num_addl_requested_cols_
      && num_requested_keyvalues_ + num_addl_requested_cols_ != 0) {
    *has_next = false;
    return Status("Encountered more keyvalues than expected.");
  }
  // If all requested columns are present, and we didn't ask for any extra ones to work
  // around an hbase bug, we avoid family-/qualifier comparisons in NextValue().
  if (num_keyvalues_ == num_requested_keyvalues_ && num_addl_requested_cols_ == 0) {
    all_keyvalues_present_ = true;
  } else {
    all_keyvalues_present_ = false;
  }
  keyvalue_index_ = 0;
  // All KeyValues are backed by the same buffer. Place it into the C buffer_.
  jobject immutable_bytes_writable =
      env->CallObjectMethod(result_, result_get_bytes_id_);
  if (byte_array_ != NULL) env->DeleteLocalRef(byte_array_);
  byte_array_ = (jbyteArray)
      env->CallObjectMethod(immutable_bytes_writable, immutable_bytes_writable_get_id_);

  int bytes_array_length = env->CallIntMethod(immutable_bytes_writable,
      immutable_bytes_writable_get_length_id_);
  result_bytes_offset_ = env->CallIntMethod(immutable_bytes_writable,
      immutable_bytes_writable_get_offset_id_);
  COUNTER_UPDATE(scan_node_->bytes_read_counter(), bytes_array_length);
  env->DeleteLocalRef(immutable_bytes_writable);

  // Copy the data from the java byte array to our buffer_.
  // Re-allocate buffer_ if necessary.
  if (buffer_length_ < bytes_array_length) {
    buffer_pool_->Clear();
    buffer_ = buffer_pool_->Allocate(bytes_array_length);
    buffer_length_ = bytes_array_length;
  }
  env->GetByteArrayRegion(byte_array_, result_bytes_offset_, bytes_array_length,
      reinterpret_cast<jbyte*>(buffer_));

  value_pool_->Clear();
  *has_next = true;
  return Status::OK;
}

void HBaseTableScanner::GetRowKey(JNIEnv* env, void** key, int* key_length) {
  if (keyvalue_ != NULL) env->DeleteLocalRef(keyvalue_);
  keyvalue_ = env->GetObjectArrayElement(keyvalues_, 0);
  *key_length = env->CallShortMethod(keyvalue_, keyvalue_get_row_length_id_);
  int key_offset =
      env->CallIntMethod(keyvalue_, keyvalue_get_row_offset_id_) - result_bytes_offset_;
  // Allocate one extra byte for null-terminator.
  *key = value_pool_->Allocate(*key_length + 1);
  memcpy(*key, reinterpret_cast<char*>(buffer_) + key_offset, *key_length);
  reinterpret_cast<char*>(*key)[*key_length] = '\0';
}

void HBaseTableScanner::GetValue(JNIEnv* env, const string& family,
    const string& qualifier, void** value, int* value_length) {
  // Current row doesn't have any more keyvalues. All remaining values are NULL.
  if (keyvalue_index_ >= num_keyvalues_) {
    *value = NULL;
    *value_length = 0;
    return;
  }
  keyvalue_ = env->GetObjectArrayElement(keyvalues_, keyvalue_index_);
  if (!all_keyvalues_present_) {
    // Check family. If it doesn't match, we have a NULL value.
    int family_offset = env->CallIntMethod(keyvalue_, keyvalue_get_family_offset_id_) -
        result_bytes_offset_;
    int family_length = env->CallByteMethod(keyvalue_, keyvalue_get_family_length_id_);
    if (CompareStrings(family, family_offset, family_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
    // Check qualifier. If it doesn't match, we have a NULL value.
    int qualifier_offset =
        env->CallIntMethod(keyvalue_, keyvalue_get_qualifier_offset_id_) -
        result_bytes_offset_;
    int qualifier_length =
        env->CallIntMethod(keyvalue_, keyvalue_get_qualifier_length_id_);
    if (CompareStrings(qualifier, qualifier_offset, qualifier_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
  }
  // The requested family/qualifier matches the keyvalue at keyvalue_index_.
  // Copy the cell.
  *value_length = env->CallIntMethod(keyvalue_, keyvalue_get_value_length_id_);
  int value_offset = env->CallIntMethod(keyvalue_, keyvalue_get_value_offset_id_) -
                     result_bytes_offset_;
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

void HBaseTableScanner::Close(JNIEnv* env) {
  if (resultscanner_ != NULL) {
    // resultscanner_.close();
    env->CallObjectMethod(resultscanner_, resultscanner_close_id_);
    env->DeleteGlobalRef(resultscanner_);
  }
  if (scan_ != NULL) {
    env->DeleteGlobalRef(scan_);
  }

  // Close the HTable so that the connections are not kept around.
  if (htable_.get() != NULL) {
    htable_->Close();
  }

  // Delete all local references
  if (result_ != NULL) env->DeleteLocalRef(result_);
  if (keyvalues_ != NULL) env->DeleteLocalRef(keyvalues_);
  if (keyvalue_ != NULL) env->DeleteLocalRef(keyvalue_);
  if (byte_array_ != NULL) env->DeleteLocalRef(byte_array_);
}
