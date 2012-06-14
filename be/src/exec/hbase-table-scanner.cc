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
jclass HBaseTableScanner::filter_list_cl_ = NULL;
jclass HBaseTableScanner::filter_list_op_cl_ = NULL;
jclass HBaseTableScanner::single_column_value_filter_cl_ = NULL;
jclass HBaseTableScanner::compare_op_cl_ = NULL;
jmethodID HBaseTableScanner::htable_ctor_ = NULL;
jmethodID HBaseTableScanner::htable_get_scanner_id_ = NULL;
jmethodID HBaseTableScanner::htable_close_id_ = NULL;
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

HBaseTableScanner::HBaseTableScanner(JNIEnv* env, ScanNode* scan_node,
    HBaseTableCache* htable_cache)
  : env_(env),
    scan_node_(scan_node),
    htable_cache_(htable_cache),
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
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/HTable",
          &htable_cl_));
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
  htable_ctor_ = env->GetMethodID(htable_cl_, "<init>",
      "(Lorg/apache/hadoop/conf/Configuration;Ljava/lang/String;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  htable_get_scanner_id_ = env->GetMethodID(htable_cl_, "getScanner",
      "(Lorg/apache/hadoop/hbase/client/Scan;)"
      "Lorg/apache/hadoop/hbase/client/ResultScanner;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  htable_close_id_ = env->GetMethodID(htable_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Scan method ids.
  scan_ctor_ = env->GetMethodID(scan_cl_, "<init>", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_max_versions_id_ = env->GetMethodID(scan_cl_, "setMaxVersions",
      "(I)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_caching_id_ = env->GetMethodID(scan_cl_, "setCaching", "(I)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_add_column_id_ = env->GetMethodID(scan_cl_, "addColumn",
      "([B[B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_filter_id_ = env->GetMethodID(scan_cl_, "setFilter",
      "(Lorg/apache/hadoop/hbase/filter/Filter;)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_start_row_id_ = env->GetMethodID(scan_cl_, "setStartRow",
      "([B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  scan_set_stop_row_id_ = env->GetMethodID(scan_cl_, "setStopRow",
      "([B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // ResultScanner method ids.
  resultscanner_next_id_ = env->GetMethodID(resultscanner_cl_, "next",
      "()Lorg/apache/hadoop/hbase/client/Result;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  resultscanner_close_id_ = env->GetMethodID(resultscanner_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Result method ids.
  result_raw_id_ = env->GetMethodID(result_cl_, "raw",
      "()[Lorg/apache/hadoop/hbase/KeyValue;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  result_get_bytes_id_ = env->GetMethodID(result_cl_, "getBytes",
      "()Lorg/apache/hadoop/hbase/io/ImmutableBytesWritable;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // ImmutableBytesWritable method ids.
  immutable_bytes_writable_get_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "get", "()[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  immutable_bytes_writable_get_length_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "getLength", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  immutable_bytes_writable_get_offset_id_ =
      env->GetMethodID(immutable_bytes_writable_cl_, "getOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // KeyValue method ids.
  keyvalue_get_buffer_id_ = env->GetMethodID(keyvalue_cl_, "getBuffer", "()[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_family_offset_id_ =
      env->GetMethodID(keyvalue_cl_, "getFamilyOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_family_length_id_ =
      env->GetMethodID(keyvalue_cl_, "getFamilyLength", "()B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_qualifier_offset_id_ =
      env->GetMethodID(keyvalue_cl_, "getQualifierOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_qualifier_length_id_ =
      env->GetMethodID(keyvalue_cl_, "getQualifierLength", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_row_offset_id_ = env->GetMethodID(keyvalue_cl_, "getRowOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_row_length_id_ = env->GetMethodID(keyvalue_cl_, "getRowLength", "()S");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_value_offset_id_ = env->GetMethodID(keyvalue_cl_, "getValueOffset", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  keyvalue_get_value_length_id_ = env->GetMethodID(keyvalue_cl_, "getValueLength", "()I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // HConstants fields.
  jfieldID empty_start_row_id =
      env->GetStaticFieldID(hconstants_cl_, "EMPTY_START_ROW", "[B");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  empty_row_ = env->GetStaticObjectField(hconstants_cl_, empty_start_row_id);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, empty_row_, &empty_row_));

  // FilterList method ids.
  filter_list_ctor_ = env->GetMethodID(filter_list_cl_, "<init>",
      "(Lorg/apache/hadoop/hbase/filter/FilterList$Operator;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  filter_list_add_filter_id_ = env->GetMethodID(filter_list_cl_, "addFilter",
      "(Lorg/apache/hadoop/hbase/filter/Filter;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // FilterList.Operator fields.
  jfieldID must_pass_all_id = env->GetStaticFieldID(filter_list_op_cl_, "MUST_PASS_ALL",
      "Lorg/apache/hadoop/hbase/filter/FilterList$Operator;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  must_pass_all_op_ = env->GetStaticObjectField(filter_list_op_cl_, must_pass_all_id);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, must_pass_all_op_, &must_pass_all_op_));

  // SingleColumnValueFilter method ids.
  single_column_value_filter_ctor_ =
      env->GetMethodID(single_column_value_filter_cl_, "<init>",
          "([B[BLorg/apache/hadoop/hbase/filter/CompareFilter$CompareOp;[B)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Get op array from CompareFilter.CompareOp.
  jmethodID compare_op_values = env->GetStaticMethodID(compare_op_cl_, "values",
      "()[Lorg/apache/hadoop/hbase/filter/CompareFilter$CompareOp;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  compare_ops_ =
      (jobjectArray) env->CallStaticObjectMethod(compare_op_cl_, compare_op_values);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, reinterpret_cast<jobject>(compare_ops_),
      reinterpret_cast<jobject*>(&compare_ops_)));

  return Status::OK;
}

Status HBaseTableScanner::ScanSetup(const TupleDescriptor* tuple_desc,
    const vector<THBaseFilter>& filters) {
  // set up HBase specific counter
  scan_setup_timer_  = ADD_COUNTER(scan_node_->runtime_profile(),
      "HBaseTableScanner.ScanSetup", TCounterType::CPU_TICKS);
  COUNTER_SCOPED_TIMER(scan_setup_timer_);

  const HBaseTableDescriptor* hbase_table =
      static_cast<const HBaseTableDescriptor*>(tuple_desc->table_desc());
  // Use global cache of HTables.
  htable_ = htable_cache_->GetHBaseTable(env_, hbase_table->table_name());
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // Setup an Scan object without the range
  // scan_ = new Scan();
  scan_ = env_->NewObject(scan_cl_, scan_ctor_);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_.setMaxVersions(1);
  scan_ = env_->CallObjectMethod(scan_, scan_set_max_versions_id_, 1);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_.setCaching(rows_cached_);
  env_->CallObjectMethod(scan_, scan_set_caching_id_, rows_cached_);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  const vector<SlotDescriptor*>& slots = tuple_desc->slots();
  // Restrict scan to materialized families/qualifiers.
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    const string& family = hbase_table->cols()[slots[i]->col_pos()].first;
    const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].second;
    // The row key has an empty qualifier.
    if (qualifier.empty()) continue;
    jbyteArray family_bytes;
    RETURN_IF_ERROR(CreateByteArray(family, &family_bytes));
    jbyteArray qualifier_bytes;
    RETURN_IF_ERROR(CreateByteArray(qualifier, &qualifier_bytes));
    // scan_.addColumn(family_bytes, qualifier_bytes);
    env_->CallObjectMethod(scan_, scan_add_column_id_, family_bytes, qualifier_bytes);
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
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
    RETURN_IF_ERROR(CreateByteArray(it->family, &family_bytes));
    jbyteArray qualifier_bytes;
    RETURN_IF_ERROR(CreateByteArray(it->qualifier, &qualifier_bytes));
    // scan_.addColumn(family_bytes, qualifier_bytes);
    env_->CallObjectMethod(scan_, scan_add_column_id_, family_bytes, qualifier_bytes);
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
    ++num_addl_requested_cols_;
  }

  // Add HBase Filters.
  if (!filters.empty()) {
    // filter_list = new FilterList(Operator.MUST_PASS_ALL);
    jobject filter_list =
        env_->NewObject(filter_list_cl_, filter_list_ctor_, must_pass_all_op_);
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
    vector<THBaseFilter>::const_iterator it;
    for (it = filters.begin(); it != filters.end(); ++it) {
      // hbase_op = CompareFilter.CompareOp.values()[it->op_ordinal];
      jobject hbase_op = env_->GetObjectArrayElement(compare_ops_, it->op_ordinal);
      RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
      jbyteArray family_bytes;
      RETURN_IF_ERROR(CreateByteArray(it->family, &family_bytes));
      jbyteArray qualifier_bytes;
      RETURN_IF_ERROR(CreateByteArray(it->qualifier, &qualifier_bytes));
      jbyteArray value_bytes;
      RETURN_IF_ERROR(CreateByteArray(it->filter_constant, &value_bytes));
      // filter = new SingleColumnValueFilter(family_bytes, qualifier_bytes, hbase_op,
      //     value_bytes);
      jobject filter = env_->NewObject(single_column_value_filter_cl_,
          single_column_value_filter_ctor_, family_bytes, qualifier_bytes, hbase_op,
          value_bytes);
      RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
      // filter_list.add(filter);
      env_->CallBooleanMethod(filter_list, filter_list_add_filter_id_, filter);
      RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
    }
    // scan.setFilter(filter_list);
    scan_ = env_->CallObjectMethod(scan_, scan_set_filter_id_, filter_list);
    RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
  }

  return Status::OK;
}

Status HBaseTableScanner::InitScanRange(const ScanRange& scan_range) {
  jbyteArray start_bytes;
  CreateByteArray(scan_range.start_key(), &start_bytes);
  jbyteArray end_bytes;
  CreateByteArray(scan_range.stop_key(), &end_bytes);

  // scan_ = scan_.setStartRow(start_bytes);
  scan_ = env_->CallObjectMethod(scan_, scan_set_start_row_id_, start_bytes);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // scan_ = scan_.setStopRow(end_bytes);
  scan_ = env_->CallObjectMethod(scan_, scan_set_stop_row_id_, end_bytes);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());

  // resultscanner_ = htable_.getScanner(scan_);
  resultscanner_ = env_->CallObjectMethod(htable_, htable_get_scanner_id_, scan_);
  RETURN_ERROR_IF_EXC(env_, JniUtil::throwable_to_string_id());
  return Status::OK;

}

Status HBaseTableScanner::StartScan(const TupleDescriptor* tuple_desc,
    const ScanRangeVector& scan_range_vector, const vector<THBaseFilter>& filters) {
  // Setup the scan without ranges first
  RETURN_IF_ERROR(ScanSetup(tuple_desc, filters));

  // Record the ranges
  scan_range_vector_ = &scan_range_vector;
  current_scan_range_idx_ = 0;

  // Now, scan the first range (we should have at least one range.)
  return InitScanRange(scan_range_vector_->at(current_scan_range_idx_));
}

Status HBaseTableScanner::CreateByteArray(const std::string& s, jbyteArray* bytes) {
  if (!s.empty()) {
    *bytes = env_->NewByteArray(s.size());
    if (*bytes == NULL) {
      return Status("Couldn't construct java byte array for key " + s);
    }
    env_->SetByteArrayRegion(*bytes, 0, s.size(),
        reinterpret_cast<const jbyte*>(s.data()));
  } else {
    *bytes = reinterpret_cast<jbyteArray>(empty_row_);
  }
  return Status::OK;
}

Status HBaseTableScanner::Next(bool* has_next) {
  while(true) {
    //result_ = resultscanner_.next();
    result_ = env_->CallObjectMethod(resultscanner_, resultscanner_next_id_);

    // jump to the next region when finished with the current region.
    if (result_ == NULL &&
        current_scan_range_idx_ + 1 < scan_range_vector_->size()) {
      ++current_scan_range_idx_;
      RETURN_IF_ERROR(InitScanRange(scan_range_vector_->at(current_scan_range_idx_)));
      continue;
    }
    break;
  }

  if (result_ == NULL) {
    *has_next = false;
    return Status::OK;
  }

  // keyvalues_ = result_.raw();
  keyvalues_ =
      reinterpret_cast<jobjectArray>(env_->CallObjectMethod(result_, result_raw_id_));
  num_keyvalues_ = env_->GetArrayLength(keyvalues_);
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
  // All KeyValues are backed by the same buffer. Place it into the C byffer_.
  jobject immutable_bytes_writable =
      env_->CallObjectMethod(result_, result_get_bytes_id_);
  byte_array_ = (jbyteArray)
      env_->CallObjectMethod(immutable_bytes_writable, immutable_bytes_writable_get_id_);

  int bytes_array_length = env_->CallIntMethod(immutable_bytes_writable,
      immutable_bytes_writable_get_length_id_);
  result_bytes_offset_ = env_->CallIntMethod(immutable_bytes_writable,
      immutable_bytes_writable_get_offset_id_);
  COUNTER_UPDATE(scan_node_->bytes_read_counter(), bytes_array_length);

  // Copy the data from the java byte array to our buffer_.
  // Re-allocate buffer_ if necessary.
  if (buffer_length_ < bytes_array_length) {
    buffer_pool_->Clear();
    buffer_ = buffer_pool_->Allocate(bytes_array_length);
    buffer_length_ = bytes_array_length;
  }
  env_->GetByteArrayRegion(byte_array_, result_bytes_offset_, bytes_array_length,
      reinterpret_cast<jbyte*>(buffer_));

  value_pool_->Clear();
  *has_next = true;
  return Status::OK;
}

void HBaseTableScanner::GetRowKey(void** key, int* key_length) {
  keyvalue_ = env_->GetObjectArrayElement(keyvalues_, 0);
  *key_length = env_->CallShortMethod(keyvalue_, keyvalue_get_row_length_id_);
  int key_offset =
      env_->CallIntMethod(keyvalue_, keyvalue_get_row_offset_id_) - result_bytes_offset_;
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
    int family_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_family_offset_id_) -
        result_bytes_offset_;
    int family_length = env_->CallByteMethod(keyvalue_, keyvalue_get_family_length_id_);
    if (CompareStrings(family, family_offset, family_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
    // Check qualifier. If it doesn't match, we have a NULL value.
    int qualifier_offset =
        env_->CallIntMethod(keyvalue_, keyvalue_get_qualifier_offset_id_) -
        result_bytes_offset_;
    int qualifier_length =
        env_->CallIntMethod(keyvalue_, keyvalue_get_qualifier_length_id_);
    if (CompareStrings(qualifier, qualifier_offset, qualifier_length) != 0) {
      *value = NULL;
      *value_length = 0;
      return;
    }
  }
  // The requested family/qualifier matches the keyvalue at keyvalue_index_.
  // Copy the cell.
  *value_length = env_->CallIntMethod(keyvalue_, keyvalue_get_value_length_id_);
  int value_offset = env_->CallIntMethod(keyvalue_, keyvalue_get_value_offset_id_) -
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

void HBaseTableScanner::Close() {
  if (resultscanner_ != NULL) {
    // resultscanner_.close();
    env_->CallObjectMethod(resultscanner_, resultscanner_close_id_);
  }
}
