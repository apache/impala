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

#include "exec/hbase/hbase-table-scanner.h"
#include "exec/hbase/hbase-scan-node.h"

#include <cstring>
#include <algorithm>

#include "util/bit-util.h"
#include "util/jni-util.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/tuple.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

jclass HBaseTableScanner::scan_cl_ = NULL;
jclass HBaseTableScanner::resultscanner_cl_ = NULL;
jclass HBaseTableScanner::result_cl_ = NULL;
jclass HBaseTableScanner::cell_cl_ = NULL;
jclass HBaseTableScanner::hconstants_cl_ = NULL;
jclass HBaseTableScanner::filter_list_cl_ = NULL;
jclass HBaseTableScanner::filter_list_op_cl_ = NULL;
jclass HBaseTableScanner::single_column_value_filter_cl_ = NULL;
jclass HBaseTableScanner::compare_op_cl_ = NULL;
jclass HBaseTableScanner::scanner_timeout_ex_cl_ = NULL;
jmethodID HBaseTableScanner::scan_ctor_ = NULL;
jmethodID HBaseTableScanner::scan_set_max_versions_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_caching_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_cache_blocks_id_ = NULL;
jmethodID HBaseTableScanner::scan_add_column_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_filter_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_start_row_id_ = NULL;
jmethodID HBaseTableScanner::scan_set_stop_row_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_next_id_ = NULL;
jmethodID HBaseTableScanner::resultscanner_close_id_ = NULL;
jmethodID HBaseTableScanner::result_isempty_id_ = NULL;
jmethodID HBaseTableScanner::result_raw_cells_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_row_array_ = NULL;
jmethodID HBaseTableScanner::cell_get_family_array_ = NULL;
jmethodID HBaseTableScanner::cell_get_qualifier_array_ = NULL;
jmethodID HBaseTableScanner::cell_get_value_array_ = NULL;
jmethodID HBaseTableScanner::cell_get_family_offset_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_family_length_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_qualifier_offset_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_qualifier_length_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_row_offset_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_row_length_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_value_offset_id_ = NULL;
jmethodID HBaseTableScanner::cell_get_value_length_id_ = NULL;
jmethodID HBaseTableScanner::filter_list_ctor_ = NULL;
jmethodID HBaseTableScanner::filter_list_add_filter_id_ = NULL;
jmethodID HBaseTableScanner::single_column_value_filter_ctor_ = NULL;
jobject HBaseTableScanner::empty_row_ = NULL;
jobject HBaseTableScanner::must_pass_all_op_ = NULL;
jobjectArray HBaseTableScanner::compare_ops_ = NULL;

const string HBASE_MEM_LIMIT_EXCEEDED = "HBaseTableScanner::$0() failed to "
    "allocate $1 bytes for $2.";

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
    HBaseScanNode* scan_node, HBaseTableFactory* htable_factory, RuntimeState* state)
  : scan_node_(scan_node),
    state_(state),
    htable_factory_(htable_factory),
    htable_(NULL),
    scan_(NULL),
    resultscanner_(NULL),
    cells_(NULL),
    cell_index_(0),
    num_requested_cells_(0),
    num_addl_requested_cols_(0),
    num_cells_(0),
    all_cells_present_(false),
    value_pool_(new MemPool(scan_node_->mem_tracker(), true)),
    scan_setup_timer_(ADD_TIMER(scan_node_->runtime_profile(),
      "HBaseTableScanner.ScanSetup")) {
  const TQueryOptions& query_option = state->query_options();
  if (query_option.__isset.hbase_caching && query_option.hbase_caching > 0) {
    rows_cached_ = query_option.hbase_caching;
  } else {
    int max_caching = scan_node_->suggested_max_caching();
    rows_cached_ = (max_caching > 0 && max_caching < DEFAULT_ROWS_CACHED) ?
        max_caching : DEFAULT_ROWS_CACHED;
  }
  cache_blocks_ = query_option.__isset.hbase_cache_blocks &&
      query_option.hbase_cache_blocks;
}

Status HBaseTableScanner::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  // Global class references:
  // HTable, Scan, ResultScanner, Result, ImmutableBytesWritable, HConstants.
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Scan", &scan_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/ResultScanner",
          &resultscanner_cl_));
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/client/Result",
          &result_cl_));
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
  // ScannerTimeoutException is removed in HBase 2.0. Leave this as null if
  // ScannerTimeoutException does not exist.
  if (JniUtil::ClassExists(env,
      "org/apache/hadoop/hbase/client/ScannerTimeoutException")) {
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
        "org/apache/hadoop/hbase/client/ScannerTimeoutException",
        &scanner_timeout_ex_cl_));
  }
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/hadoop/hbase/Cell", &cell_cl_));

  // Scan method ids.
  scan_ctor_ = env->GetMethodID(scan_cl_, "<init>", "()V");
  RETURN_ERROR_IF_EXC(env);
  scan_set_max_versions_id_ = env->GetMethodID(scan_cl_, "setMaxVersions",
      "(I)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);

  // The signature of setCaching and setCacheBlocks returns either void (old behavior)
  // or a Scan object (new behavior). Since the code doesn't use the return value,
  // tolerate either. However, the two are expected to be consistent.
  if (JniUtil::MethodExists(env, scan_cl_, "setCaching", "(I)V")) {
    scan_set_caching_id_ = env->GetMethodID(scan_cl_, "setCaching", "(I)V");
    RETURN_ERROR_IF_EXC(env);
    scan_set_cache_blocks_id_ = env->GetMethodID(scan_cl_, "setCacheBlocks", "(Z)V");
    RETURN_ERROR_IF_EXC(env);
  } else {
    scan_set_caching_id_ = env->GetMethodID(scan_cl_, "setCaching",
        "(I)Lorg/apache/hadoop/hbase/client/Scan;");
    RETURN_ERROR_IF_EXC(env);
    scan_set_cache_blocks_id_ = env->GetMethodID(scan_cl_, "setCacheBlocks",
        "(Z)Lorg/apache/hadoop/hbase/client/Scan;");
    RETURN_ERROR_IF_EXC(env);
  }

  scan_add_column_id_ = env->GetMethodID(scan_cl_, "addColumn",
      "([B[B)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  scan_set_filter_id_ = env->GetMethodID(scan_cl_, "setFilter",
      "(Lorg/apache/hadoop/hbase/filter/Filter;)Lorg/apache/hadoop/hbase/client/Scan;");
  RETURN_ERROR_IF_EXC(env);
  // TODO: IMPALA-5584: In HBase 2.0, setStartRow() and setStopRow() are deprecated.
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
  result_raw_cells_id_ = env->GetMethodID(result_cl_, "rawCells",
      "()[Lorg/apache/hadoop/hbase/Cell;");
  RETURN_ERROR_IF_EXC(env);
  result_isempty_id_ = env->GetMethodID(result_cl_, "isEmpty", "()Z");
  RETURN_ERROR_IF_EXC(env);


  // Cell method ids to retrieve buffers backing different portions of row data.
  cell_get_row_array_ = env->GetMethodID(cell_cl_, "getRowArray", "()[B");
  RETURN_ERROR_IF_EXC(env);
  cell_get_family_array_ = env->GetMethodID(cell_cl_, "getFamilyArray", "()[B");
  RETURN_ERROR_IF_EXC(env);
  cell_get_qualifier_array_ = env->GetMethodID(cell_cl_, "getQualifierArray", "()[B");
  RETURN_ERROR_IF_EXC(env);
  cell_get_value_array_ = env->GetMethodID(cell_cl_, "getValueArray", "()[B");
  RETURN_ERROR_IF_EXC(env);

  // Cell method ids for retrieving lengths and offsets into buffers backing different
  // portions of row data.
  cell_get_family_offset_id_ = env->GetMethodID(cell_cl_, "getFamilyOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  cell_get_family_length_id_ = env->GetMethodID(cell_cl_, "getFamilyLength", "()B");
  RETURN_ERROR_IF_EXC(env);
  cell_get_qualifier_offset_id_ =
      env->GetMethodID(cell_cl_, "getQualifierOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  cell_get_qualifier_length_id_ =
      env->GetMethodID(cell_cl_, "getQualifierLength", "()I");
  RETURN_ERROR_IF_EXC(env);
  cell_get_row_offset_id_ = env->GetMethodID(cell_cl_, "getRowOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  cell_get_row_length_id_ = env->GetMethodID(cell_cl_, "getRowLength", "()S");
  RETURN_ERROR_IF_EXC(env);
  cell_get_value_offset_id_ = env->GetMethodID(cell_cl_, "getValueOffset", "()I");
  RETURN_ERROR_IF_EXC(env);
  cell_get_value_length_id_ = env->GetMethodID(cell_cl_, "getValueLength", "()I");
  RETURN_ERROR_IF_EXC(env);

  // HConstants fields.
  jfieldID empty_start_row_id =
      env->GetStaticFieldID(hconstants_cl_, "EMPTY_START_ROW", "[B");
  RETURN_ERROR_IF_EXC(env);
  empty_row_ = env->GetStaticObjectField(hconstants_cl_, empty_start_row_id);
  RETURN_ERROR_IF_EXC(env);
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
  RETURN_ERROR_IF_EXC(env);
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
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, reinterpret_cast<jobject>(compare_ops_),
      reinterpret_cast<jobject*>(&compare_ops_)));

  return Status::OK();
}

Status HBaseTableScanner::ScanSetup(JNIEnv* env, const TupleDescriptor* tuple_desc,
    const vector<THBaseFilter>& filters) {
  SCOPED_TIMER(scan_setup_timer_);
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  const HBaseTableDescriptor* hbase_table =
      static_cast<const HBaseTableDescriptor*>(tuple_desc->table_desc());
  // Use global cache of HTables.
  RETURN_IF_ERROR(htable_factory_->GetTable(hbase_table->table_name(),
      &htable_));

  // Setup an Scan object without the range
  // scan_ = new Scan();
  DCHECK(scan_ == NULL);
  jobject local_scan = env->NewObject(scan_cl_, scan_ctor_);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_scan, &scan_));

  // scan_.setMaxVersions(1);
  env->CallObjectMethod(scan_, scan_set_max_versions_id_, 1);
  RETURN_ERROR_IF_EXC(env);

  // scan_.setCaching(rows_cached_);
  env->CallObjectMethod(scan_, scan_set_caching_id_, rows_cached_);
  RETURN_ERROR_IF_EXC(env);

  // scan_.setCacheBlocks(cache_blocks_);
  env->CallObjectMethod(scan_, scan_set_cache_blocks_id_, cache_blocks_);
  RETURN_ERROR_IF_EXC(env);

  const vector<SlotDescriptor*>& slots = tuple_desc->slots();
  // Restrict scan to materialized families/qualifiers.
  for (int i = 0; i < slots.size(); ++i) {
    const string& family = hbase_table->cols()[slots[i]->col_pos()].family;
    const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].qualifier;
    // The row key has an empty qualifier.
    if (qualifier.empty()) continue;
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(env));
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
      const string& family = hbase_table->cols()[slots[i]->col_pos()].family;
      const string& qualifier = hbase_table->cols()[slots[i]->col_pos()].qualifier;
      if (family == it->family && qualifier == it->qualifier) {
        requested = true;
        break;
      }
    }
    if (requested) continue;
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(env));
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
      JniLocalFrame jni_frame;
      RETURN_IF_ERROR(jni_frame.push(env));
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

  return Status::OK();
}

Status HBaseTableScanner::HandleResultScannerTimeout(JNIEnv* env, bool* timeout) {
  *timeout = false;
  jthrowable exc = env->ExceptionOccurred();
  if (exc == NULL) return Status::OK();

  // GetJniExceptionMsg gets the error message and clears the exception status (which is
  // necessary). For HBase 2.0, ScannerTimeoutException does not exist, so simply
  // return the error. Otherwise, we return the error if the exception was not a
  // ScannerTimeoutException.
  Status status = JniUtil::GetJniExceptionMsg(env, false);
  if (scanner_timeout_ex_cl_ == nullptr) return status;
  if (env->IsInstanceOf(exc, scanner_timeout_ex_cl_) != JNI_TRUE) return status;

  *timeout = true;
  const ScanRange& scan_range = (*scan_range_vector_)[current_scan_range_idx_];
  // If cells_ is NULL, then the ResultScanner timed out before it was ever used
  // so we can just re-create the ResultScanner with the same scan_range
  if (cells_ == NULL) return InitScanRange(env, scan_range);

  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  DCHECK_LT(cell_index_, num_cells_);
  jobject cell = env->GetObjectArrayElement(cells_, cell_index_);
  RETURN_ERROR_IF_EXC(env);
  // Specifically set the start_bytes to the next row since some of them were already
  // read
  jbyteArray start_bytes =
    (jbyteArray) env->CallObjectMethod(cell, cell_get_row_array_);
  RETURN_ERROR_IF_EXC(env);
  jbyteArray end_bytes;
  RETURN_IF_ERROR(CreateByteArray(env, scan_range.stop_key(), &end_bytes));
  return InitScanRange(env, start_bytes, end_bytes);
}

Status HBaseTableScanner::InitScanRange(JNIEnv* env, const ScanRange& scan_range) {
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  jbyteArray start_bytes;
  RETURN_IF_ERROR(CreateByteArray(env, scan_range.start_key(), &start_bytes));
  jbyteArray end_bytes;
  RETURN_IF_ERROR(CreateByteArray(env, scan_range.stop_key(), &end_bytes));
  return InitScanRange(env, start_bytes, end_bytes);
}

Status HBaseTableScanner::InitScanRange(JNIEnv* env, jbyteArray start_bytes,
    jbyteArray end_bytes) {
  // scan_.setStartRow(start_bytes);
  env->CallObjectMethod(scan_, scan_set_start_row_id_, start_bytes);
  RETURN_ERROR_IF_EXC(env);

  // scan_.setStopRow(end_bytes);
  env->CallObjectMethod(scan_, scan_set_stop_row_id_, end_bytes);
  RETURN_ERROR_IF_EXC(env);

  if (resultscanner_ != NULL) {
    // resultscanner_.close();
    env->CallObjectMethod(resultscanner_, resultscanner_close_id_);
    RETURN_ERROR_IF_EXC(env);
    env->DeleteGlobalRef(resultscanner_);
    resultscanner_ = NULL;
  }
  // resultscanner_ = htable_.getScanner(scan_);
  jobject local_resultscanner;
  RETURN_IF_ERROR(htable_->GetResultScanner(scan_, &local_resultscanner));
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_resultscanner, &resultscanner_));
  return Status::OK();
}

Status HBaseTableScanner::StartScan(JNIEnv* env, const TupleDescriptor* tuple_desc,
    const ScanRangeVector& scan_range_vector, const vector<THBaseFilter>& filters) {
  DCHECK(scan_range_vector.size() > 0);
  // Setup the scan without ranges first
  RETURN_IF_ERROR(ScanSetup(env, tuple_desc, filters));

  // Record the ranges
  scan_range_vector_ = &scan_range_vector;
  current_scan_range_idx_ = 0;

  // Now, scan the first range (we should have at least one range). The
  // resultscanner_ is NULL and gets created in InitScanRange, so we don't
  // need to check if it timed out.
  DCHECK(resultscanner_ == NULL);
  return InitScanRange(env, (*scan_range_vector_)[current_scan_range_idx_]);
}

Status HBaseTableScanner::CreateByteArray(JNIEnv* env, const string& s,
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
  return Status::OK();
}

Status HBaseTableScanner::Next(JNIEnv* env, bool* has_next) {
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  jobject result = NULL;
  {
    SCOPED_TIMER(scan_node_->hbase_read_timer());
    while (true) {
      DCHECK(resultscanner_ != NULL);
      // result_ = resultscanner_.next();
      result = env->CallObjectMethod(resultscanner_, resultscanner_next_id_);
      RETURN_ERROR_IF_EXC(env);
      // Normally we would check for a JNI exception via RETURN_ERROR_IF_EXC, but we
      // need to also check for scanner timeouts and handle them specially, which is
      // done by HandleResultScannerTimeout(). If a timeout occurred, then it will
      // re-create the ResultScanner so we can try again.
      bool timeout;
      RETURN_IF_ERROR(HandleResultScannerTimeout(env, &timeout));
      if (timeout) {
        result = env->CallObjectMethod(resultscanner_, resultscanner_next_id_);
        // There shouldn't be a timeout now, so we will just return any errors.
        RETURN_ERROR_IF_EXC(env);
      }
      // jump to the next region when finished with the current region.
      if (result == NULL && current_scan_range_idx_ + 1 < scan_range_vector_->size()) {
        ++current_scan_range_idx_;
        RETURN_IF_ERROR(InitScanRange(env,
            (*scan_range_vector_)[current_scan_range_idx_]));
        continue;
      }

      // Ignore empty rows
      if (result != NULL) {
        bool isEmpty = JNI_TRUE == env->CallBooleanMethod(result, result_isempty_id_);
        RETURN_ERROR_IF_EXC(env);
        if (isEmpty) {
          continue;
        }
      }
      break;
    }
  }

  if (result == NULL) {
    *has_next = false;
    return Status::OK();
  }

  if (cells_ != NULL) env->DeleteGlobalRef(cells_);
  // cells_ = result.raw();
  jobject local_cells = reinterpret_cast<jobjectArray>(
      env->CallObjectMethod(result, result_raw_cells_id_));
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_cells, &cells_));
  num_cells_ = env->GetArrayLength(cells_);
  // Check that raw() didn't return more cells than expected.
  // If num_requested_cells_ is 0 then only row key is asked for and this check
  // should pass.
  if (num_cells_ > num_requested_cells_ + num_addl_requested_cols_
      && num_requested_cells_ + num_addl_requested_cols_ != 0) {
    *has_next = false;
    return Status("Encountered more cells than expected.");
  }
  // If all requested columns are present, and we didn't ask for any extra ones to work
  // around an hbase bug, we avoid family-/qualifier comparisons in NextValue().
  if (num_cells_ == num_requested_cells_ && num_addl_requested_cols_ == 0) {
    all_cells_present_ = true;
  } else {
    all_cells_present_ = false;
  }
  cell_index_ = 0;

  value_pool_->Clear();
  *has_next = true;
  return Status::OK();
}

inline void HBaseTableScanner::WriteTupleSlot(const SlotDescriptor* slot_desc,
    Tuple* tuple, void* data) {
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  BitUtil::ByteSwap(slot, data, slot_desc->type().GetByteSize());
}

inline Status HBaseTableScanner::GetRowKey(JNIEnv* env, jobject cell,
    void** data, int* length) {
  JniLocalFrame frame;
  RETURN_IF_ERROR(frame.push(env));
  int offset = env->CallIntMethod(cell, cell_get_row_offset_id_);
  RETURN_ERROR_IF_EXC(env);
  *length = env->CallShortMethod(cell, cell_get_row_length_id_);
  RETURN_ERROR_IF_EXC(env);
  jbyteArray jdata =
      (jbyteArray) env->CallObjectMethod(cell, cell_get_row_array_);
  RETURN_ERROR_IF_EXC(env);
  *data = value_pool_->TryAllocate(*length);
  if (UNLIKELY(*data == NULL)) {
    string details = Substitute(HBASE_MEM_LIMIT_EXCEEDED, "GetRowKey",
        *length, "row array");
    return value_pool_->mem_tracker()->MemLimitExceeded(state_, details, *length);
  }
  env->GetByteArrayRegion(jdata, offset, *length, reinterpret_cast<jbyte*>(*data));
  RETURN_ERROR_IF_EXC(env);
  COUNTER_ADD(scan_node_->bytes_read_counter(), *length);
  return Status::OK();
}

inline Status HBaseTableScanner::GetFamily(JNIEnv* env, jobject cell,
    void** data, int* length) {
  int offset = env->CallIntMethod(cell, cell_get_family_offset_id_);
  RETURN_ERROR_IF_EXC(env);
  *length = env->CallShortMethod(cell, cell_get_family_length_id_);
  RETURN_ERROR_IF_EXC(env);
  jbyteArray jdata =
      (jbyteArray) env->CallObjectMethod(cell, cell_get_family_array_);
  RETURN_ERROR_IF_EXC(env);
  *data = value_pool_->TryAllocate(*length);
  if (UNLIKELY(*data == NULL)) {
    string details = Substitute(HBASE_MEM_LIMIT_EXCEEDED, "GetFamily",
        *length, "family array");
    return value_pool_->mem_tracker()->MemLimitExceeded(state_, details, *length);
  }
  env->GetByteArrayRegion(jdata, offset, *length, reinterpret_cast<jbyte*>(*data));
  RETURN_ERROR_IF_EXC(env);
  COUNTER_ADD(scan_node_->bytes_read_counter(), *length);
  return Status::OK();
}

inline Status HBaseTableScanner::GetQualifier(JNIEnv* env, jobject cell,
    void** data, int* length) {
  int offset = env->CallIntMethod(cell, cell_get_qualifier_offset_id_);
  RETURN_ERROR_IF_EXC(env);
  *length = env->CallIntMethod(cell, cell_get_qualifier_length_id_);
  RETURN_ERROR_IF_EXC(env);
  jbyteArray jdata =
      (jbyteArray) env->CallObjectMethod(cell, cell_get_qualifier_array_);
  RETURN_ERROR_IF_EXC(env);
  *data = value_pool_->TryAllocate(*length);
  if (UNLIKELY(*data == NULL)) {
    string details = Substitute(HBASE_MEM_LIMIT_EXCEEDED, "GetQualifier",
        *length, "qualifier array");
    return value_pool_->mem_tracker()->MemLimitExceeded(state_, details, *length);
  }
  env->GetByteArrayRegion(jdata, offset, *length, reinterpret_cast<jbyte*>(*data));
  RETURN_ERROR_IF_EXC(env);
  COUNTER_ADD(scan_node_->bytes_read_counter(), *length);
  return Status::OK();
}

inline Status HBaseTableScanner::GetValue(JNIEnv* env, jobject cell,
    void** data, int* length) {
  int offset = env->CallIntMethod(cell, cell_get_value_offset_id_);
  RETURN_ERROR_IF_EXC(env);
  *length = env->CallIntMethod(cell, cell_get_value_length_id_);
  RETURN_ERROR_IF_EXC(env);
  jbyteArray jdata =
      (jbyteArray) env->CallObjectMethod(cell, cell_get_value_array_);
  RETURN_ERROR_IF_EXC(env);
  *data = value_pool_->TryAllocate(*length);
  if (UNLIKELY(*data == NULL)) {
    string details = Substitute(HBASE_MEM_LIMIT_EXCEEDED, "GetValue",
        *length, "value array");
    return value_pool_->mem_tracker()->MemLimitExceeded(state_, details, *length);
  }
  env->GetByteArrayRegion(jdata, offset, *length, reinterpret_cast<jbyte*>(*data));
  RETURN_ERROR_IF_EXC(env);
  COUNTER_ADD(scan_node_->bytes_read_counter(), *length);
  return Status::OK();
}

Status HBaseTableScanner::GetRowKey(JNIEnv* env, void** key, int* key_length) {
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  jobject cell = env->GetObjectArrayElement(cells_, 0);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(GetRowKey(env, cell, key, key_length));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status HBaseTableScanner::GetRowKey(JNIEnv* env, const SlotDescriptor* slot_desc,
    Tuple* tuple) {
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  void* key;
  int key_length;
  jobject cell = env->GetObjectArrayElement(cells_, 0);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(GetRowKey(env, cell, &key, &key_length));
  DCHECK_EQ(key_length, slot_desc->type().GetByteSize());
  WriteTupleSlot(slot_desc, tuple, reinterpret_cast<char*>(key));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status HBaseTableScanner::GetCurrentValue(JNIEnv* env, const string& family,
    const string& qualifier, void** data, int* length, bool* is_null) {
  // Current row doesn't have any more cells. All remaining values are NULL.
  if (cell_index_ >= num_cells_) {
    *is_null = true;
    return Status::OK();
  }
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));
  jobject cell = env->GetObjectArrayElement(cells_, cell_index_);
  RETURN_ERROR_IF_EXC(env);
  if (!all_cells_present_) {
    // Check family. If it doesn't match, we have a NULL value.
    void* family_data;
    int family_length;
    RETURN_IF_ERROR(GetFamily(env, cell, &family_data, &family_length));
    if (CompareStrings(family, family_data, family_length) != 0) {
      *is_null = true;
      return Status::OK();
    }

    // Check qualifier. If it doesn't match, we have a NULL value.
    void* qualifier_data;
    int qualifier_length;
    RETURN_IF_ERROR(GetQualifier(env, cell, &qualifier_data, &qualifier_length));
    if (CompareStrings(qualifier, qualifier_data, qualifier_length) != 0) {
      *is_null = true;
      return Status::OK();
    }
  }
  RETURN_IF_ERROR(GetValue(env, cell, data, length));
  *is_null = false;
  return Status::OK();
}

Status HBaseTableScanner::GetValue(JNIEnv* env, const string& family,
    const string& qualifier, void** value, int* value_length) {
  bool is_null;
  RETURN_IF_ERROR(GetCurrentValue(env, family, qualifier, value, value_length, &is_null));
  RETURN_ERROR_IF_EXC(env);
  if (is_null) {
    *value = NULL;
    *value_length = 0;
    return Status::OK();
  }
  ++cell_index_;
  return Status::OK();
}

Status HBaseTableScanner::GetValue(JNIEnv* env, const string& family,
    const string& qualifier, const SlotDescriptor* slot_desc, Tuple* tuple) {
  void* value;
  int value_length;
  bool is_null;
  RETURN_IF_ERROR(
      GetCurrentValue(env, family, qualifier, &value, &value_length, &is_null));
  RETURN_ERROR_IF_EXC(env);
  if (is_null) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  DCHECK_EQ(value_length, slot_desc->type().GetByteSize());
  WriteTupleSlot(slot_desc, tuple, reinterpret_cast<char*>(value));
  ++cell_index_;
  return Status::OK();
}

int HBaseTableScanner::CompareStrings(const string& s, void* data, int length) {
  int slength = static_cast<int>(s.length());
  if (slength == 0 && length == 0) return 0;
  if (length == 0) return 1;
  if (slength == 0) return -1;
  int result = memcmp(s.data(), reinterpret_cast<char*>(data), min(slength, length));
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
    // Manually check if the ResultScanner timed out so that we can log a less scary
    // and more specific message.
    jthrowable exc = env->ExceptionOccurred();
    if (exc != NULL) {
      if (env->IsInstanceOf(exc, scanner_timeout_ex_cl_) == JNI_TRUE) {
        env->ExceptionClear();
        LOG(INFO) << "ResultScanner timed out before it was closed "
                  << "(this does not necessarily indicate a problem)";
      } else {
        // GetJniExceptionMsg will clear the exception status and log
        Status status = JniUtil::GetJniExceptionMsg(env, true,
            "Unknown error occurred while closing ResultScanner: ");
        if (!status.ok()) LOG(WARNING) << "Error closing ResultScanner()";
      }
    }
    env->DeleteGlobalRef(resultscanner_);
    resultscanner_ = NULL;
  }
  if (scan_ != NULL) env->DeleteGlobalRef(scan_);
  if (cells_ != NULL) env->DeleteGlobalRef(cells_);

  // Close the HTable so that the connections are not kept around.
  if (htable_.get() != NULL) htable_->Close(state_);

  value_pool_->FreeAll();
}
