// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <jni.h>
#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"

namespace impala {

class ExecNode;
class DescriptorTbl;
class RowBatch;
class TupleDescriptor;

class PlanExecutor {
 public:
  PlanExecutor(ExecNode* plan, const DescriptorTbl& descs);
  ~PlanExecutor();

  // Start running query. Call this prior to FetchResult().
  Status Exec();

  // Return results through 'batch'. Sets 'batch' to NULL if no more results.
  // The caller is responsible for deleting *batch.
  Status FetchResult(RowBatch** batch);

  int batch_size() const { return batch_size_; }
  void set_batch_size(int batch_size) { batch_size_ = batch_size; }

 private:
  static const int DEFAULT_BATCH_SIZE = 1024;

  ExecNode* plan_;
  const DescriptorTbl& descs_;
  std::vector<const TupleDescriptor*> tuple_descs_;
  RuntimeState runtime_state_;
  int batch_size_;
  bool done_;
};

// JNI-callable wrapper to the plan executor
extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue);

}

#endif
