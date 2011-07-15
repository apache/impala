// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <jni.h>
#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"

namespace impala {

class Expr;
class ExecNode;
class DescriptorTbl;
class RowBatch;
class TupleDescriptor;

// Deserialize plan_bytes into a TExecutePlanRequest.
// Then reconstruct plan, descs and select_list_exprs from TExecutePlanRequest.
// Input parameters:
//   env: Java environment needed to get underlying memory of  execute_plan_request_bytes.
//   execute_plan_request_bytes: Serialized TExecutePlanRequest.
//   obj_pool: Pool to allocate objects needed for plan, descs and select_list_exprs.
// Output parameters:
//   plan: Root of exec node tree.
//   descs: Descriptor table with information about slots and tables.
//   select_list_exprs: Expressions in select list.
Status DeserializeRequest(
    JNIEnv* env,
    jbyteArray execute_plan_request_bytes,
    ObjectPool* obj_pool,
    ExecNode** plan,
    DescriptorTbl** descs,
    std::vector<Expr*>* select_list_exprs);

class PlanExecutor {
 public:
  PlanExecutor(ExecNode* plan, const DescriptorTbl& descs);
  ~PlanExecutor();

  // Start running query. Call this prior to FetchResult().
  Status Exec();

  // Return results through 'batch'. Sets 'batch' to NULL if no more results.
  // The caller is responsible for deleting *batch.
  Status FetchResult(RowBatch** batch);

  RuntimeState* runtime_state() { return &runtime_state_; }

 private:
  ExecNode* plan_;
  std::vector<const TupleDescriptor*> tuple_descs_;
  RuntimeState runtime_state_;
  bool done_;
};

// JNI-callable wrapper to the plan executor
extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue);

}

#endif
