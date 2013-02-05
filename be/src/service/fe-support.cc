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

// This file contains implementations for the JNI FeSupport interface.

#include "service/fe-support.h"

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "util/uid-util.h"  // for some reasoon needed right here for hash<TUniqueId>
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/client-cache.h"
#include "service/impala-server.h"
#include "testutil/test-exec-env.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/debug-util.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;

extern "C"
JNIEXPORT jboolean JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeEvalPredicate(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate_bytes) {
  ObjectPool obj_pool;
  TExpr thrift_predicate;
  DeserializeThriftMsg(env, thrift_predicate_bytes, &thrift_predicate);
  Expr* e;
  Status status = Expr::CreateExprTree(&obj_pool, thrift_predicate, &e);
  if (status.ok()) {
    // TODO: codegen this as well.
    status = Expr::Prepare(e, NULL, RowDescriptor(), true);
  }
  if (!status.ok()) {
    string error_msg;
    status.GetErrorMsg(&error_msg);
    jclass internal_exc_cl =
        env->FindClass("com/cloudera/impala/common/InternalException");
    if (internal_exc_cl == NULL) {
      if (env->ExceptionOccurred()) env->ExceptionDescribe();
      return false;
    }
    env->ThrowNew(internal_exc_cl, error_msg.c_str());
    return false;
  }

  void* value = e->GetValue(NULL);
  // This can happen if a table has partitions with NULL key values.
  if (value == NULL) {
    return false;
  }
  bool* v = static_cast<bool*>(value);
  return *v;
}


namespace impala {

void InitFeSupport() {
  JNIEnv* env = getJNIEnv();
  JNINativeMethod nm;
  jclass native_backend_cl = env->FindClass("com/cloudera/impala/service/FeSupport");
  nm.name = const_cast<char*>("NativeEvalPredicate");
  nm.signature = const_cast<char*>("([B)Z");
  nm.fnPtr = reinterpret_cast<void*>(
      ::Java_com_cloudera_impala_service_FeSupport_NativeEvalPredicate);
  env->RegisterNatives(native_backend_cl, &nm, 1);
}

}
