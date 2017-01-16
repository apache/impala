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

// This file contains implementations for the JNI FeSupport interface.

#include "service/fe-support.h"

#include <boost/scoped_ptr.hpp>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/catalog-op-executor.h"
#include "exprs/expr-context.h"
#include "exprs/expr.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/jni-thrift-util.h"
#include "rpc/thrift-server.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "service/impala-server.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/dynamic-util.h"
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/scope-exit-trigger.h"
#include "util/symbols-util.h"

#include "common/names.h"

using namespace impala;
using namespace apache::thrift::server;

static bool fe_support_disable_codegen = true;

// Called from the FE when it explicitly loads libfesupport.so for tests.
// This creates the minimal state necessary to service the other JNI calls.
// This is not called when we first start up the BE.
extern "C"
JNIEXPORT void JNICALL
Java_org_apache_impala_service_FeSupport_NativeFeTestInit(
    JNIEnv* env, jclass caller_class) {
  DCHECK(ExecEnv::GetInstance() == NULL) << "This should only be called once from the FE";
  char* name = const_cast<char*>("FeSupport");
  // Init the JVM to load the classes in JniUtil that are needed for returning
  // exceptions to the FE.
  InitCommonRuntime(1, &name, true, TestInfo::FE_TEST);
  LlvmCodeGen::InitializeLlvm(true);
  ExecEnv* exec_env = new ExecEnv(); // This also caches it from the process.
  exec_env->InitForFeTests();
}

// Evaluates a batch of const exprs and returns the results in a serialized
// TResultRow, where each TColumnValue in the TResultRow stores the result of
// a predicate evaluation. It requires JniUtil::Init() to have been
// called. Throws a Java exception if an error or warning is encountered during
// the expr evaluation.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeEvalExprsWithoutRow(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_expr_batch,
    jbyteArray thrift_query_ctx_bytes) {
  Status status;
  jbyteArray result_bytes = NULL;
  TQueryCtx query_ctx;
  TExprBatch expr_batch;
  JniLocalFrame jni_frame;
  TResultRow expr_results;
  vector<TColumnValue> results;
  ObjectPool obj_pool;

  DeserializeThriftMsg(env, thrift_expr_batch, &expr_batch);
  DeserializeThriftMsg(env, thrift_query_ctx_bytes, &query_ctx);
  vector<TExpr>& texprs = expr_batch.exprs;
  // Disable codegen advisorily to avoid unnecessary latency. For testing purposes
  // (expr-test.cc), fe_support_disable_codegen may be set to false.
  query_ctx.disable_codegen_hint = fe_support_disable_codegen;
  // Allow logging of at least one error, so we can detect and convert it into a
  // Java exception.
  query_ctx.client_request.query_options.max_errors = 1;

  // Track memory against a dummy "fe-eval-exprs" resource pool - we don't
  // know what resource pool the query has been assigned to yet.
  RuntimeState state(query_ctx, ExecEnv::GetInstance(), "fe-eval-exprs");
  // Make sure to close the runtime state no matter how this scope is exited.
  const auto close_runtime_state =
      MakeScopeExitTrigger([&state]() { state.ReleaseResources(); });

  THROW_IF_ERROR_RET(
      jni_frame.push(env), env, JniUtil::internal_exc_class(), result_bytes);

  // Prepare() the exprs. Always Close() the exprs even in case of errors.
  vector<ExprContext*> expr_ctxs;
  for (const TExpr& texpr : texprs) {
    ExprContext* ctx;
    status = Expr::CreateExprTree(&obj_pool, texpr, &ctx);
    if (!status.ok()) goto error;

    // Add 'ctx' to vector so it will be closed if Prepare() fails.
    expr_ctxs.push_back(ctx);
    status = ctx->Prepare(&state, RowDescriptor(), state.query_mem_tracker());
    if (!status.ok()) goto error;
  }

  // UDFs which cannot be interpreted need to be handled by codegen.
  if (state.ScalarFnNeedsCodegen()) {
    status = state.CreateCodegen();
    if (!status.ok()) goto error;
    LlvmCodeGen* codegen = state.codegen();
    DCHECK(codegen != NULL);
    status = state.CodegenScalarFns();
    if (!status.ok()) goto error;
    codegen->EnableOptimizations(false);
    status = codegen->FinalizeModule();
    if (!status.ok()) goto error;
  }

  // Open() and evaluate the exprs. Always Close() the exprs even in case of errors.
  for (ExprContext* expr_ctx : expr_ctxs) {
    status = expr_ctx->Open(&state);
    if (!status.ok()) goto error;

    TColumnValue val;
    expr_ctx->EvaluateWithoutRow(&val);
    status = expr_ctx->root()->GetFnContextError(expr_ctx);
    if (!status.ok()) goto error;

    // Check for mem limit exceeded.
    status = state.CheckQueryState();
    if (!status.ok()) goto error;
    // Check for warnings registered in the runtime state.
    if (state.HasErrors()) {
      status = Status(state.ErrorLog());
      goto error;
    }

    expr_ctx->Close(&state);
    results.push_back(val);
  }

  expr_results.__set_colVals(results);
  status = SerializeThriftMsg(env, &expr_results, &result_bytes);
  if (!status.ok()) goto error;
  return result_bytes;

error:
  DCHECK(!status.ok());
  // Convert status to exception. Close all remaining expr contexts.
  for (ExprContext* expr_ctx : expr_ctxs) expr_ctx->Close(&state);
  (env)->ThrowNew(JniUtil::internal_exc_class(), status.GetDetail().c_str());
  return result_bytes;
}

// Does the symbol resolution, filling in the result in *result.
static void ResolveSymbolLookup(const TSymbolLookupParams params,
    const vector<ColumnType>& arg_types, TSymbolLookupResult* result) {
  LibCache::LibType type;
  if (params.fn_binary_type == TFunctionBinaryType::NATIVE ||
      params.fn_binary_type == TFunctionBinaryType::BUILTIN) {
    // We use TYPE_SO for builtins, since LibCache does not resolve symbols for IR
    // builtins. This is ok since builtins have the same symbol whether we run the IR or
    // native versions.
    type = LibCache::TYPE_SO;
  } else if (params.fn_binary_type == TFunctionBinaryType::IR) {
    type = LibCache::TYPE_IR;
  } else if (params.fn_binary_type == TFunctionBinaryType::JAVA) {
    type = LibCache::TYPE_JAR;
  } else {
    DCHECK(false) << params.fn_binary_type;
    type = LibCache::TYPE_JAR; // Set type to something for the case where DCHECK is off.
  }

  // Builtin functions are loaded directly from the running process
  if (params.fn_binary_type != TFunctionBinaryType::BUILTIN) {
    // Refresh the library if necessary since we're creating a new function
    LibCache::instance()->SetNeedsRefresh(params.location);
    string dummy_local_path;
    Status status = LibCache::instance()->GetLocalLibPath(
        params.location, type, &dummy_local_path);
    if (!status.ok()) {
      result->__set_result_code(TSymbolLookupResultCode::BINARY_NOT_FOUND);
      result->__set_error_msg(status.GetDetail());
      return;
    }
  }

  // Check if the FE-specified symbol exists as-is.
  // Set 'quiet' to true so we don't flood the log with unfound builtin symbols on
  // startup.
  Status status =
      LibCache::instance()->CheckSymbolExists(params.location, type, params.symbol, true);
  if (status.ok()) {
    result->__set_result_code(TSymbolLookupResultCode::SYMBOL_FOUND);
    result->__set_symbol(params.symbol);
    return;
  }

  if (params.fn_binary_type == TFunctionBinaryType::JAVA ||
      SymbolsUtil::IsMangled(params.symbol)) {
    // No use trying to mangle Hive or already mangled symbols, return the error.
    // TODO: we can demangle the user symbol here and validate it against
    // params.arg_types. This would prevent someone from typing the wrong symbol
    // by accident. This requires more string parsing of the symbol.
    result->__set_result_code(TSymbolLookupResultCode::SYMBOL_NOT_FOUND);
    stringstream ss;
    ss << "Could not find symbol '" << params.symbol << "' in: " << params.location;
    result->__set_error_msg(ss.str());
    VLOG(1) << ss.str() << endl << status.GetDetail();
    return;
  }

  string symbol = params.symbol;
  ColumnType ret_type(INVALID_TYPE);
  if (params.__isset.ret_arg_type) ret_type = ColumnType::FromThrift(params.ret_arg_type);

  // Mangle the user input
  DCHECK_NE(params.fn_binary_type, TFunctionBinaryType::JAVA);
  if (params.symbol_type == TSymbolType::UDF_EVALUATE) {
    symbol = SymbolsUtil::MangleUserFunction(params.symbol,
        arg_types, params.has_var_args, params.__isset.ret_arg_type ? &ret_type : NULL);
  } else {
    DCHECK(params.symbol_type == TSymbolType::UDF_PREPARE ||
           params.symbol_type == TSymbolType::UDF_CLOSE);
    symbol = SymbolsUtil::ManglePrepareOrCloseFunction(params.symbol);
  }

  // Look up the mangled symbol
  status = LibCache::instance()->CheckSymbolExists(params.location, type, symbol);
  if (!status.ok()) {
    result->__set_result_code(TSymbolLookupResultCode::SYMBOL_NOT_FOUND);
    stringstream ss;
    ss << "Could not find function " << params.symbol << "(";

    if (params.symbol_type == TSymbolType::UDF_EVALUATE) {
      for (int i = 0; i < arg_types.size(); ++i) {
        ss << arg_types[i].DebugString();
        if (i != arg_types.size() - 1) ss << ", ";
      }
    } else {
      ss << "impala_udf::FunctionContext*, "
         << "impala_udf::FunctionContext::FunctionStateScope";
    }

    ss << ")";
    if (params.__isset.ret_arg_type) ss << " returns " << ret_type.DebugString();
    ss << " in: " << params.location;
    if (params.__isset.ret_arg_type) {
      ss << "\nCheck that function name, arguments, and return type are correct.";
    } else {
      ss << "\nCheck that symbol and argument types are correct.";
    }
    result->__set_error_msg(ss.str());
    return;
  }

  // We were able to resolve the symbol.
  result->__set_result_code(TSymbolLookupResultCode::SYMBOL_FOUND);
  result->__set_symbol(symbol);
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeCacheJar(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TCacheJarParams params;
  DeserializeThriftMsg(env, thrift_struct, &params);

  TCacheJarResult result;
  string local_path;
  Status status = LibCache::instance()->GetLocalLibPath(params.hdfs_location,
      LibCache::TYPE_JAR, &local_path);
  status.ToThrift(&result.status);
  if (status.ok()) result.__set_local_path(local_path);

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeLookupSymbol(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TSymbolLookupParams lookup;
  DeserializeThriftMsg(env, thrift_struct, &lookup);

  vector<ColumnType> arg_types;
  for (int i = 0; i < lookup.arg_types.size(); ++i) {
    arg_types.push_back(ColumnType::FromThrift(lookup.arg_types[i]));
  }

  TSymbolLookupResult result;
  ResolveSymbolLookup(lookup, arg_types, &result);

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Calls in to the catalog server to request prioritizing the loading of metadata for
// specific catalog objects.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativePrioritizeLoad(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TPrioritizeLoadRequest request;
  DeserializeThriftMsg(env, thrift_struct, &request);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), NULL, NULL);
  TPrioritizeLoadResponse result;
  Status status = catalog_op_executor.PrioritizeLoad(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    // Create a new Status, copy in this error, then update the result.
    Status catalog_service_status(result.status);
    catalog_service_status.MergeStatus(status);
    status.ToThrift(&result.status);
  }

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

namespace impala {

static JNINativeMethod native_methods[] = {
  {
    (char*)"NativeFeTestInit", (char*)"()V",
    (void*)::Java_org_apache_impala_service_FeSupport_NativeFeTestInit
  },
  {
    (char*)"NativeEvalExprsWithoutRow", (char*)"([B[B)[B",
    (void*)::Java_org_apache_impala_service_FeSupport_NativeEvalExprsWithoutRow
  },
  {
    (char*)"NativeCacheJar", (char*)"([B)[B",
    (void*)::Java_org_apache_impala_service_FeSupport_NativeCacheJar
  },
  {
    (char*)"NativeLookupSymbol", (char*)"([B)[B",
    (void*)::Java_org_apache_impala_service_FeSupport_NativeLookupSymbol
  },
  {
    (char*)"NativePrioritizeLoad", (char*)"([B)[B",
    (void*)::Java_org_apache_impala_service_FeSupport_NativePrioritizeLoad
  },
};

void InitFeSupport(bool disable_codegen) {
  fe_support_disable_codegen = disable_codegen;
  JNIEnv* env = getJNIEnv();
  jclass native_backend_cl = env->FindClass("org/apache/impala/service/FeSupport");
  env->RegisterNatives(native_backend_cl, native_methods,
      sizeof(native_methods) / sizeof(native_methods[0]));
  EXIT_IF_EXC(env);
}

}
