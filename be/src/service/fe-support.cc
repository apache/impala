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
#include <catalog/catalog-util.h>

#include "catalog/catalog-server.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/catalog-op-executor.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/jni-thrift-util.h"
#include "rpc/thrift-server.h"
#include "runtime/client-cache.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "scheduling/cluster-membership-mgr.h"
#include "service/impala-server.h"
#include "service/query-options.h"
#include "util/bloom-filter.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/dynamic-util.h"
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/scope-exit-trigger.h"
#include "util/string-parser.h"
#include "util/symbols-util.h"

#include "common/names.h"

using namespace impala;
using namespace apache::thrift::server;

static bool fe_support_disable_codegen = true;

// Called from tests or external FE after it explicitly loads libfesupport.so.
// This creates the minimal state necessary to service the other JNI calls.
// This is not called when we first start up the BE.
extern "C"
JNIEXPORT void JNICALL
Java_org_apache_impala_service_FeSupport_NativeFeInit(
    JNIEnv* env, jclass fe_support_class, bool external_fe) {
  DCHECK(ExecEnv::GetInstance() == NULL) << "This should only be called once from the FE";
  char* env_logs_dir_str = std::getenv("IMPALA_FE_TEST_LOGS_DIR");
  if (env_logs_dir_str != nullptr) FLAGS_log_dir = env_logs_dir_str;
  char* name = const_cast<char*>("FeSupport");
  // Init the JVM to load the classes in JniUtil that are needed for returning
  // exceptions to the FE.
  InitCommonRuntime(1, &name, true,
      external_fe ? TestInfo::NON_TEST : TestInfo::FE_TEST, external_fe);
  THROW_IF_ERROR(
      LlvmCodeGen::InitializeLlvm(name, true), env, JniUtil::internal_exc_class());
  ExecEnv* exec_env = new ExecEnv(external_fe); // This also caches it from the process.
  THROW_IF_ERROR(exec_env->InitForFeSupport(), env, JniUtil::internal_exc_class());
}

// Serializes expression value 'value' to thrift structure TColumnValue 'col_val'.
// 'type' indicates the type of the expression value.
static void SetTColumnValue(
    const void* value, const ColumnType& type, TColumnValue* col_val) {
  if (value == nullptr) return;
  DCHECK(col_val != nullptr);

  string tmp;
  switch (type.type) {
    case TYPE_BOOLEAN:
      col_val->__set_bool_val(*reinterpret_cast<const bool*>(value));
      break;
    case TYPE_TINYINT:
      col_val->__set_byte_val(*reinterpret_cast<const int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      col_val->__set_short_val(*reinterpret_cast<const int16_t*>(value));
      break;
    case TYPE_INT:
      col_val->__set_int_val(*reinterpret_cast<const int32_t*>(value));
      break;
    case TYPE_BIGINT:
      col_val->__set_long_val(*reinterpret_cast<const int64_t*>(value));
      break;
    case TYPE_FLOAT:
      col_val->__set_double_val(*reinterpret_cast<const float*>(value));
      break;
    case TYPE_DOUBLE:
      col_val->__set_double_val(*reinterpret_cast<const double*>(value));
      break;
    case TYPE_DECIMAL:
      switch (type.GetByteSize()) {
        case 4:
          col_val->string_val =
              reinterpret_cast<const Decimal4Value*>(value)->ToString(type);
          break;
        case 8:
          col_val->string_val =
              reinterpret_cast<const Decimal8Value*>(value)->ToString(type);
          break;
        case 16:
          col_val->string_val =
              reinterpret_cast<const Decimal16Value*>(value)->ToString(type);
          break;
        default:
          DCHECK(false) << "Bad Type: " << type;
      }
      col_val->__isset.string_val = true;
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* string_val = reinterpret_cast<const StringValue*>(value);
      tmp.assign(string_val->Ptr(), string_val->Len());
      col_val->binary_val.swap(tmp);
      col_val->__isset.binary_val = true;
      break;
    }
    case TYPE_CHAR:
      tmp.assign(reinterpret_cast<const char*>(value), type.len);
      col_val->binary_val.swap(tmp);
      col_val->__isset.binary_val = true;
      break;
    case TYPE_TIMESTAMP: {
      const uint8_t* uint8_val = reinterpret_cast<const uint8_t*>(value);
      col_val->binary_val.assign(uint8_val, uint8_val + type.GetSlotSize());
      col_val->__isset.binary_val = true;
      RawValue::PrintValue(value, type, -1, &col_val->string_val);
      col_val->__isset.string_val = true;
      break;
    }
    case TYPE_DATE: {
      col_val->__set_int_val(*reinterpret_cast<const int32_t*>(value));
      RawValue::PrintValue(value, type, -1, &col_val->string_val);
      col_val->__isset.string_val = true;
      break;
    }
    default:
      DCHECK(false) << "bad GetValue() type: " << type.DebugString();
  }
}

// Evaluates a batch of const exprs and returns the results in a serialized
// TResultRow, where each TColumnValue in the TResultRow stores the result of
// a predicate evaluation. It requires JniUtil::Init() to have been
// called. Throws a Java exception if an error or warning is encountered during
// the expr evaluation.
// We also reject the expression rewrite if the size of the returned rewritten result
// is too large.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeEvalExprsWithoutRow(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_expr_batch,
    jbyteArray thrift_query_ctx_bytes, jlong max_result_size) {
  Status status;
  jbyteArray result_bytes = NULL;
  TQueryCtx query_ctx;
  TExprBatch expr_batch;
  TResultRow expr_results;
  vector<TColumnValue> results;
  ObjectPool obj_pool;

  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_expr_batch, &expr_batch), env,
     JniUtil::internal_exc_class(), nullptr);
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_query_ctx_bytes, &query_ctx), env,
     JniUtil::internal_exc_class(), nullptr);
  vector<TExpr>& texprs = expr_batch.exprs;

  // Disable codegen advisorily to avoid unnecessary latency. For testing purposes
  // (expr-test.cc), fe_support_disable_codegen may be set to false.
  query_ctx.disable_codegen_hint = fe_support_disable_codegen;
  // Allow logging of at least one error, so we can detect and convert it into a
  // Java exception.
  query_ctx.client_request.query_options.max_errors = 1;
  // Track memory against a dummy "fe-eval-exprs" resource pool - we don't
  // know what resource pool the query has been assigned to yet.
  query_ctx.request_pool = "fe-eval-exprs";

  RuntimeState state(query_ctx, ExecEnv::GetInstance());
  TPlanFragment fragment;
  PlanFragmentCtxPB fragment_ctx;
  FragmentState fragment_state(state.query_state(), fragment, fragment_ctx);
  // Make sure to close the runtime state no matter how this scope is exited.
  const auto close_runtime_state = MakeScopeExitTrigger([&state, &fragment_state]() {
    fragment_state.ReleaseResources();
    state.ReleaseResources();
  });

  MemPool expr_mem_pool(state.query_mem_tracker());

  // Prepare() the exprs. Always Close() the exprs even in case of errors.
  vector<ScalarExpr*> exprs;
  vector<ScalarExprEvaluator*> evals;
  for (const TExpr& texpr : texprs) {
    ScalarExpr* expr;
    status = ScalarExpr::Create(texpr, RowDescriptor(), &fragment_state, &expr);
    if (!status.ok()) goto error;
    exprs.push_back(expr);
    ScalarExprEvaluator* eval;
    status = ScalarExprEvaluator::Create(*expr, &state, &obj_pool, &expr_mem_pool,
        &expr_mem_pool, &eval);
    evals.push_back(eval);
    if (!status.ok()) goto error;
  }

  // UDFs which cannot be interpreted need to be handled by codegen.
  if (fragment_state.ScalarExprNeedsCodegen()) {
    status = fragment_state.CreateCodegen();
    if (!status.ok()) goto error;
    LlvmCodeGen* codegen = fragment_state.codegen();
    DCHECK(codegen != NULL);
    status = fragment_state.CodegenScalarExprs();
    if (!status.ok()) goto error;
    codegen->EnableOptimizations(false);
    status = codegen->FinalizeModule();
    if (!status.ok()) goto error;
  }

  // Open() and evaluate the exprs. Always Close() the exprs even in case of errors.
  for (int i = 0; i < evals.size(); ++i) {
    ScalarExprEvaluator* eval = evals[i];
    status = eval->Open(&state);
    if (!status.ok()) goto error;

    void* result = eval->GetValue(nullptr);
    status = eval->GetError();
    if (!status.ok()) goto error;

    const ColumnType& type = eval->root().type();
    // reject the expression rewrite if the returned string greater than
    if (type.IsVarLenStringType()) {
      const StringValue* string_val = reinterpret_cast<const StringValue*>(result);
      if (string_val != nullptr) {
        if (string_val->Len() > max_result_size) {
          status = Status(TErrorCode::EXPR_REWRITE_RESULT_LIMIT_EXCEEDED,
              string_val->Len(), max_result_size);
          goto error;
        }
      }
    }

    // 'output_scale' should only be set for MathFunctions::RoundUpTo()
    // with return type double.
    DCHECK(eval->output_scale() == -1 || type.type == TYPE_DOUBLE);
    TColumnValue val;
    SetTColumnValue(result, type, &val);

    // Check for mem limit exceeded.
    status = state.CheckQueryState();
    if (!status.ok()) goto error;
    // Check for warnings registered in the runtime state.
    if (state.HasErrors()) {
      status = Status(state.ErrorLog());
      goto error;
    }

    eval->Close(&state);
    exprs[i]->Close();
    results.push_back(val);
  }

  expr_results.__set_colVals(results);
  expr_mem_pool.FreeAll();
  status = SerializeThriftMsg(env, &expr_results, &result_bytes);
  if (!status.ok()) goto error;
  return result_bytes;

error:
  DCHECK(!status.ok());
  // Convert status to exception. Close all remaining expr contexts.
  for (ScalarExprEvaluator* eval: evals) eval->Close(&state);
  for (ScalarExpr* expr : exprs) expr->Close();
  expr_mem_pool.FreeAll();
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
    // Use the latest version of the file from the file system if specified.
    if (params.needs_refresh) {
      // Refresh the library if necessary.
      LibCache::instance()->SetNeedsRefresh(params.location);
    }
    LibCacheEntryHandle handle;
    string dummy_local_path;
    Status status = LibCache::instance()->GetLocalPath(
        params.location, type, -1, &handle, &dummy_local_path);
    if (!status.ok()) {
      result->__set_result_code(TSymbolLookupResultCode::BINARY_NOT_FOUND);
      result->__set_error_msg(status.GetDetail());
      return;
    }
  }

  // Check if the FE-specified symbol exists as-is.
  // Set 'quiet' to true so we don't flood the log with unfound builtin symbols on
  // startup.
  time_t mtime = -1;
  Status status = LibCache::instance()->CheckSymbolExists(
      params.location, type, params.symbol, true, &mtime);
  if (status.ok()) {
    result->__set_result_code(TSymbolLookupResultCode::SYMBOL_FOUND);
    result->__set_symbol(params.symbol);
    result->__set_last_modified_time(mtime);
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
  status = LibCache::instance()->CheckSymbolExists(
      params.location, type, symbol, false, &mtime);
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
  result->__set_last_modified_time(mtime);
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeCacheJar(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TCacheJarParams params;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &params), env,
      JniUtil::internal_exc_class(), nullptr);

  TCacheJarResult result;
  LibCacheEntryHandle handle;
  string local_path;
  // TODO(IMPALA-6727): used for external data sources; add proper mtime.
  Status status = LibCache::instance()->GetLocalPath(
      params.hdfs_location, LibCache::TYPE_JAR, -1, &handle, &local_path);
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
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TSymbolLookupParams lookup;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &lookup), env,
      JniUtil::internal_exc_class(), nullptr);

  vector<ColumnType> arg_types;
  arg_types.reserve(lookup.arg_types.size());
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

// Add a catalog update to pending_topic_updates_.
extern "C"
JNIEXPORT jint JNICALL
Java_org_apache_impala_service_FeSupport_NativeAddPendingTopicItem(JNIEnv* env,
    jclass fe_support_class, jlong native_catalog_server_ptr, jstring key, jlong version,
    jbyteArray serialized_object, jboolean deleted) {
  std::string key_string;
  {
    JniUtfCharGuard key_str;
    if (!JniUtfCharGuard::create(env, key, &key_str).ok()) {
      return static_cast<jint>(-1);
    }
    key_string.assign(key_str.get());
  }
  JniScopedArrayCritical obj_buf;
  if (!JniScopedArrayCritical::Create(env, serialized_object, &obj_buf)) {
    return static_cast<jint>(-1);
  }
  int res = reinterpret_cast<CatalogServer*>(native_catalog_server_ptr)->
      AddPendingTopicItem(std::move(key_string), version, obj_buf.get(),
      static_cast<uint32_t>(obj_buf.size()), deleted);
  return static_cast<jint>(res);
}

// Get the next catalog update pointed by 'callback_ctx'.
extern "C"
JNIEXPORT jobject JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetNextCatalogObjectUpdate(JNIEnv* env,
    jclass fe_support_class, jlong native_iterator_ptr) {
  return reinterpret_cast<JniCatalogCacheUpdateIterator*>(native_iterator_ptr)->next(env);
}

extern "C"
JNIEXPORT jboolean JNICALL
Java_org_apache_impala_service_FeSupport_NativeLibCacheSetNeedsRefresh(JNIEnv* env,
    jclass fe_support_class, jstring hdfs_location) {
  string str;
  {
    JniUtfCharGuard hdfs_location_data;
    if (!JniUtfCharGuard::create(env, hdfs_location, &hdfs_location_data).ok()) {
      return static_cast<jboolean>(false);
    }
    str.assign(hdfs_location_data.get());
  }
  LibCache::instance()->SetNeedsRefresh(str);
  return static_cast<jboolean>(true);
}

extern "C"
JNIEXPORT jboolean JNICALL
Java_org_apache_impala_service_FeSupport_NativeLibCacheRemoveEntry(JNIEnv* env,
    jclass fe_support_class, jstring hdfs_lib_file) {
  string str;
  {
    JniUtfCharGuard hdfs_lib_file_data;
    if (!JniUtfCharGuard::create(env, hdfs_lib_file, &hdfs_lib_file_data).ok()) {
      return static_cast<jboolean>(false);
    }
    str.assign(hdfs_lib_file_data.get());
  }
  LibCache::instance()->RemoveEntry(str);
  return static_cast<jboolean>(true);
}

// Calls in to the catalog server to request prioritizing the loading of metadata for
// specific catalog objects.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativePrioritizeLoad(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TPrioritizeLoadRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), NULL, NULL);
  TPrioritizeLoadResponse result;
  Status status = catalog_op_executor.PrioritizeLoad(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    // TODO: remove the wrapping; DoRPC's wrapping is sufficient.
    status.AddDetail("Error making an RPC call to Catalog server.");
    status.ToThrift(&result.status);
  }

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Calls in to the catalog server to report recently used table names and the number of
// their usages in this impalad.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeUpdateTableUsage(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TUpdateTableUsageRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), nullptr, nullptr);
  TUpdateTableUsageResponse result;
  Status status = catalog_op_executor.UpdateTableUsage(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    status.AddDetail("Error making an RPC call to Catalog server.");
    status.SetTStatus(&result);
  }

  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Calls in to the catalog server to request partial information about a
// catalog object.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetPartialCatalogObject(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TGetPartialCatalogObjectRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), nullptr, nullptr);
  TGetPartialCatalogObjectResponse result;
  Status status = catalog_op_executor.GetPartialCatalogObject(request, &result);
  THROW_IF_ERROR_RET(status, env, JniUtil::internal_exc_class(), nullptr);

  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Used to call native code from the FE to make a request to catalogd
// for per-partition statistics.
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetPartitionStats(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TGetPartitionStatsRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);
  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), nullptr, nullptr);
  TGetPartitionStatsResponse result;
  Status status = catalog_op_executor.GetPartitionStats(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    status.SetTStatus(&result);
  }
  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Used to call native code from the FE to parse and set comma-delimited key=value query
// options.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeParseQueryOptions(
    JNIEnv* env, jclass fe_support_class, jstring csv_query_options,
    jbyteArray tquery_options) {
  TQueryOptions options;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, tquery_options, &options), env,
      JniUtil::internal_exc_class(), nullptr);

  JniUtfCharGuard csv_query_options_guard;
  THROW_IF_ERROR_RET(
      JniUtfCharGuard::create(env, csv_query_options, &csv_query_options_guard), env,
      JniUtil::internal_exc_class(), nullptr);
  THROW_IF_ERROR_RET(
      impala::ParseQueryOptions(csv_query_options_guard.get(), &options, NULL), env,
      JniUtil::internal_exc_class(), nullptr);

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &options, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Get a list of known coordinators.
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetCoordinators(
    JNIEnv* env, jclass caller_class) {
  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      ExecEnv::GetInstance()->cluster_membership_mgr()->GetSnapshot();
  DCHECK(membership_snapshot != nullptr);
  vector<TNetworkAddress> coordinators = membership_snapshot->GetCoordinatorAddresses();

  TAddressesList addresses_container;
  addresses_container.__set_addresses(coordinators);
  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &addresses_container, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Get the number of live queries.
extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_impala_service_FeSupport_NativeNumLiveQueries(
    JNIEnv* env, jclass caller_class) {
  ImpalaServer* server = ExecEnv::GetInstance()->impala_server();
  if (LIKELY(server != nullptr)) {
    return server->NumLiveQueries();
  }
  // Allow calling without an ImpalaServer, such as during PlannerTest.
  return 0;
}

// Returns the log (base 2) of the minimum number of bytes we need for a Bloom filter
// with 'ndv' unique elements and a false positive probability of less than 'fpp'.
extern "C"
JNIEXPORT jint JNICALL
Java_org_apache_impala_service_FeSupport_MinLogSpaceForBloomFilter(
    JNIEnv* env, jclass fe_support_class, jlong ndv, jdouble fpp) {
  return BloomFilter::MinLogSpace(ndv, fpp);
}

/// Returns the expected false positive rate for the given ndv and log_bufferpool_space.
extern "C"
JNIEXPORT jdouble JNICALL
Java_org_apache_impala_service_FeSupport_FalsePositiveProbForBloomFilter(
    JNIEnv* env, jclass fe_support_class, jlong ndv, jint log_bufferpool_space) {
  return BloomFilter::FalsePositiveProb(ndv, log_bufferpool_space);
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_nativeParseDateString(JNIEnv* env,
    jclass fe_support_class, jstring date) {
  string date_string;
  {
    JniUtfCharGuard date_str_guard;
    THROW_IF_ERROR_RET(
        JniUtfCharGuard::create(env, date, &date_str_guard), env,
        JniUtil::internal_exc_class(), nullptr);
    date_string.assign(date_str_guard.get());
  }

  StringParser::ParseResult res;
  DateValue dv = StringParser::StringToDate(date_string.data(), date_string.length(),
      &res);

  TParseDateStringResult parse_str_result;
  int32_t days_since_epoch;
  parse_str_result.__set_valid(dv.ToDaysSinceEpoch(&days_since_epoch));
  if (parse_str_result.valid) {
    parse_str_result.__set_days_since_epoch(days_since_epoch);
    // If date is not yet in canonical form (yyyy-MM-dd), convert it to string again.
    if (date_string.length() != 10) {
      const string canonical_date_string = dv.ToString();
      parse_str_result.__set_canonical_date_string(canonical_date_string);
    }
  }

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &parse_str_result, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Native method to make a request to catalog server to get the null partition name.
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetNullPartitionName(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TGetNullPartitionNameRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);
  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), nullptr, nullptr);
  TGetNullPartitionNameResponse result;
  Status status = catalog_op_executor.GetNullPartitionName(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    status.ToThrift(&result.status);
  }
  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Native method to make a request to catalog server to get the latest compactions.
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetLatestCompactions(
    JNIEnv* env, jclass fe_support_class, jbyteArray thrift_struct) {
  TGetLatestCompactionsRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);
  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), nullptr, nullptr);
  TGetLatestCompactionsResponse result;
  Status status = catalog_op_executor.GetLatestCompactions(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    status.ToThrift(&result.status);
  }
  jbyteArray result_bytes = nullptr;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
      JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

namespace impala {

static JNINativeMethod native_methods[] = {
  {
      const_cast<char*>("NativeFeInit"), const_cast<char*>("(Z)V"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeFeInit
  },
  {
      const_cast<char*>("NativeEvalExprsWithoutRow"), const_cast<char*>("([B[BJ)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeEvalExprsWithoutRow
  },
  {
      const_cast<char*>("NativeCacheJar"), const_cast<char*>("([B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeCacheJar
  },
  {
      const_cast<char*>("NativeLookupSymbol"), const_cast<char*>("([B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeLookupSymbol
  },
  {
      const_cast<char*>("NativePrioritizeLoad"), const_cast<char*>("([B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativePrioritizeLoad
  },
  {
      const_cast<char*>("NativeGetPartialCatalogObject"),
      const_cast<char*>("([B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeGetPartialCatalogObject
  },
  {
      const_cast<char*>("NativeGetPartitionStats"), const_cast<char*>("([B)[B"),
     (void*) ::Java_org_apache_impala_service_FeSupport_NativeGetPartitionStats
  },
  {
      const_cast<char*>("NativeUpdateTableUsage"),
      const_cast<char*>("([B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeUpdateTableUsage
  },
  {
      const_cast<char*>("NativeParseQueryOptions"),
      const_cast<char*>("(Ljava/lang/String;[B)[B"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeParseQueryOptions
  },
  {
      const_cast<char*>("NativeAddPendingTopicItem"),
      const_cast<char*>("(JLjava/lang/String;J[BZ)I"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeAddPendingTopicItem
  },
  {
      const_cast<char*>("NativeGetNextCatalogObjectUpdate"),
      const_cast<char*>("(J)Lorg/apache/impala/common/Pair;"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeGetNextCatalogObjectUpdate
  },
  {
      const_cast<char*>("NativeLibCacheSetNeedsRefresh"),
      const_cast<char*>("(Ljava/lang/String;)Z"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeLibCacheSetNeedsRefresh
  },
  {
      const_cast<char*>("NativeLibCacheRemoveEntry"),
      const_cast<char*>("(Ljava/lang/String;)Z"),
      (void*)::Java_org_apache_impala_service_FeSupport_NativeLibCacheRemoveEntry
  },
  {
    const_cast<char*>("MinLogSpaceForBloomFilter"), const_cast<char*>("(JD)I"),
    (void*)::Java_org_apache_impala_service_FeSupport_MinLogSpaceForBloomFilter
  },
  {
    const_cast<char*>("FalsePositiveProbForBloomFilter"), const_cast<char*>("(JI)D"),
    (void*)::Java_org_apache_impala_service_FeSupport_FalsePositiveProbForBloomFilter
  },
  {
    const_cast<char*>("nativeParseDateString"),
    const_cast<char*>("(Ljava/lang/String;)[B"),
    (void*)::Java_org_apache_impala_service_FeSupport_nativeParseDateString
  },
  {
    const_cast<char*>("NativeGetNullPartitionName"), const_cast<char*>("([B)[B"),
    (void*) ::Java_org_apache_impala_service_FeSupport_NativeGetNullPartitionName
  },
  {
    const_cast<char*>("NativeGetLatestCompactions"), const_cast<char*>("([B)[B"),
    (void*) ::Java_org_apache_impala_service_FeSupport_NativeGetLatestCompactions
  },
  {
    const_cast<char*>("NativeGetCoordinators"), const_cast<char*>("()[B"),
    (void*)::Java_org_apache_impala_service_FeSupport_NativeGetCoordinators
  },
  {
    const_cast<char*>("NativeNumLiveQueries"), const_cast<char*>("()J"),
    (void*)::Java_org_apache_impala_service_FeSupport_NativeNumLiveQueries
  },
};

void InitFeSupport(bool disable_codegen) {
  fe_support_disable_codegen = disable_codegen;
  JNIEnv* env = JniUtil::GetJNIEnv();
  jclass native_backend_cl = env->FindClass("org/apache/impala/service/FeSupport");
  env->RegisterNatives(native_backend_cl, native_methods,
      sizeof(native_methods) / sizeof(native_methods[0]));
  ABORT_IF_EXC(env);
}

}
