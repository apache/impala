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
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
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
  char* env_logs_dir_str = std::getenv("IMPALA_FE_TEST_LOGS_DIR");
  if (env_logs_dir_str != nullptr) FLAGS_log_dir = env_logs_dir_str;
  char* name = const_cast<char*>("FeSupport");
  // Init the JVM to load the classes in JniUtil that are needed for returning
  // exceptions to the FE.
  InitCommonRuntime(1, &name, true, TestInfo::FE_TEST);
  THROW_IF_ERROR(LlvmCodeGen::InitializeLlvm(true), env, JniUtil::internal_exc_class());
  ExecEnv* exec_env = new ExecEnv(); // This also caches it from the process.
  THROW_IF_ERROR(exec_env->InitForFeTests(), env, JniUtil::internal_exc_class());
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
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
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
    default:
      DCHECK(false) << "bad GetValue() type: " << type.DebugString();
  }
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
  // Make sure to close the runtime state no matter how this scope is exited.
  const auto close_runtime_state =
      MakeScopeExitTrigger([&state]() { state.ReleaseResources(); });

  THROW_IF_ERROR_RET(
      jni_frame.push(env), env, JniUtil::internal_exc_class(), result_bytes);

  MemPool expr_mem_pool(state.query_mem_tracker());

  // Prepare() the exprs. Always Close() the exprs even in case of errors.
  vector<ScalarExpr*> exprs;
  vector<ScalarExprEvaluator*> evals;
  for (const TExpr& texpr : texprs) {
    ScalarExpr* expr;
    status = ScalarExpr::Create(texpr, RowDescriptor(), &state, &expr);
    if (!status.ok()) goto error;
    exprs.push_back(expr);
    ScalarExprEvaluator* eval;
    status = ScalarExprEvaluator::Create(*expr, &state, &obj_pool, &expr_mem_pool,
        &expr_mem_pool, &eval);
    evals.push_back(eval);
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
  for (int i = 0; i < evals.size(); ++i) {
    ScalarExprEvaluator* eval = evals[i];
    status = eval->Open(&state);
    if (!status.ok()) goto error;

    void* result = eval->GetValue(nullptr);
    status = eval->GetError();
    if (!status.ok()) goto error;
    // 'output_scale' should only be set for MathFunctions::RoundUpTo()
    // with return type double.
    const ColumnType& type = eval->root().type();
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
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &params), env,
      JniUtil::internal_exc_class(), nullptr);

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
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &lookup), env,
      JniUtil::internal_exc_class(), nullptr);

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

// Add a catalog update to pending_topic_updates_.
extern "C"
JNIEXPORT jboolean JNICALL
Java_org_apache_impala_service_FeSupport_NativeAddPendingTopicItem(JNIEnv* env,
    jclass caller_class, jlong native_catalog_server_ptr, jstring key,
    jbyteArray serialized_object, jboolean deleted) {
  std::string key_string;
  {
    JniUtfCharGuard key_str;
    if (!JniUtfCharGuard::create(env, key, &key_str).ok()) {
      return static_cast<jboolean>(false);
    }
    key_string.assign(key_str.get());
  }
  JniScopedArrayCritical obj_buf;
  if (!JniScopedArrayCritical::Create(env, serialized_object, &obj_buf)) {
    return static_cast<jboolean>(false);
  }
  reinterpret_cast<CatalogServer*>(native_catalog_server_ptr)->AddPendingTopicItem(
      std::move(key_string), obj_buf.get(), static_cast<uint32_t>(obj_buf.size()),
      deleted);
  return static_cast<jboolean>(true);
}

// Get the next catalog update pointed by 'callback_ctx'.
extern "C"
JNIEXPORT jobject JNICALL
Java_org_apache_impala_service_FeSupport_NativeGetNextCatalogObjectUpdate(JNIEnv* env,
    jclass caller_class, jlong native_iterator_ptr) {
  return reinterpret_cast<JniCatalogCacheUpdateIterator*>(native_iterator_ptr)->next(env);
}

extern "C"
JNIEXPORT jboolean JNICALL
Java_org_apache_impala_service_FeSupport_NativeLibCacheSetNeedsRefresh(JNIEnv* env,
    jclass caller_class, jstring hdfs_location) {
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
    jclass caller_class, jstring hdfs_lib_file) {
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
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TPrioritizeLoadRequest request;
  THROW_IF_ERROR_RET(DeserializeThriftMsg(env, thrift_struct, &request), env,
      JniUtil::internal_exc_class(), nullptr);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), NULL, NULL);
  TPrioritizeLoadResponse result;
  Status status = catalog_op_executor.PrioritizeLoad(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    status.AddDetail("Error making an RPC call to Catalog server.");
    status.ToThrift(&result.status);
  }

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Used to call native code from the FE to parse and set comma-delimited key=value query
// options.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_org_apache_impala_service_FeSupport_NativeParseQueryOptions(
    JNIEnv* env, jclass caller_class, jstring csv_query_options,
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

// Returns the log (base 2) of the minimum number of bytes we need for a Bloom filter
// with 'ndv' unique elements and a false positive probability of less than 'fpp'.
extern "C"
JNIEXPORT jint JNICALL
Java_org_apache_impala_service_FeSupport_MinLogSpaceForBloomFilter(
    JNIEnv* env, jclass caller_class, jlong ndv, jdouble fpp) {
  return BloomFilter::MinLogSpace(ndv, fpp);
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
  {
      (char*)"NativeParseQueryOptions", (char*)"(Ljava/lang/String;[B)[B",
      (void*)::Java_org_apache_impala_service_FeSupport_NativeParseQueryOptions
  },
  {
      (char*)"NativeAddPendingTopicItem", (char*)"(JLjava/lang/String;[BZ)Z",
      (void*)::Java_org_apache_impala_service_FeSupport_NativeAddPendingTopicItem
  },
  {
      (char*)"NativeGetNextCatalogObjectUpdate",
      (char*)"(J)Lorg/apache/impala/common/Pair;",
      (void*)::Java_org_apache_impala_service_FeSupport_NativeGetNextCatalogObjectUpdate
  },
  {
      (char*)"NativeLibCacheSetNeedsRefresh", (char*)"(Ljava/lang/String;)Z",
      (void*)::Java_org_apache_impala_service_FeSupport_NativeLibCacheSetNeedsRefresh
  },
  {
      (char*)"NativeLibCacheRemoveEntry", (char*)"(Ljava/lang/String;)Z",
      (void*)::Java_org_apache_impala_service_FeSupport_NativeLibCacheRemoveEntry
  },
  {
    (char*)"MinLogSpaceForBloomFilter", (char*)"(JD)I",
    (void*)::Java_org_apache_impala_service_FeSupport_MinLogSpaceForBloomFilter
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
