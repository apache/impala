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

#include <stdio.h>
#include <iostream>

#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"

#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/timezone_db.h"
#include "util/benchmark.h"

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "common/thread-debug-info.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "service/impala-server.h"

#include "common/names.h"

DECLARE_string(hdfs_zone_info_zip);

using namespace apache::thrift;
using namespace impala;

// Struct holding reference to RuntimeState, FragmentState, ScalarExpr, and
// ScalarExprEvaluator for a specific expression.
// This ExprTestData should be obtained through Planner::PrepareScalarExpression() that
// will populate all the objects and open the ScalarExprEvaluator.
struct ExprTestData {
  ExprTestData(RuntimeState* state, FragmentState* fragment_state, ScalarExpr* expr,
      ScalarExprEvaluator* eval)
    : state_(state),
      fragment_state_(fragment_state),
      expr_(expr),
      eval_(eval),
      dummy_result_(0) {}

  ~ExprTestData() {
    eval_->Close(state_);
    expr_->Close();
    fragment_state_->ReleaseResources();
    state_->ReleaseResources();
  }

  RuntimeState* state_;
  FragmentState* fragment_state_;
  ScalarExpr* expr_;
  ScalarExprEvaluator* eval_;
  int64_t dummy_result_;
};

// Utility class to take (ascii) sql and return the plan.  This does minimal
// error handling.
class Planner {
 public:
  Planner() : expr_perm_pool_(&tracker_), expr_results_pool_(&tracker_) {
    frontend_.SetCatalogIsReady();
    ABORT_IF_ERROR(exec_env_.InitForFeSupport());
    query_options_.enable_expr_rewrites = false;
  }

  // Tell planner to enable/disable codegen on PrepareScalarExpression.
  void EnableCodegen(bool enable) { query_options_.__set_disable_codegen(!enable); }

  // Create ExprTestData for a given query.
  // Returned *test_data contains valid ExprTestData* in which eval_ is already opened.
  // Assumes 'query' is a constant query.
  Status PrepareScalarExpression(const string& query, ExprTestData** test_data) {
    TExecRequest request;
    TQueryCtx query_ctx;
    query_ctx.__set_session(session_state_);
    query_ctx.client_request.__set_stmt(query);
    query_ctx.client_request.__set_query_options(query_options_);
    string dummy_hostname = "";
    NetworkAddressPB dummy_addr;
    ImpalaServer::PrepareQueryContext(dummy_hostname, dummy_addr, &query_ctx);

    RuntimeState* state = pool_.Add(new RuntimeState(query_ctx, &exec_env_));
    TPlanFragment* fragment = state->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx = state->obj_pool()->Add(new PlanFragmentCtxPB());

    FragmentState* fragment_state = state->obj_pool()->Add(
        new FragmentState(state->query_state(), *fragment, *fragment_ctx));
    RETURN_IF_ERROR(frontend_.GetExecRequest(query_ctx, &request));

    // For query "select 1 + 1", planner will return query plan:
    //
    // F00:PLAN FRAGMENT [UNPARTITIONED] hosts=1 instances=1
    // |  Per-Host Resources: mem-estimate=4.00MB mem-reservation=4.00MB thread-reservation=1
    // PLAN-ROOT SINK
    // |  output exprs: 1 + 1
    // |  mem-estimate=4.00MB mem-reservation=4.00MB spill-buffer=2.00MB thread-reservation=0
    // |
    // 00:UNION
    //    constant-operands=1
    //    mem-estimate=0B mem-reservation=0B thread-reservation=0
    //    tuple-ids=0 row-size=2B cardinality=1
    //
    // However, the TExpr for the scalar expression is not in the output_sink's
    // output_exprs. Instead, it is in the union node's const_expr_lists.
    const TQueryExecRequest& query_request = request.query_exec_request;
    TUnionNode union_node =
        query_request.plan_exec_info[0].fragments[0].plan.nodes[0].union_node;
    vector<TExpr> texprs = union_node.const_expr_lists[0];
    DCHECK_EQ(texprs.size(), 1);

    ScalarExpr* expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(texprs[0], RowDescriptor(), fragment_state, &expr));
    ScalarExprEvaluator* eval;
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(
        *expr, state, state->obj_pool(), &expr_perm_pool_, &expr_results_pool_, &eval));

    // UDFs which cannot be interpreted need to be handled by codegen.
    // This follow examples from fe-support.cc
    if (fragment_state->ScalarExprNeedsCodegen()) {
      RETURN_IF_ERROR(fragment_state->CreateCodegen());
      LlvmCodeGen* codegen = fragment_state->codegen();
      DCHECK(codegen != NULL);
      RETURN_IF_ERROR(fragment_state->CodegenScalarExprs());
      codegen->EnableOptimizations(true);
      RETURN_IF_ERROR(codegen->FinalizeModule());
    }
    RETURN_IF_ERROR(eval->Open(state));

    *test_data = pool_.Add(new ExprTestData(state, fragment_state, expr, eval));
    return Status::OK();
  }

  void ClearExprPermPool() { expr_perm_pool_.Clear(); }

  void ClearExprResultsPool() { expr_results_pool_.Clear(); }

 private:
  Frontend frontend_;
  ExecEnv exec_env_;

  TQueryOptions query_options_;
  TSessionState session_state_;

  ObjectPool pool_;
  MemTracker tracker_;
  MemPool expr_perm_pool_;
  MemPool expr_results_pool_;
};

Planner* planner;

static string BenchmarkName(const string& name, bool codegen) {
  if (!codegen) {
    return name;
  } else {
    return name + "Codegen";
  }
}

static ExprTestData* GenerateBenchmarkExprs(const string& query) {
  stringstream ss;
  ss << "select " << query;
  ExprTestData* test_data = nullptr;
  ABORT_IF_ERROR(planner->PrepareScalarExpression(ss.str(), &test_data));
  if (test_data == nullptr) {
    ABORT_WITH_ERROR("Failed to prepare ExprTestData for query '" + ss.str() + "'");
  } else {
    return test_data;
  }
}

const int ITERATIONS = 256;

// Benchmark driver to run expr multiple times.
void BenchmarkQueryFn(int batch_size, void* d) {
  ExprTestData* data = reinterpret_cast<ExprTestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    for (int n = 0; n < ITERATIONS; ++n) {
      void* value = data->eval_->GetValue(NULL);
      // Dummy result to prevent this from being optimized away
      data->dummy_result_ += reinterpret_cast<int64_t>(value);
    }
  }
}

void SetupBenchmark(void* d) {
  planner->ClearExprResultsPool();
}

#define BENCHMARK(name, stmt) \
  suite->AddBenchmark(name, BenchmarkQueryFn, GenerateBenchmarkExprs(stmt))
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// Literals:                  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                 int                700      706      710         1X         1X         1X
//                               float                397      400      402     0.567X     0.566X     0.567X
//                              double                396      401      403     0.566X     0.568X     0.568X
//                              string                668      674      676     0.954X     0.954X     0.952X
//
// LiteralsCodegen:           Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                 int                780      786      790         1X         1X         1X
//                               float                610      615      617     0.783X     0.783X     0.781X
//                              double                610      616      619     0.782X     0.784X     0.783X
//                              string                780      786      790         1X         1X         1X
Benchmark* BenchmarkLiterals(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("Literals", codegen));
  BENCHMARK("int", "1");
  BENCHMARK("float", "1.1f");
  BENCHMARK("double", "1.1");
  BENCHMARK("string", "'1.1'");
  return suite;
}

// Arithmetic:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             int-add                319      323      324         1X         1X         1X
//                          double-add                 40     40.3     40.5     0.125X     0.125X     0.125X
//
// ArithmeticCodegen:         Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             int-add                781      787      791         1X         1X         1X
//                          double-add                610      617      619     0.781X     0.784X     0.783X
Benchmark* BenchmarkArithmetic(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("Arithmetic", codegen));
  BENCHMARK("int-add", "1 + 2");
  BENCHMARK("double-add", "1.1 + 2.2");
  return suite;
}

// Like:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                              equals                254      255      257         1X         1X         1X
//                          not equals                320      322      324      1.26X      1.26X      1.26X
//                              strstr                106      107      107     0.419X     0.419X     0.418X
//                            strncmp1                215      216      217     0.848X     0.847X     0.845X
//                            strncmp2                209      211      211     0.825X     0.825X     0.822X
//                            strncmp3                254      256      257         1X         1X         1X
//                               regex               28.1     28.2     28.3     0.111X      0.11X      0.11X
//
// LikeCodegen:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                              equals                820      827      830         1X         1X         1X
//                          not equals                824      829      836         1X         1X      1.01X
//                              strstr                130      131      131     0.158X     0.158X     0.158X
//                            strncmp1                334      337      339     0.407X     0.408X     0.408X
//                            strncmp2                355      359      360     0.433X     0.434X     0.434X
//                            strncmp3                468      472      474     0.571X     0.571X     0.572X
//                               regex               29.3     29.5     29.6    0.0357X    0.0357X    0.0357X
Benchmark* BenchmarkLike(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("Like", codegen));
  BENCHMARK("equals", "'abcdefghijklmnopqrstuvwxyz' = 'abcdefghijklmnopqrstuvwxyz'");
  BENCHMARK("not equals", "'abcdefghijklmnopqrstuvwxyz' = 'lmnopqrstuvwxyz'");
  BENCHMARK("strstr", "'abcdefghijklmnopqrstuvwxyz' LIKE '%lmnopq%'");
  BENCHMARK("strncmp1", "'abcdefghijklmnopqrstuvwxyz' LIKE '%xyz'");
  BENCHMARK("strncmp2", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc%'");
  BENCHMARK("strncmp3", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc'");
  BENCHMARK("regex", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc%z'");
  return suite;
}

// Machine Info: AMD EPYC 7K62 48-Core Processor
// Cast:                      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          int_to_int                161      167      169         1X         1X         1X
//                         int_to_bool                191      201      205      1.18X       1.2X      1.21X
//                       int_to_double                506      522      529      3.14X      3.13X      3.13X
//                       int_to_string               28.2     29.3     30.1     0.175X     0.176X     0.178X
//                   double_to_boolean               67.6     69.7     71.4      0.42X     0.418X     0.422X
//                    double_to_bigint               48.1       49     49.8     0.299X     0.294X     0.295X
//                   decimal_to_string               22.4     22.8       23     0.139X     0.137X     0.136X
//                    double_to_string               7.55     7.75     7.83    0.0468X    0.0464X    0.0463X
//                     float_to_string               7.97     8.15     8.24    0.0495X    0.0489X    0.0488X
//                       string_to_int                138      142      147     0.859X     0.854X      0.87X
//                     string_to_float               57.7     59.3     60.2     0.358X     0.355X     0.356X
//                 string_to_timestamp               22.4     23.2     23.5     0.139X     0.139X     0.139X
//
// CastCodegen:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          int_to_int                166      167      169         1X         1X         1X
//                         int_to_bool                198      202      204      1.19X      1.21X      1.21X
//                       int_to_double                521      526      531      3.14X      3.15X      3.14X
//                       int_to_string               28.9     29.7     30.5     0.174X     0.178X      0.18X
//                   double_to_boolean               68.7     70.1     71.4     0.414X     0.419X     0.422X
//                    double_to_bigint               48.4     49.2     49.8     0.292X     0.294X     0.295X
//                   decimal_to_string               22.5     22.8     23.2     0.136X     0.137X     0.137X
//                    double_to_string               7.64     7.75     7.83     0.046X    0.0463X    0.0463X
//                     float_to_string               8.02     8.15      8.3    0.0483X    0.0487X    0.0491X
//                       string_to_int                140      145      147     0.847X     0.869X     0.868X
//                     string_to_float               58.6     59.4     60.6     0.353X     0.355X     0.358X
//                 string_to_timestamp                 23     23.3     23.7     0.138X     0.139X      0.14X
Benchmark* BenchmarkCast(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("Cast", codegen));
  BENCHMARK("int_to_int", "cast(1 as INT)");
  BENCHMARK("int_to_bool", "cast(1 as BOOLEAN)");
  BENCHMARK("int_to_double", "cast(1 as DOUBLE)");
  BENCHMARK("int_to_string", "cast(1 as STRING)");
  BENCHMARK("double_to_boolean", "cast(3.14 as BOOLEAN)");
  BENCHMARK("double_to_bigint", "cast(3.14 as BIGINT)");
  BENCHMARK("decimal_to_string", "cast(3.14 as STRING)");
  BENCHMARK("double_to_string", "cast(cast(3.14 as DOUBLE) as STRING)");
  BENCHMARK("float_to_string", "cast(cast(3.14 as FLOAT) as STRING)");
  BENCHMARK("string_to_int", "cast('1234' as INT)");
  BENCHMARK("string_to_float", "cast('1234.5678' as FLOAT)");
  BENCHMARK("string_to_timestamp", "cast('2011-10-22 09:10:11' as TIMESTAMP)");
  return suite;
}

// DecimalCasts:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                     int_to_decimal4                104      105      105         1X         1X         1X
//                decimal4_to_decimal4                396      399      402       3.8X       3.8X      3.82X
//                decimal8_to_decimal4                 44       44     44.4     0.422X      0.42X     0.421X
//               decimal16_to_decimal4               31.3     31.6     31.7       0.3X     0.301X     0.301X
//                  double_to_decimal4               58.4     58.9     59.5      0.56X     0.562X     0.565X
//                  string_to_decimal4               50.9     51.1     51.4     0.488X     0.488X     0.488X
//                     int_to_decimal8                114      115      116      1.09X       1.1X       1.1X
//                decimal4_to_decimal8               51.3     51.6     52.1     0.492X     0.492X     0.494X
//                decimal8_to_decimal8                 44     44.4     44.6     0.422X     0.423X     0.423X
//               decimal16_to_decimal8               31.6     31.9       32     0.303X     0.304X     0.303X
//                  double_to_decimal8               60.6     60.8     61.1     0.582X     0.581X      0.58X
//                  string_to_decimal8               43.1     43.6       44     0.414X     0.416X     0.417X
//                    int_to_decimal16                110      112      113      1.06X      1.07X      1.07X
//               decimal4_to_decimal16               36.9       37     37.3     0.353X     0.353X     0.353X
//               decimal8_to_decimal16               43.1     43.5     43.8     0.414X     0.415X     0.415X
//              decimal16_to_decimal16               31.7     31.8     31.9     0.304X     0.303X     0.303X
//                 double_to_decimal16               54.2     54.5     54.7      0.52X      0.52X     0.519X
//                 string_to_decimal16               27.2     27.2     27.4     0.261X      0.26X      0.26X
//                 decimal4_to_tinyint               89.6     90.3       91      0.86X     0.862X     0.864X
//                 decimal8_to_tinyint               75.9     76.1     76.3     0.728X     0.726X     0.724X
//                decimal16_to_tinyint               50.1     50.1     50.5      0.48X     0.478X     0.479X
//                decimal4_to_smallint               85.6     85.9     86.4     0.821X      0.82X     0.819X
//                decimal8_to_smallint               77.2     77.6     77.8      0.74X      0.74X     0.738X
//               decimal16_to_smallint               50.5     50.6     50.9     0.484X     0.483X     0.482X
//                     decimal4_to_int               86.3     86.6     87.7     0.828X     0.826X     0.832X
//                     decimal8_to_int               76.9     77.1     77.5     0.738X     0.736X     0.735X
//                    decimal16_to_int               56.2     56.4     56.6     0.539X     0.538X     0.537X
//                  decimal4_to_bigint               89.6     89.8     90.4      0.86X     0.857X     0.857X
//                  decimal8_to_bigint               74.7     74.9     75.3     0.717X     0.715X     0.714X
//                 decimal16_to_bigint               57.2     57.2     57.5     0.549X     0.546X     0.546X
//                   decimal4_to_float                700      705      712      6.71X      6.73X      6.75X
//                   decimal8_to_float                700      704      711      6.72X      6.72X      6.74X
//                  decimal16_to_float                703      704      711      6.74X      6.72X      6.74X
//                  decimal4_to_double                700      709      712      6.72X      6.76X      6.75X
//                  decimal8_to_double                701      708      715      6.72X      6.76X      6.78X
//                 decimal16_to_double                702      705      712      6.73X      6.73X      6.75X
//                  decimal4_to_string               54.5       55     55.4     0.523X     0.525X     0.525X
//                  decimal8_to_string               53.3       54     54.4     0.512X     0.515X     0.516X
//                 decimal16_to_string               5.81     5.87     5.87    0.0557X     0.056X    0.0556X
//
// DecimalCastsCodegen:       Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                     int_to_decimal4                610      614      618         1X         1X         1X
//                decimal4_to_decimal4                608      615      618     0.996X         1X         1X
//                decimal8_to_decimal4                610      616      619         1X         1X         1X
//               decimal16_to_decimal4                610      613      617         1X     0.999X     0.998X
//                  double_to_decimal4                608      613      617     0.997X     0.998X     0.998X
//                  string_to_decimal4               83.2     83.5     84.1     0.136X     0.136X     0.136X
//                     int_to_decimal8                638      645      647      1.05X      1.05X      1.05X
//                decimal4_to_decimal8                638      642      648      1.05X      1.05X      1.05X
//                decimal8_to_decimal8                636      645      647      1.04X      1.05X      1.05X
//               decimal16_to_decimal8                639      642      645      1.05X      1.04X      1.04X
//                  double_to_decimal8                637      642      647      1.04X      1.05X      1.05X
//                  string_to_decimal8               79.9     82.5       83     0.131X     0.134X     0.134X
//                    int_to_decimal16                427      430      434       0.7X       0.7X     0.702X
//               decimal4_to_decimal16                426      431      433     0.698X     0.702X     0.701X
//               decimal8_to_decimal16                428      430      434     0.702X     0.701X     0.702X
//              decimal16_to_decimal16                426      431      435     0.698X     0.702X     0.703X
//                 double_to_decimal16                427      431      434       0.7X     0.702X     0.702X
//                 string_to_decimal16               36.7     36.9     37.1    0.0601X      0.06X      0.06X
//                 decimal4_to_tinyint                777      782      790      1.27X      1.27X      1.28X
//                 decimal8_to_tinyint                777      785      791      1.27X      1.28X      1.28X
//                decimal16_to_tinyint                778      784      788      1.28X      1.28X      1.27X
//                decimal4_to_smallint                776      781      790      1.27X      1.27X      1.28X
//                decimal8_to_smallint                778      787      790      1.28X      1.28X      1.28X
//               decimal16_to_smallint                777      783      791      1.27X      1.28X      1.28X
//                     decimal4_to_int                824      832      836      1.35X      1.35X      1.35X
//                     decimal8_to_int                823      831      837      1.35X      1.35X      1.35X
//                    decimal16_to_int                819      825      832      1.34X      1.34X      1.35X
//                  decimal4_to_bigint                777      783      790      1.27X      1.28X      1.28X
//                  decimal8_to_bigint                778      786      789      1.28X      1.28X      1.28X
//                 decimal16_to_bigint                780      783      791      1.28X      1.28X      1.28X
//                   decimal4_to_float                778      784      790      1.28X      1.28X      1.28X
//                   decimal8_to_float                778      782      791      1.28X      1.27X      1.28X
//                  decimal16_to_float                778      785      790      1.28X      1.28X      1.28X
//                  decimal4_to_double                777      784      789      1.27X      1.28X      1.28X
//                  decimal8_to_double                777      784      791      1.27X      1.28X      1.28X
//                 decimal16_to_double                778      784      790      1.28X      1.28X      1.28X
//                  decimal4_to_string                121      122      124     0.199X     0.199X       0.2X
//                  decimal8_to_string                121      122      123     0.198X     0.198X     0.199X
//                 decimal16_to_string               8.42     8.49     8.49    0.0138X    0.0138X    0.0137X
Benchmark* BenchmarkDecimalCast(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("DecimalCasts", codegen));
  BENCHMARK("int_to_decimal4", "cast(12345678 as DECIMAL(9,1))");
  BENCHMARK("decimal4_to_decimal4", "cast(12345678.5 as DECIMAL(9,1))");
  BENCHMARK("decimal8_to_decimal4", "cast(12345678.345 as DECIMAL(9,1))");
  BENCHMARK("decimal16_to_decimal4", "cast(12345678.123456783456789 as DECIMAL(9,1))");
  BENCHMARK("double_to_decimal4", "cast(e() as DECIMAL(9,7))");
  BENCHMARK("string_to_decimal4", "cast('12345678.123456783456789' as DECIMAL(9,1))");
  BENCHMARK("int_to_decimal8", "cast(12345678 as DECIMAL(18,2))");
  BENCHMARK("decimal4_to_decimal8", "cast(12345678.5 as DECIMAL(18,2))");
  BENCHMARK("decimal8_to_decimal8", "cast(12345678.345 as DECIMAL(18,2))");
  BENCHMARK("decimal16_to_decimal8", "cast(12345678.123456783456789 as DECIMAL(18,2))");
  BENCHMARK("double_to_decimal8", "cast(e() as DECIMAL(18,7))");
  BENCHMARK("string_to_decimal8", "cast('12345678.123456783456789' as DECIMAL(18,7))");
  BENCHMARK("int_to_decimal16", "cast(12345678 as DECIMAL(28,2))");
  BENCHMARK("decimal4_to_decimal16", "cast(12345678.5 as DECIMAL(28,2))");
  BENCHMARK("decimal8_to_decimal16", "cast(12345678.345 as DECIMAL(28,2))");
  BENCHMARK("decimal16_to_decimal16", "cast(12345678.123456783456789 as DECIMAL(28,2))");
  BENCHMARK("double_to_decimal16", "cast(e() as DECIMAL(28,7))");
  BENCHMARK("string_to_decimal16", "cast('12345678.123456783456789' as DECIMAL(28,7))");
  BENCHMARK("decimal4_to_tinyint", "cast(78.5 as TINYINT)");
  BENCHMARK("decimal8_to_tinyint", "cast(0.12345678345 as TINYINT)");
  BENCHMARK("decimal16_to_tinyint", "cast(78.12345678123456783456789 as TINYINT)");
  BENCHMARK("decimal4_to_smallint", "cast(78.5 as SMALLINT)");
  BENCHMARK("decimal8_to_smallint", "cast(0.12345678345 as SMALLINT)");
  BENCHMARK("decimal16_to_smallint", "cast(78.12345678123456783456789 as SMALLINT)");
  BENCHMARK("decimal4_to_int", "cast(12345678.5 as INT)");
  BENCHMARK("decimal8_to_int", "cast(12345678.345 as INT)");
  BENCHMARK("decimal16_to_int", "cast(12345678.123456783456789 as INT)");
  BENCHMARK("decimal4_to_bigint", "cast(12345678.5 as BIGINT)");
  BENCHMARK("decimal8_to_bigint", "cast(12345678.345 as BIGINT)");
  BENCHMARK("decimal16_to_bigint", "cast(12345678.123456783456789 as BIGINT)");
  BENCHMARK("decimal4_to_float", "cast(12345678.5 as FLOAT)");
  BENCHMARK("decimal8_to_float", "cast(12345678.345 as FLOAT)");
  BENCHMARK("decimal16_to_float", "cast(12345678.123456783456789 as FLOAT)");
  BENCHMARK("decimal4_to_double", "cast(12345678.5 as DOUBLE)");
  BENCHMARK("decimal8_to_double", "cast(12345678.345 as DOUBLE)");
  BENCHMARK("decimal16_to_double", "cast(12345678.123456783456789 as DOUBLE)");
  BENCHMARK("decimal4_to_string", "cast(12345678.5 as STRING)");
  BENCHMARK("decimal8_to_string", "cast(12345678.345 as STRING)");
  BENCHMARK("decimal16_to_string", "cast(12345678.123456783456789 as STRING)");
  return suite;
}

// ConditionalFn:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            not_null                353      357      359         1X         1X         1X
//                             is null                334      337      340     0.946X     0.944X     0.947X
//                            compound                300      303      305     0.851X     0.849X      0.85X
//                         int_between                154      155      156     0.437X     0.435X     0.436X
//                   timestamp_between               8.79     8.87     8.87    0.0249X    0.0248X    0.0247X
//                      string_between                128      129      130     0.364X     0.361X     0.362X
//                             bool_in                249      251      253     0.706X     0.703X     0.705X
//                              int_in                247      248      249     0.699X     0.695X     0.695X
//                            float_in                227      228      230     0.643X     0.638X      0.64X
//                           string_in                161      162      163     0.456X     0.453X     0.455X
//                        timestamp_in               9.35     9.35     9.35    0.0265X    0.0262X    0.0261X
//                              if_int                401      404      407      1.14X      1.13X      1.13X
//                           if_string                363      366      369      1.03X      1.03X      1.03X
//                        if_timestamp               37.9     38.2     38.3     0.107X     0.107X     0.107X
//                       coalesce_bool                201      202      204     0.569X     0.567X     0.568X
//                            case_int                104      105      106     0.295X     0.294X     0.295X
//
// ConditionalFnCodegen:      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            not_null                822      828      835         1X         1X         1X
//                             is null                819      824      829     0.996X     0.995X     0.993X
//                            compound                820      827      832     0.997X     0.998X     0.997X
//                         int_between                819      826      832     0.996X     0.997X     0.997X
//                   timestamp_between               11.4     11.4     11.5    0.0138X    0.0138X    0.0137X
//                      string_between                825      831      836         1X         1X         1X
//                             bool_in                474      477      482     0.577X     0.576X     0.577X
//                              int_in                561      566      570     0.682X     0.684X     0.682X
//                            float_in                563      565      569     0.684X     0.683X     0.682X
//                           string_in                352      355      357     0.428X     0.429X     0.428X
//                        timestamp_in               11.9       12       12    0.0144X    0.0145X    0.0144X
//                              if_int                777      783      789     0.945X     0.945X     0.945X
//                           if_string                778      783      789     0.947X     0.946X     0.945X
//                        if_timestamp                 45     45.2     45.4    0.0547X    0.0546X    0.0544X
//                       coalesce_bool                823      828      835         1X         1X         1X
//                            case_int                771      776      781     0.937X     0.937X     0.935X
Benchmark* BenchmarkConditionalFunctions(bool codegen) {
  // TODO: expand these cases when the parser issues are fixed (see corresponding tests
  // in expr-test).
  Benchmark* suite = new Benchmark(BenchmarkName("ConditionalFn", codegen));
  BENCHMARK("not_null", "!NULL");
  BENCHMARK("is null", "5 IS NOT NULL");
  BENCHMARK("compound", "(TRUE && TRUE) || FALSE");
  BENCHMARK("int_between", "5 between 5 and 6");
  BENCHMARK("timestamp_between", "cast('2011-10-22 09:10:11' as timestamp) between "
      "cast('2011-09-22 09:10:11' as timestamp) and "
      "cast('2011-12-22 09:10:11' as timestamp)");
  BENCHMARK("string_between", "'abc' between 'aaa' and 'aab'");
  BENCHMARK("bool_in", "true in (true, false, false)");
  BENCHMARK("int_in", "1 in (2, 3, 1)");
  BENCHMARK("float_in","1.1 not in (2, 3, 4.5)");
  BENCHMARK("string_in", "'ab' in ('cd', 'efg', 'ab', 'h')");
  BENCHMARK("timestamp_in", "cast('2011-11-23' as timestamp) "
        "in (cast('2011-11-22 09:10:11' as timestamp), "
        "cast('2011-11-23 09:11:12' as timestamp), "
        "cast('2011-11-24 09:12:13' as timestamp))");
  BENCHMARK("if_int", "if(TRUE, 10, 20)");
  BENCHMARK("if_string", "if(TRUE, 'abc', 'defgh')");
  BENCHMARK("if_timestamp", "if(TRUE, cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))");
  BENCHMARK("coalesce_bool", "coalesce(if(true, NULL, NULL), if(true, NULL, NULL))");
  BENCHMARK("case_int", "case 21 when 20 then 1 when 19 then 2 when 21 then 3 end");
  return suite;
}

// StringFn:                  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                              length                420      425      427         1X         1X         1X
//                          substring1                372      376      380     0.886X     0.886X     0.889X
//                          substring2                406      410      415     0.969X     0.966X      0.97X
//                                left                370      372      377     0.882X     0.875X     0.883X
//                               right                369      373      378      0.88X     0.879X     0.884X
//                               lower                159      161      162     0.379X     0.379X     0.379X
//                               upper                157      157      159     0.373X      0.37X     0.373X
//                             reverse                266      271      273     0.635X     0.637X     0.638X
//                                trim                446      452      455      1.06X      1.06X      1.06X
//                               ltrim                546      556      559       1.3X      1.31X      1.31X
//                               rtrim                545      553      556       1.3X       1.3X       1.3X
//                               space                372      376      380     0.888X     0.886X     0.889X
//                               ascii                809      817      826      1.93X      1.92X      1.93X
//                               instr                220      223      225     0.524X     0.526X     0.526X
//                              locate                217      218      219     0.517X     0.512X     0.511X
//                             locate2                229      230      232     0.545X     0.542X     0.543X
//                              concat                316      318      322     0.753X      0.75X     0.753X
//                             concat2                257      260      263     0.613X     0.611X     0.615X
//                            concatws                593      597      599      1.41X      1.41X       1.4X
//                           concatws2                228      229      232     0.543X     0.539X     0.543X
//                              repeat                208      209      212     0.495X     0.493X     0.497X
//                                lpad                169      170      170     0.403X       0.4X     0.399X
//                                rpad                167      169      170     0.397X     0.397X     0.397X
//                         find_in_set                321      323      325     0.765X     0.761X      0.76X
//                      regexp_extract                 42     42.8       43       0.1X     0.101X     0.101X
//                      regexp_replace               2.94     2.96        3   0.00701X   0.00698X   0.00702X
//                  prettyprint_bytes                9.54     9.72     9.75    0.0227X    0.0229X    0.0228X
//
// StringFnCodegen:           Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                              length                461      466      471         1X         1X         1X
//                          substring1                372      377      380     0.806X     0.809X     0.808X
//                          substring2                369      376      378     0.802X     0.806X     0.802X
//                                left                367      369      375     0.797X     0.792X     0.797X
//                               right                351      356      359     0.761X     0.764X     0.763X
//                               lower                160      161      162     0.347X     0.345X     0.344X
//                               upper                156      157      157     0.339X     0.336X     0.334X
//                             reverse                272      275      277     0.591X     0.591X     0.588X
//                                trim                445      450      453     0.966X     0.966X     0.963X
//                               ltrim                551      558      563       1.2X       1.2X       1.2X
//                               rtrim                544      552      557      1.18X      1.18X      1.18X
//                               space                374      377      380     0.811X     0.808X     0.807X
//                               ascii                805      813      823      1.75X      1.74X      1.75X
//                               instr                220      223      224     0.478X     0.479X     0.476X
//                              locate                219      220      220     0.475X     0.471X     0.468X
//                             locate2                225      227      231     0.489X     0.487X      0.49X
//                              concat                316      319      322     0.686X     0.683X     0.684X
//                             concat2                258      260      264      0.56X     0.557X      0.56X
//                            concatws                593      598      602      1.29X      1.28X      1.28X
//                           concatws2                227      229      232     0.493X      0.49X     0.493X
//                              repeat                203      205      208     0.441X     0.439X     0.441X
//                                lpad                169      170      170     0.367X     0.364X     0.362X
//                                rpad                169      169      170     0.366X     0.363X     0.361X
//                         find_in_set                320      323      325     0.694X     0.693X     0.691X
//                      regexp_extract               42.2       43     43.2    0.0915X    0.0922X    0.0918X
//                      regexp_replace               2.89     2.94        3   0.00626X   0.00631X   0.00637X
//                  prettyprint_bytes                9.55     9.72     9.74    0.0207X    0.0208X    0.0207X
Benchmark* BenchmarkStringFunctions(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("StringFn", codegen));
  BENCHMARK("length", "length('Hello World!')");
  BENCHMARK("substring1", "substring('Hello World!', 5)");
  BENCHMARK("substring2", "substring('Hello World!', 5, 5)");
  BENCHMARK("left", "strleft('Hello World!', 7)");
  BENCHMARK("right", "strleft('Hello World!', 7)");
  BENCHMARK("lower", "lower('Hello World!')");
  BENCHMARK("upper", "upper('Hello World!')");
  BENCHMARK("reverse", "reverse('Hello World!')");
  BENCHMARK("trim", "trim('  Hello World!  ')");
  BENCHMARK("ltrim", "ltrim('  Hello World!  ')");
  BENCHMARK("rtrim", "rtrim('  Hello World!  ')");
  BENCHMARK("space", "space(7)");
  BENCHMARK("ascii", "ascii('abcd')");
  BENCHMARK("instr", "instr('xyzabc', 'abc')");
  BENCHMARK("locate", "locate('abc', 'xyzabc')");
  BENCHMARK("locate2", "locate('abc', 'abcxyzabc', 3)");
  BENCHMARK("concat", "concat('a', 'bcd')");
  BENCHMARK("concat2", "concat('a', 'bb', 'ccc', 'dddd')");
  BENCHMARK("concatws", "concat_ws('a', 'b')");
  BENCHMARK("concatws2", "concat_ws('a', 'b', 'c', 'd')");
  BENCHMARK("repeat", "repeat('abc', 7)");
  BENCHMARK("lpad", "lpad('abc', 7, 'xyz')");
  BENCHMARK("rpad", "rpad('abc', 7, 'xyz')");
  BENCHMARK("find_in_set", "find_in_set('ab', 'abc,ad,ab,ade,cde')");
  BENCHMARK("regexp_extract", "regexp_extract('abxcy1234a', 'a.x.y.*a', 0)");
  BENCHMARK("regexp_replace", "regexp_replace('axcaycazc', '', 'r')");
  BENCHMARK("prettyprint_bytes", "prettyprint_bytes(987654321)");
  return suite;
}

// UrlFn:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           authority                113      114      114         1X         1X         1X
//                                file               93.7     94.3     94.6     0.829X     0.828X     0.827X
//                                host               90.2     90.6       91     0.798X     0.797X     0.796X
//                                path                104      105      106     0.924X     0.924X     0.923X
//                            protocol                112      112      113     0.989X     0.988X     0.986X
//                                user                105      105      106     0.925X     0.924X     0.923X
//                           user_info               93.2     93.8     94.2     0.825X     0.825X     0.823X
//                          query_name               44.4     44.5     44.8     0.393X     0.391X     0.392X
//
// UrlFnCodegen:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           authority                158      159      160         1X         1X         1X
//                                file                123      124      125      0.78X      0.78X     0.781X
//                                host                132      133      133     0.832X     0.834X     0.835X
//                                path                142      143      144     0.899X     0.899X     0.899X
//                            protocol                159      160      160      1.01X         1X         1X
//                                user                138      139      139     0.871X     0.871X     0.871X
//                           user_info                143      144      144     0.902X     0.902X     0.901X
//                          query_name               49.9     50.1     50.3     0.316X     0.315X     0.315X
Benchmark* BenchmarkUrlFunctions(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("UrlFn", codegen));
  BENCHMARK("authority", "parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')");
  BENCHMARK("file", "parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking   ', 'FILE')");
  BENCHMARK("host", "parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')");
  BENCHMARK("path", "parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')");
  BENCHMARK("protocol",
      "parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')");
  BENCHMARK("user", "parse_url('http://user@example.com/docs/books/tutorial/"
        "index.html?name=networking#DOWNLOADING', 'USERINFO')");
  BENCHMARK("user_info", "parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')");
  BENCHMARK("query_name", "parse_url('http://example.com:80/docs/books/tutorial/"
      "index.htmltest=true&name=networking&op=true', 'QUERY', 'name')");
  return suite;
}

// MathFn:                    Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                  pi                329      330      334         1X         1X         1X
//                                   e                339      343      346      1.03X      1.04X      1.04X
//                                 abs                144      145      146     0.438X     0.438X     0.437X
//                                  ln                126      127      128     0.383X     0.385X     0.384X
//                               log10                116      116      118     0.352X     0.351X     0.352X
//                                log2                131      131      132     0.397X     0.397X     0.394X
//                                 log               85.6     85.9     86.2      0.26X      0.26X     0.258X
//                                 pow               62.7     62.9     63.2     0.191X      0.19X     0.189X
//                                sqrt                315      318      320     0.957X     0.962X     0.958X
//                                sign                320      324      325     0.972X     0.981X     0.973X
//                                 sin                146      147      148     0.443X     0.444X     0.444X
//                                asin                244      247      250     0.741X     0.747X     0.748X
//                                 cos                134      135      136     0.407X     0.407X     0.407X
//                                acos                250      251      255     0.759X      0.76X     0.762X
//                                 tan                151      152      154     0.459X     0.459X      0.46X
//                                atan                136      137      138     0.414X     0.415X     0.413X
//                             radians                331      334      336         1X      1.01X         1X
//                             degrees                310      312      315     0.942X     0.943X     0.943X
//                                 bin               94.7     95.7     96.1     0.288X      0.29X     0.288X
//                            pmod_int                118      118      120     0.359X     0.358X     0.359X
//                          pmod_float                121      121      122     0.367X     0.367X     0.366X
//                            positive                308      310      313     0.937X     0.938X     0.936X
//                            negative                322      326      328     0.978X     0.986X     0.981X
//                                ceil               62.1     62.3       63     0.189X     0.189X     0.189X
//                               floor               58.3     58.6     58.9     0.177X     0.177X     0.176X
//                               round               60.6     61.1     61.4     0.184X     0.185X     0.184X
//                              round2               57.7     58.4     58.7     0.175X     0.177X     0.176X
//                             hex_int                8.6     8.68     8.68    0.0262X    0.0263X     0.026X
//                          hex_string               6.42     6.42     6.52    0.0195X    0.0194X    0.0195X
//                               unhex               4.81     4.81     4.81    0.0146X    0.0146X    0.0144X
//                            conv_int               57.8     58.2     58.5     0.176X     0.176X     0.175X
//                         conv_string               58.4     58.9     59.3     0.177X     0.178X     0.178X
//
// MathFnCodegen:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                  pi                777      786      791         1X         1X         1X
//                                   e                778      783      791         1X     0.996X         1X
//                                 abs                609      615      620     0.784X     0.782X     0.783X
//                                  ln                778      785      789         1X     0.999X     0.998X
//                               log10                778      783      793         1X     0.996X         1X
//                                log2                778      782      790         1X     0.995X     0.999X
//                                 log                777      785      787         1X     0.998X     0.995X
//                                 pow                777      784      790     0.999X     0.997X     0.999X
//                                sqrt                779      783      791         1X     0.996X         1X
//                                sign                777      786      791         1X         1X         1X
//                                 sin                778      784      790         1X     0.997X     0.999X
//                                asin                379      382      386     0.487X     0.487X     0.488X
//                                 cos                778      784      791         1X     0.998X         1X
//                                acos                391      393      397     0.503X       0.5X     0.502X
//                                 tan                778      785      790         1X     0.999X     0.999X
//                                atan                216      218      219     0.278X     0.277X     0.277X
//                             radians                777      784      790     0.999X     0.998X     0.998X
//                             degrees                779      784      789         1X     0.997X     0.998X
//                                 bin                166      168      170     0.214X     0.214X     0.215X
//                            pmod_int                777      784      789         1X     0.997X     0.997X
//                          pmod_float                778      783      792         1X     0.996X         1X
//                            positive                778      783      792         1X     0.996X         1X
//                            negative                778      782      790         1X     0.995X     0.999X
//                                ceil                609      614      620     0.784X     0.781X     0.783X
//                               floor                608      614      619     0.782X     0.781X     0.782X
//                               round                610      612      617     0.785X     0.778X      0.78X
//                              round2                608      613      619     0.782X     0.781X     0.782X
//                             hex_int               8.79     8.87     8.87    0.0113X    0.0113X    0.0112X
//                          hex_string               6.74     6.79     6.79   0.00868X   0.00864X   0.00859X
//                               unhex                  5        5        5   0.00643X   0.00636X   0.00632X
//                            conv_int                170      171      173     0.219X     0.218X     0.219X
//                         conv_string                189      191      193     0.243X     0.243X     0.244X
Benchmark* BenchmarkMathFunctions(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("MathFn", codegen));
  BENCHMARK("pi", "pi()");
  BENCHMARK("e", "e()");
  BENCHMARK("abs", "abs(-1.0)");
  BENCHMARK("ln", "ln(3.14)");
  BENCHMARK("log10", "log10(3.14)");
  BENCHMARK("log2", "log2(3.14)");
  BENCHMARK("log", "log(3.14, 5)");
  BENCHMARK("pow", "pow(3.14, 5)");
  BENCHMARK("sqrt", "sqrt(3.14)");
  BENCHMARK("sign", "sign(1.0)");
  BENCHMARK("sin", "sin(3.14)");
  BENCHMARK("asin", "asin(3.14)");
  BENCHMARK("cos", "cos(3.14)");
  BENCHMARK("acos", "acos(3.14)");
  BENCHMARK("tan", "tan(3.14)");
  BENCHMARK("atan", "atan(3.14)");
  BENCHMARK("radians", "radians(3.14)");
  BENCHMARK("degrees", "degrees(3.14)");
  BENCHMARK("bin", "bin(12345)");
  BENCHMARK("pmod_int", "pmod(12345, 12)");
  BENCHMARK("pmod_float", "pmod(12345.678, 12.34)");
  BENCHMARK("positive", "positive(12345)");
  BENCHMARK("negative", "negative(12345)");
  BENCHMARK("ceil", "ceil(-10.05)");
  BENCHMARK("floor", "floor(-10.05)");
  BENCHMARK("round", "round(-10.05)");
  BENCHMARK("round2", "round(-10.056789, 4)");
  BENCHMARK("hex_int", "hex(16)");
  BENCHMARK("hex_string", "hex('impala')");
  BENCHMARK("unhex", "hex('496D70616C61')");
  BENCHMARK("conv_int", "conv(100101, 2, 36)");
  BENCHMARK("conv_string", "conv('100101', 2, 36)");
  return suite;
}

// Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz
// TimestampFn:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             literal               10.8     11.6       12         1X         1X         1X
//                           to_string               4.34     4.91     5.02     0.402X     0.424X     0.419X
//                            add_year               6.17     7.11     7.29     0.572X     0.614X     0.609X
//                           sub_month               6.13     6.93     7.12     0.568X     0.598X     0.595X
//                           add_weeks               6.97     8.71     9.02     0.646X     0.752X     0.754X
//                            sub_days               6.73     8.34     8.57     0.624X     0.721X     0.716X
//                                 add               7.19     8.84     9.02     0.666X     0.763X     0.754X
//                           sub_hours               7.37     8.07     8.24     0.683X     0.697X     0.688X
//                         add_minutes               7.28     7.93     8.21     0.674X     0.685X     0.687X
//                         sub_seconds               7.63     7.89     8.14     0.707X     0.682X      0.68X
//                           add_milli               6.85      7.5     7.73     0.635X     0.648X     0.646X
//                           sub_micro               6.97     7.49     7.64     0.646X     0.647X     0.639X
//                            add_nano               6.79      7.4     7.62     0.629X     0.639X     0.637X
//                     unix_timestamp1               12.9       14     14.5      1.19X      1.21X      1.21X
//                     unix_timestamp2               18.1     19.9     20.4      1.67X      1.72X       1.7X
//                          from_unix1               9.54     10.7       11     0.884X     0.924X     0.918X
//                          from_unix2               14.4     16.1     16.7      1.34X      1.39X       1.4X
//                          from_unix3               9.79     10.9     11.3     0.907X     0.945X     0.948X
//                                year               12.4     14.2     14.5      1.15X      1.23X      1.21X
//                               month                 13     14.4     14.6       1.2X      1.24X      1.22X
//                        day of month               13.2     14.5     14.9      1.22X      1.25X      1.25X
//                         day of year               11.7     12.7     12.9      1.08X      1.09X      1.08X
//                        week of year               11.8     12.8     13.1      1.09X      1.11X       1.1X
//                     hour(timestamp)               6.71     7.26     7.41     0.622X     0.627X     0.619X
//                   minute(timestamp)               6.48     7.25     7.41     0.601X     0.626X     0.619X
//                   second(timestamp)               6.55     7.24     7.41     0.607X     0.626X     0.619X
//              millisecond(timestamp)               6.55     7.28     7.41     0.606X     0.629X     0.619X
//                        hour(string)                 10       11     11.2     0.927X     0.946X     0.933X
//                      minute(string)               9.91       11     11.2     0.918X     0.947X     0.933X
//                      second(string)                9.9     10.9     11.2     0.917X     0.942X     0.933X
//                 millisecond(string)               9.88     10.6       11     0.915X     0.916X     0.918X
//                             to date               6.07     6.98     7.25     0.563X     0.603X     0.606X
//                           date diff               5.75     6.39     6.55     0.533X     0.551X     0.547X
//                            from utc               7.56     8.25     8.55       0.7X     0.712X     0.714X
//                              to utc                6.6     7.28     7.49     0.612X     0.629X     0.626X
//                                 now                103      109      112       9.5X      9.43X       9.4X
//                      unix_timestamp               80.2     86.4     88.8      7.43X      7.46X      7.42X
//
// TimestampFnCodegen:        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             literal               11.4     11.8     12.1         1X         1X         1X
//                           to_string               4.83        5     5.09     0.425X     0.424X      0.42X
//                            add_year               6.98     7.18     7.36     0.615X     0.609X     0.607X
//                           sub_month               6.69     6.93     7.05     0.589X     0.588X     0.581X
//                           add_weeks               8.56     8.86     9.02     0.754X     0.752X     0.743X
//                            sub_days                8.1     8.39     8.58     0.714X     0.712X     0.707X
//                                 add               8.56     8.86     9.02     0.754X     0.752X     0.743X
//                           sub_hours               7.87     8.09     8.34     0.693X     0.687X     0.688X
//                         add_minutes               7.76        8     8.18     0.683X     0.679X     0.674X
//                         sub_seconds               7.75     7.95      8.1     0.683X     0.674X     0.668X
//                           add_milli                7.3     7.46      7.6     0.643X     0.633X     0.626X
//                           sub_micro               7.33     7.59     7.79     0.645X     0.644X     0.642X
//                            add_nano                7.2     7.46     7.59     0.634X     0.633X     0.625X
//                     unix_timestamp1               13.5       14     14.3      1.19X      1.19X      1.18X
//                     unix_timestamp2               19.3       20     20.4       1.7X       1.7X      1.68X
//                          from_unix1               10.3     10.6       11     0.908X     0.901X     0.905X
//                          from_unix2               15.8     16.4     16.9       1.4X      1.39X      1.39X
//                          from_unix3               10.5     11.1     11.4     0.922X     0.945X     0.937X
//                                year               14.3     14.8     15.1      1.26X      1.26X      1.24X
//                               month               14.3     14.8     15.1      1.26X      1.26X      1.24X
//                        day of month               14.3     14.6     14.9      1.26X      1.24X      1.23X
//                         day of year               12.6     13.2     13.4      1.11X      1.12X      1.11X
//                        week of year               12.2       13     13.4      1.08X       1.1X       1.1X
//                     hour(timestamp)               7.28     7.41      7.6     0.641X     0.629X     0.626X
//                   minute(timestamp)               7.16     7.41     7.55      0.63X     0.629X     0.622X
//                   second(timestamp)               7.16      7.4      7.6      0.63X     0.628X     0.626X
//              millisecond(timestamp)                7.2     7.41     7.59     0.634X     0.629X     0.625X
//                        hour(string)               10.5       11     11.3     0.928X      0.93X     0.929X
//                      minute(string)               10.5       11     11.2     0.921X      0.93X      0.92X
//                      second(string)               10.6       11     11.3     0.933X      0.93X      0.93X
//                 millisecond(string)               10.6       11     11.4     0.933X      0.93X     0.937X
//                             to date               6.43     7.11     7.23     0.566X     0.603X     0.596X
//                           date diff                6.3     6.43     6.55     0.554X     0.545X     0.539X
//                            from utc               8.14     8.57     8.73     0.716X     0.727X     0.719X
//                              to utc               7.13     7.41     7.55     0.628X     0.629X     0.622X
//                                 now                107      110      113      9.45X      9.36X      9.34X
//                      unix_timestamp               84.4     86.4     88.7      7.43X      7.33X      7.31X
Benchmark* BenchmarkTimestampFunctions(bool codegen) {
  Benchmark* suite = new Benchmark(BenchmarkName("TimestampFn", codegen));
  BENCHMARK("literal", "cast('2012-01-01 09:10:11.123456789' as timestamp)");
  BENCHMARK("to_string",
      "cast(cast('2012-01-01 09:10:11.123456789' as timestamp) as string)");
  BENCHMARK("add_year", "date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 years)");
  BENCHMARK("sub_month", "date_sub(cast('2012-02-29 09:10:11.123456789' "
      "as timestamp), interval 1 month)");
  BENCHMARK("add_weeks", "date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 53 weeks)");
  BENCHMARK("sub_days", "date_sub(cast('2011-12-22 09:10:11.12345678' "
      "as timestamp), interval 365 days)");
  BENCHMARK("add", "date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), 10)");
  BENCHMARK("sub_hours", "date_sub(cast('2012-01-02 01:00:00.123456789' "
      "as timestamp), interval 25 hours)");
  BENCHMARK("add_minutes", "date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 1533 minutes)");
  BENCHMARK("sub_seconds", "date_sub(cast('2012-01-02 01:00:33.123456789' "
      "as timestamp), interval 90033 seconds)");
  BENCHMARK("add_milli", "date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 90000033 milliseconds)");
  BENCHMARK("sub_micro", "date_sub(cast('2012-01-01 00:00:00.001033001' "
      "as timestamp), interval 1033 microseconds)");
  BENCHMARK("add_nano", "date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 1033 nanoseconds)");
  BENCHMARK("unix_timestamp1",
      "unix_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')");
  BENCHMARK("unix_timestamp2",
      "unix_timestamp('1970-10-01', 'yyyy-MM-dd')");
  BENCHMARK("from_unix1", "from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')");
  BENCHMARK("from_unix2", "from_unixtime(0, 'yyyy-MM-dd')");
  BENCHMARK("from_unix3", "from_unixtime(0)");
  BENCHMARK("year", "year(cast('2011-12-22' as timestamp))");
  BENCHMARK("month", "month(cast('2011-12-22' as timestamp))");
  BENCHMARK("day of month", "dayofmonth(cast('2011-12-22' as timestamp))");
  BENCHMARK("day of year", "dayofyear(cast('2011-12-22' as timestamp))");
  BENCHMARK("week of year", "weekofyear(cast('2011-12-22' as timestamp))");
  BENCHMARK("hour(timestamp)",
      "hour(cast('1970-01-01 09:10:11.130000' as timestamp))");
  BENCHMARK("minute(timestamp)",
      "minute(cast('1970-01-01 09:10:11.130000' as timestamp))");
  BENCHMARK(
      "second(timestamp)", "second(cast('1970-01-01 09:10:11.130000' as timestamp))");
  BENCHMARK(
      "millisecond(timestamp)", "millisecond(cast('1970-01-01 09:10:11.130000' as timestamp))");
  BENCHMARK("hour(string)", "hour('09:10:11.130000')");
  BENCHMARK("minute(string)", "minute('09:10:11.130000')");
  BENCHMARK("second(string)", "second('09:10:11.130000')");
  BENCHMARK("millisecond(string)", "millisecond('09:10:11.130000')");
  BENCHMARK("to date",
      "to_date(cast('2011-12-22 09:10:11.12345678' as timestamp))");
  BENCHMARK("date diff", "datediff(cast('2011-12-22 09:10:11.12345678' as timestamp), "
      "cast('2012-12-22' as timestamp))");
  BENCHMARK("from utc",
      "from_utc_timestamp(cast(1.3041352164485E9 as timestamp), 'PST')");
  BENCHMARK("to utc",
      "to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST')");
  BENCHMARK("now", "now()");
  BENCHMARK("unix_timestamp", "unix_timestamp()");
  return suite;
}

typedef Benchmark* (*SingleBenchmark)(bool);

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  ThreadDebugInfo debugInfo;

  // The host running this test might have an out-of-date tzdata package installed.
  // To avoid tzdata related issues, we will load time-zone db from the testdata
  // directory.
  FLAGS_hdfs_zone_info_zip =
      Substitute("file://$0/testdata/tzdb/2017c.zip", getenv("IMPALA_HOME"));
  ABORT_IF_ERROR(TimezoneDatabase::Initialize());

  // Dynamically construct at runtime as the planner initialization depends on
  // static objects being initialized in other compilation modules.
  planner = new Planner();

  // List benchmark functions that will be exercised.
  vector<SingleBenchmark> benchmarks;
  benchmarks.push_back(&BenchmarkLiterals);
  benchmarks.push_back(&BenchmarkArithmetic);
  benchmarks.push_back(&BenchmarkLike);
  benchmarks.push_back(&BenchmarkCast);
  benchmarks.push_back(&BenchmarkDecimalCast);
  benchmarks.push_back(&BenchmarkConditionalFunctions);
  benchmarks.push_back(&BenchmarkStringFunctions);
  benchmarks.push_back(&BenchmarkUrlFunctions);
  benchmarks.push_back(&BenchmarkMathFunctions);
  benchmarks.push_back(&BenchmarkTimestampFunctions);

  cout << Benchmark::GetMachineInfo() << endl;
  for (auto& benchmark : benchmarks) {
    for (int codegen = 0; codegen <= 1; codegen++) {
      planner->ClearExprPermPool();
      planner->EnableCodegen(codegen);
      Benchmark* suite = (*benchmark)(codegen);
      cout << suite->Measure(50, 10, SetupBenchmark) << endl;
    }
  }

  return 0;
}
