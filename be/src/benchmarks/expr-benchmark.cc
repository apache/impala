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

#include <jni.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"

#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "util/backend-gflag-util.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/jni-util.h"
#include "rpc/jni-thrift-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "rpc/thrift-server.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "service/impala-server.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace impala;

// Utility class to take (ascii) sql and return the plan.  This does minimal
// error handling.
class Planner {
 public:
  Planner() {
    frontend_.SetCatalogIsReady();
    ABORT_IF_ERROR(exec_env_.InitForFeTests());
  }

  Status GeneratePlan(const string& stmt, TExecRequest* result) {
    TQueryCtx query_ctx;
    query_ctx.client_request.stmt = stmt;
    query_ctx.client_request.query_options = query_options_;
    query_ctx.__set_session(session_state_);
    TNetworkAddress dummy;
    ImpalaServer::PrepareQueryContext(dummy, dummy, &query_ctx);
    runtime_state_.reset(new RuntimeState(query_ctx, &exec_env_));

    return frontend_.GetExecRequest(query_ctx, result);
  }

  RuntimeState* GetRuntimeState() {
    return runtime_state_.get();
  }

 private:
  Frontend frontend_;
  ExecEnv exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;

  TQueryOptions query_options_;
  TSessionState session_state_;
};

struct TestData {
  ScalarExprEvaluator* eval;
  int64_t dummy_result;
};

Planner* planner;
ObjectPool pool;
MemTracker tracker;
MemPool mem_pool(&tracker);

// Utility function to get prepare select list for exprs.  Assumes this is a
// constant query.
static Status PrepareSelectList(
    const TExecRequest& request, ScalarExprEvaluator** eval) {
  const TQueryExecRequest& query_request = request.query_exec_request;
  vector<TExpr> texprs = query_request.plan_exec_info[0].fragments[0].output_exprs;
  DCHECK_EQ(texprs.size(), 1);
  RuntimeState* state = planner->GetRuntimeState();
  ScalarExpr* expr;
  RETURN_IF_ERROR(ScalarExpr::Create(texprs[0], RowDescriptor(), state, &expr));
  RETURN_IF_ERROR(
      ScalarExprEvaluator::Create(*expr, state, &pool, &mem_pool, &mem_pool, eval));
  return Status::OK();
}

// TODO: handle codegen.  Codegen needs a new driver that is also codegen'd.
static TestData* GenerateBenchmarkExprs(const string& query, bool codegen) {
  stringstream ss;
  ss << "select " << query;
  TestData* test_data = new TestData;
  TExecRequest request;
  ABORT_IF_ERROR(planner->GeneratePlan(ss.str(), &request));
  ABORT_IF_ERROR(PrepareSelectList(request, &test_data->eval));
  return test_data;
}

const int ITERATIONS = 256;

// Benchmark driver to run expr multiple times.
void BenchmarkQueryFn(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    for (int n = 0; n < ITERATIONS; ++n) {
      void* value = data->eval->GetValue(NULL);
      // Dummy result to prevent this from being optimized away
      data->dummy_result += reinterpret_cast<int64_t>(value);
    }
  }
}

#define BENCHMARK(name, stmt)\
  suite->AddBenchmark(name, BenchmarkQueryFn, GenerateBenchmarkExprs(stmt, false))
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// Literals:             Function                Rate          Comparison
// ----------------------------------------------------------------------
//                            int                2154                  1X
//                          float                2155              1.001X
//                         double                2179              1.011X
//                         string                2179              1.011X
Benchmark* BenchmarkLiterals() {
  Benchmark* suite = new Benchmark("Literals");
  BENCHMARK("int", "1");
  BENCHMARK("float", "1.1f");
  BENCHMARK("double", "1.1");
  BENCHMARK("string", "'1.1'");
  return suite;
}

// Arithmetic:           Function                Rate          Comparison
// ----------------------------------------------------------------------
//                        int-add               527.5                  1X
//                     double-add                 528              1.001X
Benchmark* BenchmarkArithmetic() {
  Benchmark* suite = new Benchmark("Arithmetic");
  BENCHMARK("int-add", "1 + 2");
  BENCHMARK("double-add", "1.1 + 2.2");
  return suite;
}

// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// Like:                 Function                Rate          Comparison
// ----------------------------------------------------------------------
//                         equals               203.9                  1X
//                     not equals               426.4              2.091X
//                         strstr               142.8             0.7001X
//                       strncmp1               269.7              1.323X
//                       strncmp2               294.1              1.442X
//                       strncmp3               775.7              3.804X
//                          regex                19.7             0.0966X
Benchmark* BenchmarkLike() {
  Benchmark* suite = new Benchmark("Like");
  BENCHMARK("equals", "'abcdefghijklmnopqrstuvwxyz' = 'abcdefghijklmnopqrstuvwxyz'");
  BENCHMARK("not equals", "'abcdefghijklmnopqrstuvwxyz' = 'lmnopqrstuvwxyz'");
  BENCHMARK("strstr", "'abcdefghijklmnopqrstuvwxyz' LIKE '%lmnopq%'");
  BENCHMARK("strncmp1", "'abcdefghijklmnopqrstuvwxyz' LIKE '%xyz'");
  BENCHMARK("strncmp2", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc%'");
  BENCHMARK("strncmp3", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc'");
  BENCHMARK("regex", "'abcdefghijklmnopqrstuvwxyz' LIKE 'abc%z'");
  return suite;
}

// Cast:                 Function                Rate          Comparison
// ----------------------------------------------------------------------
//                     int_to_int                 824                  1X
//                    int_to_bool                 878              1.066X
//                  int_to_double               775.4              0.941X
//                  int_to_string               32.47            0.03941X
//              double_to_boolean               823.5             0.9994X
//               double_to_bigint               775.4              0.941X
//               double_to_string               4.682           0.005682X
//                  string_to_int               402.6             0.4886X
//                string_to_float               145.8             0.1769X
//            string_to_timestamp               83.76             0.1017X
Benchmark* BenchmarkCast() {
  Benchmark* suite = new Benchmark("Cast");
  BENCHMARK("int_to_int", "cast(1 as INT)");
  BENCHMARK("int_to_bool", "cast(1 as BOOLEAN)");
  BENCHMARK("int_to_double", "cast(1 as DOUBLE)");
  BENCHMARK("int_to_string", "cast(1 as STRING)");
  BENCHMARK("double_to_boolean", "cast(3.14 as BOOLEAN)");
  BENCHMARK("double_to_bigint", "cast(3.14 as BIGINT)");
  BENCHMARK("double_to_string", "cast(3.14 as STRING)");
  BENCHMARK("string_to_int", "cast('1234' as INT)");
  BENCHMARK("string_to_float", "cast('1234.5678' as FLOAT)");
  BENCHMARK("string_to_timestamp", "cast('2011-10-22 09:10:11' as TIMESTAMP)");
  return suite;
}

Benchmark* BenchmarkDecimalCast() {
  Benchmark* suite = new Benchmark("Decimal Casts");
  BENCHMARK("int_to_decimal4", "cast 12345678 as DECIMAL(9,2)");
  BENCHMARK("decimal4_to_decimal4", "cast 12345678.5 as DECIMAL(9,2)");
  BENCHMARK("decimal8_to_decimal4", "cast 12345678.345 as DECIMAL(9,2)");
  BENCHMARK("decimal16_to_decimal4", "cast 12345678.123456783456789 as DECIMAL(9,2)");
  BENCHMARK("double_to_decimal4", "cast e() as DECIMAL(9,7)");
  BENCHMARK("string_to_decimal4", "cast '12345678.123456783456789' as DECIMAL(9,2)");
  BENCHMARK("int_to_decimal8", "cast 12345678 as DECIMAL(18,2)");
  BENCHMARK("decimal4_to_decimal8", "cast 12345678.5 as DECIMAL(18,2)");
  BENCHMARK("decimal8_to_decimal8", "cast 12345678.345 as DECIMAL(18,2)");
  BENCHMARK("decimal16_to_decimal8", "cast 12345678.123456783456789 as DECIMAL(18,2)");
  BENCHMARK("double_to_decimal8", "cast e() as DECIMAL(18,7)");
  BENCHMARK("string_to_decimal8", "cast '12345678.123456783456789' as DECIMAL(18,7)");
  BENCHMARK("int_to_decimal16", "cast 12345678 as DECIMAL(28,2)");
  BENCHMARK("decimal4_to_decimal16", "cast 12345678.5 as DECIMAL(28,2)");
  BENCHMARK("decimal8_to_decimal16", "cast 12345678.345 as DECIMAL(28,2)");
  BENCHMARK("decimal16_to_decimal16", "cast 12345678.123456783456789 as DECIMAL(28,2)");
  BENCHMARK("double_to_decimal16", "cast e() as DECIMAL(28,7)");
  BENCHMARK("string_to_decimal16", "cast '12345678.123456783456789' as DECIMAL(28,7)");
  BENCHMARK("decimal4_to_tinyint", "cast 78.5 as TINYINT");
  BENCHMARK("decimal8_to_tinyint", "cast 0.12345678345 as TINYINT");
  BENCHMARK("decimal16_to_tinyint", "cast 78.12345678123456783456789 as TINYINT");
  BENCHMARK("decimal4_to_smallint", "cast 78.5 as SMALLINT");
  BENCHMARK("decimal8_to_smallint", "cast 0.12345678345 as SMALLINT");
  BENCHMARK("decimal16_to_smallint", "cast 78.12345678123456783456789 as SMALLINT");
  BENCHMARK("decimal4_to_int", "cast 12345678.5 as INT");
  BENCHMARK("decimal8_to_int", "cast 12345678.345 as INT");
  BENCHMARK("decimal16_to_int", "cast 12345678.123456783456789 as INT");
  BENCHMARK("decimal4_to_bigint", "cast 12345678.5 as BIGINT");
  BENCHMARK("decimal8_to_bigint", "cast 12345678.345 as BIGINT");
  BENCHMARK("decimal16_to_bigint", "cast 12345678.123456783456789 as BIGINT");
  BENCHMARK("decimal4_to_float", "cast 12345678.5 as FLOAT");
  BENCHMARK("decimal8_to_float", "cast 12345678.345 as FLOAT");
  BENCHMARK("decimal16_to_float", "cast 12345678.123456783456789 as FLOAT");
  BENCHMARK("decimal4_to_double", "cast 12345678.5 as DOUBLE");
  BENCHMARK("decimal8_to_double", "cast 12345678.345 as DOUBLE");
  BENCHMARK("decimal16_to_double", "cast 12345678.123456783456789 as DOUBLE");
  BENCHMARK("decimal4_to_string", "cast 12345678.5 as STRING");
  BENCHMARK("decimal8_to_string", "cast 12345678.345 as STRING");
  BENCHMARK("decimal16_to_string", "cast 12345678.123456783456789 as STRING");
  return suite;
}

// ConditionalFunctions: Function                Rate          Comparison
// ----------------------------------------------------------------------
//                       not_null               877.8                  1X
//                        is null               938.3              1.069X
//                       compound               240.2             0.2736X
//                    int_between                 191             0.2176X
//              timestamp_between                18.5            0.02108X
//                 string_between               93.94              0.107X
//                        bool_in               356.6             0.4063X
//                         int_in               209.7             0.2389X
//                       float_in               216.4             0.2465X
//                      string_in               120.1             0.1368X
//                   timestamp_in               19.79            0.02255X
//                         if_int               506.8             0.5773X
//                      if_string               470.6             0.5361X
//                   if_timestamp               70.19            0.07996X
//                  coalesce_bool               194.2             0.2213X
//                       case_int                 259             0.2951X
Benchmark* BenchmarkConditionalFunctions() {
// TODO: expand these cases when the parser issues are fixed (see corresponding tests
// in expr-test).
  Benchmark* suite = new Benchmark("ConditionalFunctions");
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

// StringFunctions:      Function                Rate          Comparison
// ----------------------------------------------------------------------
//                         length               920.2                  1X
//                     substring1               351.4             0.3819X
//                     substring2               327.9             0.3563X
//                           left               508.6             0.5527X
//                          right               508.2             0.5522X
//                          lower               103.9             0.1129X
//                          upper               103.2             0.1121X
//                        reverse               324.9             0.3531X
//                           trim               421.2             0.4578X
//                          ltrim               526.6             0.5723X
//                          rtrim               566.5             0.6156X
//                          space               94.63             0.1028X
//                          ascii                1048              1.139X
//                          instr               175.6             0.1909X
//                         locate               184.7             0.2007X
//                        locate2               175.8             0.1911X
//                         concat               109.5              0.119X
//                        concat2               75.83            0.08241X
//                       concatws               143.4             0.1559X
//                      concatws2               70.38            0.07649X
//                         repeat               98.54             0.1071X
//                           lpad               154.7             0.1681X
//                           rpad               145.6             0.1582X
//                    find_in_set               83.38            0.09061X
//                 regexp_extract                6.42           0.006977X
//                 regexp_replace              0.7435           0.000808X
Benchmark* BenchmarkStringFunctions() {
  Benchmark* suite = new Benchmark("StringFunctions");
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
  return suite;
}

// UrlFunctions:         Function                Rate          Comparison
// ----------------------------------------------------------------------
//                      authority               118.1                  1X
//                           file               95.52              0.809X
//                           host               94.52             0.8005X
//                           path               98.63             0.8353X
//                       protocol               36.29             0.3073X
//                           user               121.1              1.026X
//                      user_info               121.4              1.029X
//                     query_name               41.34             0.3501X
Benchmark* BenchmarkUrlFunctions() {
  Benchmark* suite = new Benchmark("UrlFunctions");
  BENCHMARK("authority", "parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')");
  BENCHMARK("file", "parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking   ', 'FILE')");
  BENCHMARK("host", "parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')");
  BENCHMARK("path", "parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')");
  BENCHMARK("protocol", "parse_url('user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')");
  BENCHMARK("user", "parse_url('http://user@example.com/docs/books/tutorial/"
        "index.html?name=networking#DOWNLOADING', 'USERINFO')");
  BENCHMARK("user_info", "parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')");
  BENCHMARK("query_name", "parse_url('http://example.com:80/docs/books/tutorial/"
      "index.htmltest=true&name=networking&op=true', 'QUERY', 'name')");
  return suite;
}

// MathFunctions:        Function                Rate          Comparison
// ----------------------------------------------------------------------
//                             pi                1642                  1X
//                              e                1546             0.9416X
//                            abs                 877             0.5342X
//                             ln               110.7            0.06744X
//                          log10               88.48             0.0539X
//                           log2               108.3            0.06597X
//                            log               55.61            0.03387X
//                            pow               53.93            0.03285X
//                           sqrt               629.6             0.3835X
//                           sign                 732             0.4459X
//                            sin               176.3             0.1074X
//                           asin               169.6             0.1033X
//                            cos               156.3             0.0952X
//                           acos               167.5              0.102X
//                            tan               176.3             0.1074X
//                           atan               153.8            0.09371X
//                        radians               601.5             0.3664X
//                        degrees               601.5             0.3664X
//                            bin                  45            0.02741X
//                       pmod_int               147.3            0.08976X
//                     pmod_float               172.9             0.1053X
//                       positive                 877             0.5342X
//                       negative               936.9             0.5707X
//                           ceil               466.7             0.2843X
//                          floor               390.9             0.2381X
//                          round               820.5             0.4998X
//                         round2               220.2             0.1341X
//                        hex_int               5.745             0.0035X
//                     hex_string               4.441           0.002705X
//                          unhex               3.394           0.002067X
//                       conv_int               33.15            0.02019X
//                    conv_string                36.6            0.02229X
Benchmark* BenchmarkMathFunctions() {
  Benchmark* suite = new Benchmark("MathFunctions");
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

// TimestampFunctions:   Function                Rate          Comparison
// ----------------------------------------------------------------------
//                        literal               68.18                  1X
//                      to_string               1.131            0.01659X
//                       add_year               34.57              0.507X
//                      sub_month               33.04             0.4846X
//                      add_weeks               56.15             0.8236X
//                       sub_days               57.21             0.8391X
//                            add               55.85             0.8191X
//                      sub_hours               44.44             0.6519X
//                    add_minutes               43.96             0.6448X
//                    sub_seconds               42.78             0.6274X
//                      add_milli               43.43             0.6371X
//                      sub_micro               43.88             0.6436X
//                       add_nano               41.83             0.6135X
//                unix_timestamp1               32.74             0.4803X
//                unix_timestamp2               39.39             0.5778X
//                     from_unix1               1.192            0.01748X
//                     from_unix2               1.602             0.0235X
//                           year                73.4              1.077X
//                          month               72.53              1.064X
//                   day of month               71.98              1.056X
//                    day of year               56.67             0.8312X
//                   week of year               50.68             0.7433X
//                           hour               100.1              1.468X
//                         minute               97.18              1.425X
//                         second                96.7              1.418X
//                        to date               3.075            0.04511X
//                      date diff               39.54             0.5799X
Benchmark* BenchmarkTimestampFunctions() {
  Benchmark* suite = new Benchmark("TimestampFunctions");
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
  BENCHMARK("year", "year(cast('2011-12-22' as timestamp))");
  BENCHMARK("month", "month(cast('2011-12-22' as timestamp))");
  BENCHMARK("day of month", "dayofmonth(cast('2011-12-22' as timestamp))");
  BENCHMARK("day of year", "dayofyear(cast('2011-12-22' as timestamp))");
  BENCHMARK("week of year", "weekofyear(cast('2011-12-22' as timestamp))");
  BENCHMARK("hour", "hour(cast('09:10:11.000000' as timestamp))");
  BENCHMARK("minute", "minute(cast('09:10:11.000000' as timestamp))");
  BENCHMARK("second", "second(cast('09:10:11.000000' as timestamp))");
  BENCHMARK("to date",
      "to_date(cast('2011-12-22 09:10:11.12345678' as timestamp))");
  BENCHMARK("date diff", "datediff(cast('2011-12-22 09:10:11.12345678' as timestamp), "
      "cast('2012-12-22' as timestamp))");
#if 0
  // TODO: need to create a valid runtime state for these functions
  BENCHMARK("from utc",
      "from_utc_timestamp(cast(1.3041352164485E9 as timestamp), 'PST')");
  BENCHMARK("to utc",
      "to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST')");
  BENCHMARK("now", "now()");
  BENCHMARK("unix_timestamp", "unix_timestamp()");
#endif
  return suite;
}

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());

  // Dynamically construct at runtime as the planner initialization depends on
  // static objects being initialized in other compilation modules.
  planner = new Planner();

  // Generate all the tests first (this does the planning)
  Benchmark* literals = BenchmarkLiterals();
  Benchmark* arithmetics = BenchmarkArithmetic();
  Benchmark* like = BenchmarkLike();
  Benchmark* cast = BenchmarkCast();
  Benchmark* decimal_cast = BenchmarkDecimalCast();
  Benchmark* conditional_fns = BenchmarkConditionalFunctions();
  Benchmark* string_fns = BenchmarkStringFunctions();
  Benchmark* url_fns = BenchmarkUrlFunctions();
  Benchmark* math_fns = BenchmarkMathFunctions();
  Benchmark* timestamp_fns = BenchmarkTimestampFunctions();

  cout << Benchmark::GetMachineInfo() << endl;
  cout << literals->Measure() << endl;
  cout << arithmetics->Measure() << endl;
  cout << like->Measure() << endl;
  cout << cast->Measure() << endl;
  cout << decimal_cast->Measure() << endl;
  cout << conditional_fns->Measure() << endl;
  cout << string_fns->Measure() << endl;
  cout << url_fns->Measure() << endl;
  cout << math_fns->Measure() << endl;
  cout << timestamp_fns->Measure() << endl;

  return 0;
}
