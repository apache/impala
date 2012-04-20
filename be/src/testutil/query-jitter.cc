// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/heap-profiler.h>
#include <google/profiler.h>
#include <server/TServer.h>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>

#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "runtime/raw-value.h"
#include "service/backend-service.h"
#include "testutil/query-executor.h"
#include "util/benchmark.h"
#include "util/jni-util.h"
#include "util/perf-counters.h"
#include "util/runtime-profile.h"
#include "util/stat-util.h"

DEFINE_string(query, "", "query to jit.");
DEFINE_bool(enable_optimizations, false, "if true, enable jit optimizations");
DEFINE_bool(verify_jit, true, "if true, verifys jitted IR");
DEFINE_bool(benchmark, false, "if true, benchmarks the expr jitting");

using namespace std;
using namespace boost;
using namespace impala;

static const int NUM_NODES = 1;
static const bool ABORT_ON_ERROR = true;
static const int MAX_ERRORS = 1;

void ExprBenchmark(int iters, void* e) {
  Expr* expr = reinterpret_cast<Expr*>(e);
  for (int i = 0; i < iters; ++i) {
    expr->GetValue(NULL);
  }
}

namespace impala {
class QueryJitter {
 public:
  static void Exec() {
    if (FLAGS_query.length() == 0) {
      cerr << "Must specify query." << endl;
      return;
    }
    vector<PrimitiveType> col_types;

    ExecEnv exec_env;
    QueryExecutor exec(&exec_env);
    exec.DisableJit();
    Status status = exec.Setup();
    DCHECK(status.ok());
    status = exec.Exec(FLAGS_query, &col_types);
    DCHECK(status.ok());
    
    const TQueryExecRequest& request = exec.query_request();

    // we always need at least one plan fragment
    DCHECK_GT(request.fragmentRequests.size(), 0);

    vector<Expr*> output_exprs = exec.select_list_exprs();
    cout << "Exprs: " << Expr::DebugString(output_exprs) << endl;

    LlvmCodeGen code_gen("Expr Jit");
    code_gen.EnableOptimizations(FLAGS_enable_optimizations);
    code_gen.EnableVerifier(FLAGS_verify_jit);
    status = code_gen.Init();
    if (!status.ok()) {
      cerr << "Could not initialize code gen: " << status.GetErrorMsg() << endl;
      return;
    }

    cout << "Generating IR..." << endl;
    Expr* root = output_exprs[0];
    int scratch_size;
    void* func = root->CodegenExprTree(&code_gen, &scratch_size);
    if (func == NULL) {
      cout << "Could not jit expression tree." << endl;
      return;
    }

    string llvm_ir = code_gen.GetIR();
    cout << llvm_ir << endl;

    // No FROM clause, run the jitted expr tree.
    if (!request.fragmentRequests[0].__isset.descTbl) {
      if (FLAGS_benchmark) {
        double interpreted_rate = Benchmark::Measure(ExprBenchmark, root);
        root->SetComputeFn(func, scratch_size);
        double jitted_rate = Benchmark::Measure(ExprBenchmark, root);
        cout << "Interpreted Expr Eval Rate: " << interpreted_rate << endl;
        cout << "Jitted Expr Eval Rate: " << jitted_rate << endl;
      } else {
        root->SetComputeFn(func, scratch_size);
        void* result = root->GetValue(NULL);
        string result_string;
        RawValue::PrintValue(result, root->type(), &result_string);
        cout << "Result: " << result_string << endl;
      }

    } else {
      if (FLAGS_benchmark) {
        cout << "Cannot benchmark statements with FROM clause." << endl;
      }
    }
  }
};
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  JniUtil::Init();
  QueryJitter::Exec();
  JniUtil::Cleanup();
}

