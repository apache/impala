// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>
#include <google/heap-profiler.h>
#include <google/profiler.h>
#include <server/TServer.h>
#include <boost/thread/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>

#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exec/hbase-table-scanner.h"
#include "testutil/query-executor.h"
#include "runtime/exec-env.h"
#include "testutil/test-exec-env.h"
#include "service/backend-service.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "util/jni-util.h"
#include "util/perf-counters.h"
#include "util/runtime-profile.h"
#include "util/debug-counters.h"
#include "util/stat-util.h"
#include "runtime/data-stream-mgr.h"

DEFINE_string(query, "", "query to execute.  Multiple queries can be ; separated");
DEFINE_bool(init_hbase, true, "if true, call hbase jni initialization");
DEFINE_string(profile_output_file, "pprof.out", "google pprof output file");
DEFINE_int32(iterations, 1, "Number of times to run the query (for perf testing)");
DEFINE_bool(enable_counters, true, "if false, disable using counters (so a profiler can use them");
DECLARE_int32(num_nodes);
DECLARE_int32(backend_port);
DECLARE_string(backends);

using namespace std;
using namespace impala;
using namespace boost;
using namespace apache::thrift::server;

static void Exec(ExecEnv* exec_env) {
  bool enable_profiling = false;
  if (FLAGS_enable_counters && FLAGS_profile_output_file.size() != 0) {
    ProfilerStart(FLAGS_profile_output_file.c_str());
    enable_profiling = true;
  }

  vector<double> elapsed_times;
  elapsed_times.resize(FLAGS_iterations);
  int num_rows = 0;

  vector<string> queries;
  split(queries, FLAGS_query, is_any_of(";"), token_compress_on );

  if (queries.size() == 0) {
    cout << "Invalid query: " << FLAGS_query << endl;
    return;
  }

  // If the number of iterations is greater than 1, run once to Ignore JVM startup time.
  if (FLAGS_iterations > 1) {
    QueryExecutor executor(exec_env);
    EXIT_IF_ERROR(executor.Setup());
    EXIT_IF_ERROR(executor.Exec(queries[0], NULL));
    while (true) {
      string row;
      EXIT_IF_ERROR(executor.FetchResult(&row));
      if (row.empty()) break;
    }
  }

  PerfCounters hw_counters;
  if (FLAGS_enable_counters) {
    hw_counters.AddDefaultCounters();
    hw_counters.Snapshot("Setup");
  }

  ObjectPool profile_pool;
  for (vector<string>::const_iterator iter = queries.begin();
      iter != queries.end(); ++iter) {
    if (iter->size() == 0) continue;

    if (queries.size() > 0) {
      cout << "Running query: " << *iter << endl;
    }

    RuntimeProfile aggregate_profile(&profile_pool, "RunQuery");

    for (int i = 0; i < FLAGS_iterations; ++i) {
      QueryExecutor executor(exec_env);
      EXIT_IF_ERROR(executor.Setup());

      struct timeval start_time;
      gettimeofday(&start_time, NULL);

      EXIT_IF_ERROR(executor.Exec(*iter, NULL));

      while (true) {
        string row;
        EXIT_IF_ERROR(executor.FetchResult(&row));
        if (row.empty()) break;
        // Only print results for first run
        if (i == 0) cout << row << endl;
        ++num_rows;
      }

      struct timeval end_time;
      gettimeofday(&end_time, NULL);
      double elapsed_usec = end_time.tv_sec * 1000000 + end_time.tv_usec;
      elapsed_usec -= start_time.tv_sec * 1000000 + start_time.tv_usec;
      elapsed_times[i] = elapsed_usec;

      if (FLAGS_enable_counters) {
        hw_counters.Snapshot("Query");
      }

      if (executor.ErrorString().size() > 0 || executor.FileErrors().size() > 0) {
        // Print runtime errors, e.g., parsing errors.
        cout << executor.ErrorString() << endl;
        // Print file errors.
        cout << executor.FileErrors() << endl;
        break;
      }

      RuntimeProfile* profile = executor.query_profile();
      if (FLAGS_iterations > 1 && profile->children().size() == 1) {
        // Rename the query to drop the query id so the results merge
        profile->children()[0]->Rename("Query");
      }
      aggregate_profile.Merge(*profile);
    }
  
    num_rows /= FLAGS_iterations;

    if (FLAGS_iterations == 1) {
      cout << "returned " << num_rows << (num_rows == 1 ? " row" : " rows")
          << " in " << setiosflags(ios::fixed) << setprecision(3)
          << elapsed_times[0]/1000000.0 << " s" << endl << endl;
    } else {
      double mean, stddev;
      StatUtil::ComputeMeanStddev<double>(&elapsed_times[0], elapsed_times.size(), &mean, &stddev);
      cout << "returned " << num_rows << (num_rows == 1 ? " row" : " rows")
          << " in " << setiosflags(ios::fixed) << setprecision(3)
          << mean/1000000.0 << " s with stddev "
          << setiosflags(ios::fixed) << setprecision(3) << stddev/1000000.0
          << " s" << endl << endl;
      aggregate_profile.Divide(FLAGS_iterations);
    }

    aggregate_profile.PrettyPrint(&cout);
    PRETTY_PRINT_DEBUG_COUNTERS(&cout);
    cout << endl;
  }

  if (FLAGS_enable_counters) {
    hw_counters.PrettyPrint(&cout);
  }

  if (enable_profiling) {
    const char* profile = GetHeapProfile();
    fputs(profile, stdout);
    free(const_cast<char*>(profile));
    ProfilerStop();
  }
}

static void RunServer(TServer* server) {
  server->serve();
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  scoped_ptr<ExecEnv> exec_env;
  if (FLAGS_backends.empty()) {
    TestExecEnv* test_exec_env = new TestExecEnv(
        FLAGS_num_nodes > 0 ? FLAGS_num_nodes - 1 : 4, FLAGS_backend_port + 1);
    test_exec_env->StartBackends();
    exec_env.reset(test_exec_env);
  } else {
    exec_env.reset(new ExecEnv());
  }
  if (FLAGS_num_nodes != 1) {
    // start backend service to feed stream_mgr
    TServer* server = StartImpalaBackendService(exec_env.get(), FLAGS_backend_port);
    thread server_thread = thread(&RunServer, server);
  }
  EXIT_IF_ERROR(JniUtil::Init());
  if (FLAGS_init_hbase) {
    EXIT_IF_ERROR(HBaseTableScanner::Init());
    EXIT_IF_ERROR(RuntimeState::InitHBaseConf());
  }

  Exec(exec_env.get());

  // Delete all global JNI references.
  JniUtil::Cleanup();
}
