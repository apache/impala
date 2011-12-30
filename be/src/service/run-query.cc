// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>
#include <google/heap-profiler.h>
#include <google/profiler.h>

#include "common/status.h"
#include "exec/hbase-table-scanner.h"
#include "testutil/query-executor.h"
#include "service/plan-executor-adaptor.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "util/jni-util.h"
#include "util/perf-counters.h"

DEFINE_string(query, "", "query to execute");
DEFINE_bool(init_hbase, true, "if true, call hbase jni initialization");
DEFINE_string(profile_output_file, "pprof.out", "google pprof output file");
DEFINE_int32(iterations, 1, "Number of times to run the query (for perf testing)");

using namespace std;
using namespace impala;

static void Exec() {
  bool enable_profiling = false;
  if (FLAGS_profile_output_file.size() != 0) {
    ProfilerStart(FLAGS_profile_output_file.c_str());
    enable_profiling = true;
  }

  for (int i = 0; i < FLAGS_iterations; ++i) {
    QueryExecutor executor;
    EXIT_IF_ERROR(executor.Setup());

    struct timeval start_time;
    gettimeofday(&start_time, NULL);

    PerfCounters counters;
    counters.AddDefaultCounters();
    counters.Snapshot("Setup");

    EXIT_IF_ERROR(executor.Exec(FLAGS_query, NULL));

    int num_rows = 0;
    while (true) {
      string row;
      EXIT_IF_ERROR(executor.FetchResult(&row));
      if (row.empty()) break;
      cout << row << endl;
      ++num_rows;
    }

    // Print runtime errors, e.g., parsing errors.
    cout << executor.ErrorString() << endl;

    // Print file errors.
    cout << executor.FileErrors() << endl;

    counters.Snapshot("Query");

    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    double elapsed_usec = end_time.tv_sec * 1000000 + end_time.tv_usec;
    elapsed_usec -= start_time.tv_sec * 1000000 + start_time.tv_usec;

    cout << "returned " << num_rows << (num_rows == 1 ? " row" : " rows")
        << " in " << setiosflags(ios::fixed) << setprecision(3)
        << elapsed_usec/1000000.0 << " s" << endl << endl;

    counters.PrettyPrint(&cout);
  }

  if (enable_profiling) {
    const char* profile = GetHeapProfile();
    fputs(profile, stdout);
    free(const_cast<char*>(profile));
    ProfilerStop();
  }
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  EXIT_IF_ERROR(JniUtil::Init());
  if (FLAGS_init_hbase) {
    EXIT_IF_ERROR(HBaseTableScanner::Init());
    EXIT_IF_ERROR(RuntimeState::InitHBaseConf());
  }
  EXIT_IF_ERROR(PlanExecutorAdaptor::Init());

  Exec();

  // Delete all global JNI references.
  JniUtil::Cleanup();
}

