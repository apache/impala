// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>

#include "common/status.h"
#include "exec/hbase-table-scanner.h"
#include "testutil/query-executor.h"
#include "service/plan-executor-adaptor.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "util/jni-util.h"

DEFINE_string(query, "", "query to execute");
DEFINE_bool(init_hbase, true, "if true, call hbase jni initialization");
DEFINE_int32(batch_size, 0, "backend's batch size");

using namespace std;
using namespace impala;

static void Exec() {
  QueryExecutor executor;
  EXIT_IF_ERROR(executor.Setup());

  struct timeval start_time;
  gettimeofday(&start_time, NULL);

  // Arbitrary default values for reporting runtime errors/warnings.
  bool abort_on_error = false;
  int max_errors = 100;

  EXIT_IF_ERROR(
      executor.Exec(FLAGS_query, NULL, FLAGS_batch_size, abort_on_error, max_errors));

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

  struct timeval end_time;
  gettimeofday(&end_time, NULL);
  double elapsed_usec = end_time.tv_sec * 1000000 + end_time.tv_usec;
  elapsed_usec -= start_time.tv_sec * 1000000 + start_time.tv_usec;

  cout << "returned " << num_rows << (num_rows == 1 ? " row" : " rows")
       << " in " << setiosflags(ios::fixed) << setprecision(3)
       << elapsed_usec/1000000.0 << " s" << endl;
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

