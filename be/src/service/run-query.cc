// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "testutil/query-executor.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"

DEFINE_string(query, "", "query to execute");

using namespace std;
using namespace impala;

#define EXIT_IF_ERROR(stmt) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string msg; \
      status.GetErrorMsg(&msg); \
      cerr << msg; \
      exit(1); \
    } \
  } while (false)


int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  QueryExecutor executor;
  EXIT_IF_ERROR(executor.Setup());

  struct timeval start_time;
  gettimeofday(&start_time, NULL);

  EXIT_IF_ERROR(executor.Exec(FLAGS_query));
  int num_rows = 0;
  while (true) {
    string row;
    EXIT_IF_ERROR(executor.FetchResult(&row));
    if (row.empty()) break;
    cout << row << endl;
    ++num_rows;
  }

  struct timeval end_time;
  gettimeofday(&end_time, NULL);
  double elapsed_usec = end_time.tv_sec * 1000000 + end_time.tv_usec;
  elapsed_usec -= start_time.tv_sec * 1000000 + start_time.tv_usec;

  cout << "returned " << num_rows << (num_rows == 1 ? " row" : " rows")
       << " in " << setiosflags(ios::fixed) << setprecision(3)
       << elapsed_usec/1000000.0 << " s" << endl;
}

