// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <iomanip>
#include <jni.h>
#include <google/heap-profiler.h>
#include <google/profiler.h>
#include <server/TServer.h>
#include <boost/thread/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include "common/logging.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exec/hbase-table-scanner.h"
#include "runtime/hbase-table-cache.h"
#include "testutil/query-executor-if.h"
// TODO: fix this: we need to include uid-util.h apparently right before
// in-process-query-executor.h, it's not clear why
#include "util/uid-util.h"
#include "testutil/impalad-query-executor.h"
#include "runtime/exec-env.h"
#include "exec/exec-stats.h"
#include "testutil/test-exec-env.h"
#include "service/impala-server.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/perf-counters.h"
#include "util/runtime-profile.h"
#include "util/debug-counters.h"
#include "util/stat-util.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/authorization.h"
#include "runtime/data-stream-mgr.h"

DEFINE_string(exec_options, "", "key:value pair of execution options for impalad,"
    " separated by ;");
DEFINE_string(input_file, "", "file containing ';'-separated list of queries");
DEFINE_string(query, "", "query to execute.  Multiple queries can be ; separated");
DEFINE_bool(init_hbase, true, "if true, call hbase jni initialization");
DEFINE_string(profile_output_file, "pprof.out", "google pprof output file");
DEFINE_int32(iterations, 1, "Number of times to run the query (for perf testing)");
DEFINE_bool(enable_counters, true, "if false, disable using counters (so a profiler can use them");
DEFINE_bool(explain_plan, false, "if true, print the explain plan only");
DECLARE_int32(num_nodes);
DECLARE_int32(fe_port);
DECLARE_int32(be_port);
DECLARE_string(impalad);
DECLARE_bool(use_statestore);

using namespace std;
using namespace impala;
using namespace boost;
using namespace sasl;

// Creates a summary string for output to stdout once a query has finished
static void ConstructSummaryString(ExecStats::QueryType query_type, int num_rows,
                                   const vector<double>& elapsed_times, string* summary) {
  string verb(query_type == ExecStats::INSERT ? "inserted " : "returned ");
  stringstream summary_stream;
  if (FLAGS_iterations == 1) {
    summary_stream << verb << num_rows << (num_rows == 1 ? " row" : " rows")
                   << " in " << setiosflags(ios::fixed) << setprecision(3)
                   << elapsed_times[0]/1000.0 << " s" << endl << endl;
  } else {
    double mean, stddev;
    StatUtil::ComputeMeanStddev<double>(&elapsed_times[0],
                                        elapsed_times.size(), &mean, &stddev);
    summary_stream << verb << num_rows << (num_rows == 1 ? " row" : " rows")
                   << " in " << setiosflags(ios::fixed) << setprecision(3)
                   << mean/1000.0 << " s with stddev "
                   << setiosflags(ios::fixed) << setprecision(3) << stddev/1000.0
                   << " s" << endl << endl;
  }

  summary->append(summary_stream.str());
}

static QueryExecutorIf* CreateExecutor() {
  CHECK(!FLAGS_impalad.empty());
  ImpaladQueryExecutor* executor = new ImpaladQueryExecutor();
  if (FLAGS_exec_options.size() > 0) {
    vector<string> exec_options;
    split(exec_options, FLAGS_exec_options, is_any_of(";"), token_compress_on );
    // Check the specified option against TImpalaExecutionOption
    BOOST_FOREACH(string exec_option, exec_options) {
      trim(exec_option);
      vector<string> key_value;
      split(key_value, exec_option, is_any_of(":"), token_compress_on );
      if (key_value.size() != 2) {
        cout << "exec_options must be a list of key:value pairs: " << exec_option
             << endl;
        exit(1);
      }
      if (ImpalaServer::GetQueryOption(key_value[0]) < 0) {
        cout << "invalid exec option: " << key_value[0] << endl;
        exit(1);
      }
    }
    executor->setExecOptions(exec_options);
  }
  return executor;
}

static void GetQueries(vector<string>* queries) {
  queries->clear();

  if (FLAGS_input_file.length() > 0) {
    if (FLAGS_query.length() > 0) {
      cout << "Use either -query or -input_file but not both" << endl;
      return;
    }
    FILE *input_file = fopen(FLAGS_input_file.c_str(), "r");
    if (input_file == NULL) {
      cerr << "Unable top open file: " << FLAGS_input_file.c_str() << endl;
      return;
    }

    fseek(input_file, 0 ,SEEK_END);
    size_t file_size = ftell(input_file);
    rewind(input_file);

    char* buffer = new char[(sizeof(char) * file_size + 1)];
    fread(buffer, 1, file_size, input_file);

    string str(buffer, file_size);
    split(*queries, str, is_any_of(";"), token_compress_on );
  } else if (FLAGS_query.length() > 0) {
    split(*queries, FLAGS_query, is_any_of(";"), token_compress_on );
  }
}

static void Explain(const vector<string>& queries) {
  scoped_ptr<QueryExecutorIf> executor(CreateExecutor());
  EXIT_IF_ERROR(executor->Setup());
  string explain_plan;

  for (int i = 0; i < queries.size(); ++i) {
    EXIT_IF_ERROR(executor->Explain(queries[i], &explain_plan));
    cout << "Explain Plan: ";
    if (queries.size() > 1)  cout << queries[i];
    cout << endl << explain_plan << endl;
  }
}

static void Exec(const vector<string>& queries) {
  bool enable_profiling = false;
  if (FLAGS_enable_counters && FLAGS_profile_output_file.size() != 0) {
    ProfilerStart(FLAGS_profile_output_file.c_str());
    enable_profiling = true;
  }

  vector<double> elapsed_times;
  elapsed_times.resize(FLAGS_iterations);

  // If the number of iterations is greater than 1, run once to Ignore JVM startup time.
  if (FLAGS_iterations > 1) {
    scoped_ptr<QueryExecutorIf> executor(CreateExecutor());
    EXIT_IF_ERROR(executor->Setup());
    EXIT_IF_ERROR(executor->Exec(queries[0], NULL));
    while (true) {
      string row;
      EXIT_IF_ERROR(executor->FetchResult(&row));
      if (row.empty() || executor->eos()) break;
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
    int num_rows = 0;

    ExecStats::QueryType query_type;

    for (int i = 0; i < FLAGS_iterations; ++i) {
      scoped_ptr<QueryExecutorIf> executor(CreateExecutor());
      EXIT_IF_ERROR(executor->Setup());

      WallClockStopWatch sw;
      sw.Start();

      EXIT_IF_ERROR(executor->Exec(*iter, NULL));
      while (true) {
        string row;
        EXIT_IF_ERROR(executor->FetchResult(&row));
        // Only print results for first run
        if (!row.empty() && i == 0) cout << row << endl;
        if (executor->eos()) break;
      }
      sw.Stop();

      num_rows += executor->exec_stats()->num_rows();
      query_type = executor->exec_stats()->query_type();

      elapsed_times[i] = sw.ElapsedTime();

      if (FLAGS_enable_counters) {
        hw_counters.Snapshot("Query");
      }

      if (executor->ErrorString().size() > 0 || executor->FileErrors().size() > 0) {
        // Print runtime errors, e.g., parsing errors.
        cout << executor->ErrorString() << endl;
        // Print file errors.
        cout << executor->FileErrors() << endl;
        break;
      }

      RuntimeProfile* profile = executor->query_profile();
      // Rename the profiles so they merge (they have different query ids)
      if (profile != NULL) {
        vector<RuntimeProfile*> children;
        profile->GetChildren(&children);
        for (int i = 0; i < children.size(); ++i) {
          const string& name = children[i]->name();
          // Parse out the fragment id (e.g. Fragment QueryId:FragmentId) and drop the
          // QueryId
          if (name.compare(0, strlen("Fragment"), "Fragment") == 0) {
            int query_id, fragment_id;
            if (sscanf(name.c_str(), "Fragment %d:%d:", &query_id, &fragment_id) == 2) {
              stringstream ss;
              ss << "Fragment " << fragment_id;
              children[i]->set_name(ss.str());
            }
            children[i]->set_name("Fragment");
          }
        }
        aggregate_profile.Merge(profile);
      }
    }

    num_rows /= FLAGS_iterations;

    string summary;
    ConstructSummaryString(query_type, num_rows, elapsed_times, &summary);
    cout << summary;

    if (FLAGS_iterations > 1) aggregate_profile.Divide(FLAGS_iterations);

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

int main(int argc, char** argv) {
  // Default for run-query is not to use a state-store when executing queries itself
  FLAGS_use_statestore = false;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  CpuInfo::Init();
  DiskInfo::Init();
  InitThriftLogging();
  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();

  if (!FLAGS_principal.empty()) EXIT_IF_ERROR(InitKerberos("runquery"));
  EXIT_IF_ERROR(JniUtil::Init());
  if (FLAGS_init_hbase) {
    EXIT_IF_ERROR(HBaseTableScanner::Init());
    EXIT_IF_ERROR(HBaseTableCache::Init());
  }

  vector<string> queries;
  GetQueries(&queries);
  if (queries.empty()) {
    cout << "No input queries." << endl;
    return 1;
  }

  if (FLAGS_explain_plan) {
    Explain(queries);
  } else {
    Exec(queries);
  }

  // Delete all global JNI references.
  JniUtil::Cleanup();
}
