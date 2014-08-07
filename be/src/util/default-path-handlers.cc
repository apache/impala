// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/default-path-handlers.h"

#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <google/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/pprof-path-handlers.h"
#include "util/webserver.h"

using namespace std;
using namespace google;
using namespace boost;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void LogsHandler(const Webserver::ArgumentMap& args, Document* document) {
  string logfile;
  impala::GetFullLogFilename(google::INFO, &logfile);
  Value log_path(logfile.c_str(), document->GetAllocator());
  document->AddMember("logfile", log_path, document->GetAllocator());

  struct stat file_stat;
  if (stat(logfile.c_str(), &file_stat) == 0) {
    long size = file_stat.st_size;
    long seekpos = size < FLAGS_web_log_bytes ? 0L : size - FLAGS_web_log_bytes;
    ifstream log(logfile.c_str(), ios::in);
    // Note if the file rolls between stat and seek, this could fail (and we could wind up
    // reading the whole file). But because the file is likely to be small, this is
    // unlikely to be an issue in practice.
    log.seekg(seekpos);
    document->AddMember("num_bytes", FLAGS_web_log_bytes, document->GetAllocator());
    stringstream ss;
    ss << log.rdbuf();
    Value log_json(ss.str().c_str(), document->GetAllocator());
    document->AddMember("log", log_json, document->GetAllocator());
  } else {
    Value error(Substitute("Couldn't open INFO log file: $0", logfile).c_str(),
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

// Registered to handle "/flags", and produces Json with 'title" and 'contents' members
// where the latter is a string with all the command-line flags and their values.
void FlagsHandler(const Webserver::ArgumentMap& args, Document* document) {
  Value title("Command-line Flags", document->GetAllocator());
  document->AddMember("title", title, document->GetAllocator());
  Value flags(CommandlineFlagsIntoString().c_str(), document->GetAllocator());
  document->AddMember("contents", flags, document->GetAllocator());
}

// Registered to handle "/memz"
void MemUsageHandler(MemTracker* mem_tracker, const Webserver::ArgumentMap& args,
    Document* document) {
  DCHECK(mem_tracker != NULL);
  Value mem_limit(PrettyPrinter::Print(mem_tracker->limit(), TCounterType::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("mem_limit", mem_limit, document->GetAllocator());
  Value consumption(
      PrettyPrinter::Print(mem_tracker->consumption(), TCounterType::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("consumption", consumption, document->GetAllocator());

  stringstream ss;
#ifdef ADDRESS_SANITIZER
  ss << "Memory tracking is not available with address sanitizer builds.";
#else
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  ss << string(buf);
#endif

  Value overview(ss.str().c_str(), document->GetAllocator());
  document->AddMember("overview", overview, document->GetAllocator());

  if (args.find("detailed") != args.end()) {
    // Dump all mem trackers.
    Value detailed(mem_tracker->LogUsage().c_str(), document->GetAllocator());
    document->AddMember("detailed", detailed, document->GetAllocator());
  }
}

void impala::AddDefaultUrlCallbacks(
    Webserver* webserver, MemTracker* process_mem_tracker) {
  webserver->RegisterUrlCallback("/logs", "logs.tmpl", LogsHandler);
  webserver->RegisterUrlCallback("/varz", "common-pre.tmpl", FlagsHandler);
  if (process_mem_tracker != NULL) {
    webserver->RegisterUrlCallback("/memz","memz.tmpl",
        bind<void>(&MemUsageHandler, process_mem_tracker, _1, _2));
  }

#ifndef ADDRESS_SANITIZER
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofUrlCallbacks(webserver);
  }
#endif
}
