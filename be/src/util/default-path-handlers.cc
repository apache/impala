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

#include "common/logging.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/pprof-path-handlers.cc"
#include "util/webserver.h"

using namespace std;
using namespace google;
using namespace boost;
using namespace impala;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

// Html/Text formatting tags
struct Tags {
  string pre_tag, end_pre_tag, line_break, header, end_header;

  // If as_text is true, set the html tags to a corresponding raw text representation.
  Tags(bool as_text) {
    if (as_text) {
      pre_tag = "";
      end_pre_tag = "\n";
      line_break = "\n";
      header = "";
      end_header = "";
    } else {
      pre_tag = "<pre>";
      end_pre_tag = "</pre>";
      line_break = "<br/>";
      header = "<h2>";
      end_header = "</h2>";
    }
  }
};

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void LogsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);
  string logfile;
  impala::GetFullLogFilename(google::INFO, &logfile);
  (*output) << tags.header <<"INFO logs" << tags.end_header << endl;
  (*output) << "Log path is: " << logfile << endl;

  struct stat file_stat;
  if (stat(logfile.c_str(), &file_stat) == 0) {
    long size = file_stat.st_size;
    long seekpos = size < FLAGS_web_log_bytes ? 0L : size - FLAGS_web_log_bytes;
    ifstream log(logfile.c_str(), ios::in);
    // Note if the file rolls between stat and seek, this could fail
    // (and we could wind up reading the whole file). But because the
    // file is likely to be small, this is unlikely to be an issue in
    // practice.
    log.seekg(seekpos);
    (*output) << tags.line_break <<"Showing last " << FLAGS_web_log_bytes
              << " bytes of log" << endl;
    (*output) << tags.line_break << tags.pre_tag << log.rdbuf() << tags.end_pre_tag;

  } else {
    (*output) << tags.line_break << "Couldn't open INFO log file: " << logfile;
  }
}

// Registered to handle "/flags", and prints out all command-line flags and their values
void FlagsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);
  (*output) << tags.header << "Command-line Flags" << tags.end_header;
  (*output) << tags.pre_tag << CommandlineFlagsIntoString() << tags.end_pre_tag;
}

// Registered to handle "/memz", and prints out memory allocation statistics.
void MemUsageHandler(MemTracker* mem_tracker, const Webserver::ArgumentMap& args,
    stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);

  DCHECK(mem_tracker != NULL);
  (*output) << tags.pre_tag
            << "Mem Limit: "
            << PrettyPrinter::Print(mem_tracker->limit(), TCounterType::BYTES)
            << endl
            << "Mem Consumption: "
            << PrettyPrinter::Print(mem_tracker->consumption(), TCounterType::BYTES)
            << endl
            << tags.end_pre_tag;

  (*output) << tags.pre_tag;
#ifdef ADDRESS_SANITIZER
  (*output) << "Memory tracking is not available with address sanitizer builds.";
#else
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  // Replace new lines with <br> for html
  string tmp(buf);
  replace_all(tmp, "\n", tags.line_break);
  (*output) << tmp << tags.end_pre_tag;
#endif

  if (args.find("detailed") != args.end()) {
    // Dump all mem trackers.
    (*output) << tags.pre_tag
              << "Detailed usage: " << endl
              << mem_tracker->LogUsage()
              << tags.end_pre_tag;
  }
}

void impala::AddDefaultPathHandlers(
    Webserver* webserver, MemTracker* process_mem_tracker) {
  webserver->RegisterPathHandler("/logs", LogsHandler);
  webserver->RegisterPathHandler("/varz", FlagsHandler);
  if (process_mem_tracker != NULL) {
    webserver->RegisterPathHandler("/memz",
        bind<void>(&MemUsageHandler, process_mem_tracker, _1, _2));
  }

#ifndef ADDRESS_SANITIZER
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofPathHandlers(webserver);
  }
#endif
}
