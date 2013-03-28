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
#include <google/malloc_extension.h>

#include "common/logging.h"
#include "util/webserver.h"
#include "util/logging.h"

using namespace std;
using namespace google;
using namespace boost;
using namespace impala;

DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void LogsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  string logfile;
  impala::GetFullLogFilename(google::INFO, &logfile);
  (*output) << "<h2>INFO logs</h2>" << endl;
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
    (*output) << "<br/>Showing last " << FLAGS_web_log_bytes << " bytes of log" << endl;
    (*output) << "<br/><pre>" << log.rdbuf() << "</pre>";

  } else {
    (*output) << "<br/>Couldn't open INFO log file: " << logfile;
  }

}

// Registered to handle "/flags", and prints out all command-line flags and their values
void FlagsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  (*output) << "<h2>Command-line Flags</h2>";
  (*output) << "<pre>" << CommandlineFlagsIntoString() << "</pre>";
}

// Registered to handle "/memz", and prints out memory allocation statistics.
void MemUsageHandler(const Webserver::ArgumentMap& args, stringstream* output) {
#ifdef ADDRESS_SANITIZER
  (*output) << "Memory tracking is not available with address sanitizer builds.";
#else
  (*output) << "<pre>";
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  // Replace new lines with <br> for html
  string tmp(buf);
  replace_all(tmp, "\n", "<br>");
  (*output) << tmp << "</pre>";
#endif
}

void impala::AddDefaultPathHandlers(Webserver* webserver) {
  webserver->RegisterPathHandler("/logs", LogsHandler);
  webserver->RegisterPathHandler("/varz", FlagsHandler);
  webserver->RegisterPathHandler("/memz", MemUsageHandler);
}
