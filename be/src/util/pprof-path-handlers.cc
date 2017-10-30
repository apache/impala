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

#include "util/pprof-path-handlers.h"

#include <boost/bind.hpp>
#include <fstream>
#include <sys/stat.h>
#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>

#include "common/logging.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace google;
using namespace impala;
using namespace rapidjson;

DECLARE_string(heap_profile_dir);

static const int PPROF_DEFAULT_SAMPLE_SECS = 30; // pprof default sample time in seconds.

// pprof asks for the url /pprof/cmdline to figure out what application it's profiling.
// The server should respond by reading the contents of /proc/self/cmdline.
void PprofCmdLineHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  ifstream cmd_line_file("/proc/self/cmdline", ios::in);
  if (!cmd_line_file.is_open()) {
    (*output) << "Unable to open file: /proc/self/cmdline";
    return;
  } else {
    (*output) << cmd_line_file.rdbuf();
    cmd_line_file.close();
  }
}

// pprof asks for the url /pprof/heap to get heap information. This should be implemented
// by calling HeapProfileStart(filename), continue to do work, and then, some number of
// seconds later, call GetHeapProfile() followed by HeapProfilerStop().
void PprofHeapHandler(const Webserver::ArgumentMap& args, stringstream* output) {
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  (void)PPROF_DEFAULT_SAMPLE_SECS; // Avoid unused variable warning.
  (*output) << "Heap profiling is not available with address/thread sanitizer builds.";
#else
  Webserver::ArgumentMap::const_iterator it = args.find("seconds");
  int seconds = PPROF_DEFAULT_SAMPLE_SECS;
  if (it != args.end()) {
    seconds = atoi(it->second.c_str());
  }

  HeapProfilerStart(FLAGS_heap_profile_dir.c_str());
  // Sleep to allow for some samples to be collected.
  sleep(seconds);
  const char* profile = GetHeapProfile();
  HeapProfilerStop();
  (*output) << profile;
  delete profile;
#endif
}

// pprof asks for the url /pprof/profile?seconds=XX to get cpu-profiling information.
// The server should respond by calling ProfilerStart(), continuing to do its work,
// and then, XX seconds later, calling ProfilerStop().
void PprofCpuProfileHandler(const Webserver::ArgumentMap& args, stringstream* output) {
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  (*output) << "CPU profiling is not available with address/thread sanitizer builds.";
#else
  Webserver::ArgumentMap::const_iterator it = args.find("seconds");
  int seconds = PPROF_DEFAULT_SAMPLE_SECS;
  if (it != args.end()) {
    seconds = atoi(it->second.c_str());
  }
  ostringstream tmp_prof_file_name;
  // Build a temporary file name that is hopefully unique.
  tmp_prof_file_name << "/tmp/impala_cpu_profile." << getpid() << "." << rand();
  ProfilerStart(tmp_prof_file_name.str().c_str());
  sleep(seconds);
  ProfilerStop();
  ifstream prof_file(tmp_prof_file_name.str().c_str(), ios::in);
  if (!prof_file.is_open()) {
    (*output) << "Unable to open cpu profile: " << tmp_prof_file_name.str();
    return;
  }
  (*output) << prof_file.rdbuf();
  prof_file.close();
#endif
}

// pprof asks for the url /pprof/growth to get heap-profiling delta (growth) information.
// The server should respond by calling:
// MallocExtension::instance()->GetHeapGrowthStacks(&output);
void PprofGrowthHandler(const Webserver::ArgumentMap& args, stringstream* output) {
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  (*output) << "Growth profiling is not available with address/thread sanitizer builds.";
#else
  string heap_growth_stack;
  MallocExtension::instance()->GetHeapGrowthStacks(&heap_growth_stack);
  (*output) << heap_growth_stack;
#endif
}

// pprof asks for the url /pprof/symbol to map from hex addresses to variable names.
// When the server receives a GET request for /pprof/symbol, it should return a line
// formatted like: num_symbols: ###
// where ### is the number of symbols found in the binary. For now, the only important
// distinction is whether the value is 0, which it is for executables that lack debug
// information, or not-0).
//
// TODO: This part is not implemented:
// In addition to the GET request for this url, the server must accept POST requests.
// This means that after the HTTP headers, pprof will pass in a list of hex addresses
// connected by +, like:
//   curl -d '0x0824d061+0x0824d1cf' http://remote_host:80/pprof/symbol
// The server should read the POST data, which will be in one line, and for each hex value
// should write one line of output to the output stream, like so:
// <hex address><tab><function name>
// For instance:
// 0x08b2dabd    _Update
void PprofSymbolHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  // TODO: Implement symbol resolution. Without this, the binary needs to be passed
  // to pprof to resolve all symbols.
  (*output) << "num_symbols: 0";
}

void impala::AddPprofUrlCallbacks(Webserver* webserver) {
  // Path handlers for remote pprof profiling. For information see:
  // https://gperftools.googlecode.com/svn/trunk/doc/pprof_remote_servers.html
  webserver->RegisterUrlCallback("/pprof/cmdline",
      bind<void>(PprofCmdLineHandler, _1, _2));
  webserver->RegisterUrlCallback("/pprof/heap", bind<void>(PprofHeapHandler, _1, _2));
  webserver->RegisterUrlCallback("/pprof/growth", bind<void>(PprofGrowthHandler, _1, _2));
  webserver->RegisterUrlCallback("/pprof/profile",
      bind<void>(PprofCpuProfileHandler, _1, _2));
  webserver->RegisterUrlCallback("/pprof/symbol", bind<void>(PprofSymbolHandler, _1, _2));
}
