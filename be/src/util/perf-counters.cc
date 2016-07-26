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

// TODO: "Performance Counters for Linux" is not supported in Linux < 2.6.31.
//       This file will not compile under RHEL 5 or any of it's derivitives.

#include "util/perf-counters.h"
#include "util/debug-util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <sys/syscall.h>
#include <linux/perf_event.h>

#define COUNTER_SIZE (sizeof(void*))
#define BUFFER_SIZE 256
#define PRETTY_PRINT_WIDTH 13

#include "common/names.h"

namespace impala {

// This is the order of the counters in /proc/self/io
enum PERF_IO_IDX {
  PROC_IO_READ = 0,
  PROC_IO_WRITE,
  PROC_IO_SYS_RREAD,
  PROC_IO_SYS_WRITE,
  PROC_IO_DISK_READ,
  PROC_IO_DISK_WRITE,
  PROC_IO_CANCELLED_WRITE,
  PROC_IO_LAST_COUNTER,
};

// Wrapper around sys call.  This syscall is hard to use and this is how it is recommended
// to be used.
static inline int sys_perf_event_open(
    struct perf_event_attr* attr,
    pid_t pid, int cpu, int group_fd,
    unsigned long flags) {
  attr->size = sizeof(*attr);
  return syscall(__NR_perf_event_open, attr, pid, cpu, group_fd, flags);
}

// Remap PerfCounters::Counter to Linux kernel enums
static bool InitEventAttr(perf_event_attr* attr, PerfCounters::Counter counter) {
  memset(attr, 0, sizeof(perf_event_attr));

  switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
      attr->type = PERF_TYPE_SOFTWARE;
      attr->config = PERF_COUNT_SW_CPU_CLOCK;
      break;
    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
      attr->type = PERF_TYPE_SOFTWARE;
      attr->config = PERF_COUNT_SW_PAGE_FAULTS;
      break;
    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
      attr->type = PERF_TYPE_SOFTWARE;
      attr->config = PERF_COUNT_SW_PAGE_FAULTS;
      break;
    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
      attr->type = PERF_TYPE_SOFTWARE;
      attr->config = PERF_COUNT_SW_CPU_MIGRATIONS;
      break;
    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_CPU_CYCLES;
      break;
    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_INSTRUCTIONS;
      break;
    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_CACHE_REFERENCES;
      break;
    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_CACHE_MISSES;
      break;
    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_BRANCH_INSTRUCTIONS;
      break;
    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_BRANCH_MISSES;
      break;
    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
      attr->type = PERF_TYPE_HARDWARE;
      attr->config = PERF_COUNT_HW_BUS_CYCLES;
      break;
    default:
      return false;
  }

  return true;
}

static string GetCounterName(PerfCounters::Counter counter) {
  switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
      return "CPUTime";
    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
      return "PageFaults";
    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
      return "ContextSwitches";
    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
      return "CPUMigrations";
    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
      return "HWCycles";
    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
      return "Instructions";
    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
      return "CacheHit";
    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
      return "CacheMiss";
    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
      return "Branches";
    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
      return "BranchMiss";
    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
      return "BusCycles";
    case PerfCounters::PERF_COUNTER_VM_USAGE:
      return "VmUsage";
    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
      return "PeakVmUsage";
    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
      return "WorkingSet";
    case PerfCounters::PERF_COUNTER_BYTES_READ:
      return "BytesRead";
    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
      return "BytesWritten";
    case PerfCounters::PERF_COUNTER_DISK_READ:
      return "DiskRead";
    case PerfCounters::PERF_COUNTER_DISK_WRITE:
      return "DiskWrite";
    default:
      return "";
  }
}

bool PerfCounters::InitSysCounter(Counter counter) {
  CounterData data;
  data.counter = counter;
  data.source = PerfCounters::SYS_PERF_COUNTER;
  data.fd = -1;
  perf_event_attr attr;
  if (!InitEventAttr(&attr, counter)) {
    return false;
  }
  int fd = sys_perf_event_open(&attr, getpid(), -1, group_fd_, 0);
  if (fd < 0) {
    return false;
  }
  if (group_fd_ == -1) {
    group_fd_ = fd;
  }
  data.fd = fd;

  if (counter == PERF_COUNTER_SW_CPU_CLOCK) {
    data.type = TUnit::TIME_NS;
  } else {
    data.type = TUnit::UNIT;
  }
  counters_.push_back(data);
  return true;
}

bool PerfCounters::InitProcSelfIOCounter(Counter counter) {
  CounterData data;
  data.counter = counter;
  data.source = PerfCounters::PROC_SELF_IO;
  data.type = TUnit::BYTES;

  switch (counter) {
    case PerfCounters::PERF_COUNTER_BYTES_READ:
      data.proc_io_line_number = PROC_IO_READ;
      break;
    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
      data.proc_io_line_number = PROC_IO_WRITE;
      break;
    case PerfCounters::PERF_COUNTER_DISK_READ:
      data.proc_io_line_number = PROC_IO_DISK_READ;
      break;
    case PerfCounters::PERF_COUNTER_DISK_WRITE:
      data.proc_io_line_number = PROC_IO_DISK_WRITE;
      break;
    default:
      return false;
  }
  counters_.push_back(data);
  return true;
}

bool PerfCounters::InitProcSelfStatusCounter(Counter counter) {
  CounterData data;
  data.counter = counter;
  data.source = PerfCounters::PROC_SELF_STATUS;
  data.type = TUnit::BYTES;

  switch (counter) {
    case PerfCounters::PERF_COUNTER_VM_USAGE:
      data.proc_status_field = "VmSize";
      break;
    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
      data.proc_status_field = "VmPeak";
      break;
    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
      data.proc_status_field = "VmRS";
      break;
    default:
      return false;
  }
  counters_.push_back(data);
  return true;
}

bool PerfCounters::GetSysCounters(vector<int64_t>& buffer) {
  for (int i = 0; i < counters_.size(); i++) {
    if (counters_[i].source == SYS_PERF_COUNTER) {
      int num_bytes = read(counters_[i].fd, &buffer[i], COUNTER_SIZE);
      if (num_bytes != COUNTER_SIZE) return false;
      if (counters_[i].type == TUnit::TIME_NS) {
        buffer[i] /= 1000000;
      }
    }
  }
  return true;
}

// Parse out IO counters from /proc/self/io.  The file contains a list of 
// (name,byte) pairs.
// For example:
//    rchar: 210212
//    wchar: 94
//    syscr: 118
//    syscw: 3
//    read_bytes: 0
//    write_bytes: 0
//    cancelled_write_bytes: 0
bool PerfCounters::GetProcSelfIOCounters(vector<int64_t>& buffer) {
  ifstream file("/proc/self/io", ios::in);
  string buf;
  int64_t values[PROC_IO_LAST_COUNTER];

  for (int i = 0; i < PROC_IO_LAST_COUNTER; ++i) {
    if (!file) goto end;
    getline(file, buf);
    size_t colon = buf.find(':');
    if (colon == string::npos) goto end;
    buf = buf.substr(colon + 1);
    istringstream stream(buf);
    stream >> values[i];
  }

  for (int i = 0; i < counters_.size(); ++i) {
    if (counters_[i].source == PROC_SELF_IO) {
      buffer[i] = values[counters_[i].proc_io_line_number];
    }
  }
end:
  if (file.is_open()) file.close();
  return true;
}

bool PerfCounters::GetProcSelfStatusCounters(vector<int64_t>& buffer) {
  ifstream file("/proc/self/status", ios::in);
  string buf;
  
  while (file) {
    getline(file, buf);
    for (int i = 0; i < counters_.size(); ++i) {
      if (counters_[i].source == PROC_SELF_STATUS) {
        size_t field = buf.find(counters_[i].proc_status_field);
        if (field == string::npos) continue;
        size_t colon = field + counters_[i].proc_status_field.size() + 1;
        buf = buf.substr(colon + 1);
        istringstream stream(buf);
        int64_t value;
        stream >> value;
        buffer[i] = value * 1024;  // values in file are in kb
      }
    }
  }
  if (file.is_open()) file.close();
  return true;
}

PerfCounters::PerfCounters() : group_fd_(-1) {
}

// Close all fds for the counters
PerfCounters::~PerfCounters() {
  for (int i = 0; i < counters_.size(); ++i) {
    if (counters_[i].source == SYS_PERF_COUNTER) {
      close(counters_[i].fd);
    }
  }
}

// Add here the default ones that are most useful
bool PerfCounters::AddDefaultCounters() {
  bool result = true;
  result &= AddCounter(PERF_COUNTER_SW_CPU_CLOCK); 
  // These hardware ones don't work on a vm, just ignore if they fail
  // TODO: these don't work reliably and aren't that useful.  Turn them off.
  //AddCounter(PERF_COUNTER_HW_INSTRUCTIONS);
  //AddCounter(PERF_COUNTER_HW_CPU_CYCLES);
  //AddCounter(PERF_COUNTER_HW_BRANCHES);
  //AddCounter(PERF_COUNTER_HW_BRANCH_MISSES);
  //AddCounter(PERF_COUNTER_HW_CACHE_MISSES);
  AddCounter(PERF_COUNTER_VM_USAGE);
  AddCounter(PERF_COUNTER_VM_PEAK_USAGE);
  AddCounter(PERF_COUNTER_RESIDENT_SET_SIZE);
  result &= AddCounter(PERF_COUNTER_DISK_READ);
  return result;
}

// Add a specific counter
bool PerfCounters::AddCounter(Counter counter) {
  // Ignore if it's already added.
  for (int i = 0; i < counters_.size(); ++i) {
    if (counters_[i].counter == counter) {
      return true;
    }
  }
  bool result = false;
  switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
      result = InitSysCounter(counter);
      break;
    case PerfCounters::PERF_COUNTER_BYTES_READ:
    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
    case PerfCounters::PERF_COUNTER_DISK_READ:
    case PerfCounters::PERF_COUNTER_DISK_WRITE:
      result = InitProcSelfIOCounter(counter);
      break;
    case PerfCounters::PERF_COUNTER_VM_USAGE:
    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
      result = InitProcSelfStatusCounter(counter);
      break;
    default:
      return false;
  }
  if (result) counter_names_.push_back(GetCounterName(counter));
  return result;
}

// Query all the counters right now and store the values in results
void PerfCounters::Snapshot(const string& name) {
  if (counters_.size() == 0) {
    return;
  }

  string fixed_name = name;
  if (fixed_name.size() == 0) {
    stringstream ss;
    ss << snapshots_.size() + 1;
    fixed_name = ss.str();
  }

  vector<int64_t> buffer(counters_.size());

  GetSysCounters(buffer);
  GetProcSelfIOCounters(buffer);
  GetProcSelfStatusCounters(buffer);

  snapshots_.push_back(buffer);
  snapshot_names_.push_back(fixed_name);
}

const vector<int64_t>* PerfCounters::counters(int snapshot) const {
  if (snapshot < 0 || snapshot >= snapshots_.size()) return NULL;
  return &snapshots_[snapshot];
}

void PerfCounters::PrettyPrint(ostream* s) const {
  ostream& stream = *s;
  stream << setw(8) << "Snapshot";
  for (int i = 0; i < counter_names_.size(); ++i) {
    stream << setw(PRETTY_PRINT_WIDTH) << counter_names_[i];
  }
  stream << endl;

  for (int s = 0; s < snapshots_.size(); s++) {
    stream << setw(8) << snapshot_names_[s];
    const vector<int64_t>& snapshot = snapshots_[s];
    for (int i = 0; i < snapshot.size(); ++i) {
      stream << setw(PRETTY_PRINT_WIDTH) << PrettyPrinter::Print(snapshot[i], counters_[i].type);
    }
    stream << endl;
  }
  stream << endl;
}

}

