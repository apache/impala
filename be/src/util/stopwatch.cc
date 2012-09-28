// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/stopwatch.h"

#include <sys/time.h>
  
using namespace impala;

uint64_t WallClockStopWatch::GetTime() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec * 1000 + t.tv_usec / 1000;
}
