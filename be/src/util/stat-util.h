// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_STAT_UTIL_H
#define IMPALA_UTIL_STAT_UTIL_H

#include <math.h>

namespace impala {

class StatUtil {
 public:
  // Computes mean and standard deviation
  template <typename T>
  static void ComputeMeanStddev(const T* values, int N, double* mean, double* stddev) {
    *mean = 0;
    *stddev = 0;

    for (int i = 0; i < N; ++i) {
      *mean += values[i];
    }
    *mean /= N;

    for (int i = 0; i < N; ++i) {
      double d = values[i] - *mean;
      *stddev += d*d;
    }

    *stddev /= N;
    *stddev = sqrt(*stddev);
  }
};

}

#endif
