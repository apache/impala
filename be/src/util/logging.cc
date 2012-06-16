// (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/logging.h"
#include <glog/logging.h>
#include <boost/thread/mutex.hpp>

bool logging_initialized = false;

using namespace boost;

mutex logging_mutex;

void impala::InitGoogleLoggingSafe(const char* arg) {
  mutex::scoped_lock logging_lock(logging_mutex);
  if (logging_initialized) return;
  google::InitGoogleLogging(arg);
  logging_initialized = true;
}
