// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <errno.h>
#include <string.h>

#include "util/hdfs-util.h"

using namespace std;

namespace impala {

string AppendHdfsErrorMessage(const string& message, const string& file) {
  stringstream ss;
  ss << message << file
     << "\nError(" << errno << "): " << strerror(errno);
  return ss.str();
}

}

