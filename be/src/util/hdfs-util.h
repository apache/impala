// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_HDFS_UTIL_H
#define IMPALA_UTIL_HDFS_UTIL_H

#include <string>

namespace impala {

// HDFS will set errno on error.  Append this to message for better diagnostic messages.
// The optional 'file' argument is appended to the returned message.
std::string AppendHdfsErrorMessage(const std::string& message, 
    const std::string& file = "");

}

#endif // IMPALA_UTIL_HDFS_UTIL_H
