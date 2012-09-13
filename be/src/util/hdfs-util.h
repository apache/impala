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


#ifndef IMPALA_UTIL_HDFS_UTIL_H
#define IMPALA_UTIL_HDFS_UTIL_H

#include <string>
#include <hdfs.h>
#include "common/status.h"

namespace impala {

// HDFS will set errno on error.  Append this to message for better diagnostic messages.
// The optional 'file' argument is appended to the returned message.
std::string AppendHdfsErrorMessage(const std::string& message, 
    const std::string& file = "");

// Return the size, in bytes, of a file from the hdfs connection.
Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize);

}
#endif // IMPALA_UTIL_HDFS_UTIL_H
