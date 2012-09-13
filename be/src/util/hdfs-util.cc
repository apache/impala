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

Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize) {
  hdfsFileInfo* info = hdfsGetPathInfo(connection, filename);

  if (info == NULL) {
    stringstream msg;
    msg << "Failed to get info on ." << filename;
    return Status(AppendHdfsErrorMessage(msg.str()));
  }

  *filesize = info->mSize;
  hdfsFreeFileInfo(info, 1);

  return Status::OK;
}


}

