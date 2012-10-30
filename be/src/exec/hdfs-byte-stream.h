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


#ifndef IMPALA_EXEC_HDFS_BYTE_STREAM_H_
#define IMPALA_EXEC_HDFS_BYTE_STREAM_H_

#include "exec/byte-stream.h"
#include "exec/hdfs-scan-node.h"

#include <string>
#include <hdfs.h>

namespace impala {

class Status;

// A ByteStream implementation that is backed by an HDFS file
class HdfsByteStream : public ByteStream {
 public:
  // hdfs_connection: connection to the hadoop file system
  // scan_node: scan node which uses this byte stream, passed to record statistics.
  HdfsByteStream(hdfsFS hdfs_connection, HdfsScanNode* scan_node);

  virtual Status Open(const std::string& location);
  virtual Status Close();
  virtual Status Read(uint8_t *buf, int64_t req_length, int64_t* actual_length);
  virtual Status Seek(int64_t offset);
  virtual Status SeekRelative(int64_t offset);
  virtual Status GetPosition(int64_t* position);
  virtual Status Eof(bool* eof);

 private:
  hdfsFS hdfs_connection_;
  hdfsFile hdfs_file_;
  HdfsScanNode* scan_node_;
};

}

#endif
