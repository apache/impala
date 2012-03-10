// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-byte-stream.h"
#include "common/status.h"
#include <glog/logging.h>
#include <sstream>

using namespace impala;
using namespace std;

// HDFS will set errno on error.  Append this to message for better diagnostic messages.
static string AppendHdfsErrorMessage(const string& message) {
  stringstream ss;
  ss << message << "\nError(" << errno << "): " << strerror(errno);
  return ss.str();
}

HdfsByteStream::HdfsByteStream(hdfsFS hdfs_connection)
    : hdfs_connection_(hdfs_connection),
      hdfs_file_(NULL) {
}

Status HdfsByteStream::GetPosition(int64_t* position) {
  // TODO: Deal with error codes from hdfsTell
  DCHECK(hdfs_file_ != NULL);
  *position = hdfsTell(hdfs_connection_, hdfs_file_);
  return Status::OK;
}

Status HdfsByteStream::Open(const string& location) {
  DCHECK(hdfs_file_ == NULL);
  location_ = location;
  hdfs_file_ = hdfsOpenFile(hdfs_connection_, location_.c_str(), O_RDONLY, 0, 0, 0);
  if (hdfs_file_ == NULL) {
    return Status(AppendHdfsErrorMessage("Failed to open HDFS file " + location_));
  }
  VLOG(1) << "HdfsByteStream: opened file " << location_;
  return Status::OK;
}

Status HdfsByteStream::Read(char* buf, int64_t req_length, int64_t* actual_length) {
  DCHECK(buf != NULL);
  DCHECK(req_length >= 0);

  int n_read = 0;
  while (n_read < req_length) {
    int last_read =
      hdfsReadDirect(hdfs_connection_, hdfs_file_, buf + n_read, req_length - n_read);
    if (last_read == 0) {
      *actual_length = n_read;
      return Status::OK;
    } else if (last_read == -1) {
      // In case of error, *actual_length is not updated
      return Status(AppendHdfsErrorMessage("Error reading from HDFS file: " + location_));
    }

    n_read += last_read;
  }

  *actual_length = n_read;
  return Status::OK;
}

Status HdfsByteStream::Close() {
  if (hdfs_file_ == NULL) return Status::OK;
  int hdfs_ret = hdfsCloseFile(hdfs_connection_, hdfs_file_);
  if (hdfs_ret != 0) {
    return Status(AppendHdfsErrorMessage("Error closing HDFS file:" + location_));
  }
  hdfs_file_ = NULL;
  return Status::OK;
}

Status HdfsByteStream::Seek(int64_t offset) {
  DCHECK(hdfs_file_ != NULL);
  if (hdfsSeek(hdfs_connection_, hdfs_file_, offset) != 0) {
    return Status("Error seeking HDFS file: " + location_);
  }

  return Status::OK;
}
