// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <hdfs.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <boost/static_assert.hpp>
#include <stdint.h>  // for intptr_t

#define HDFS_PREFIX "file:"


// Return any non-NULL value.
hdfsFS hdfsConnect(const char* host, tPort port) {
  return reinterpret_cast<hdfsFS>(1);
}

int hdfsDisconnect(hdfsFS fs) {
  return 0;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* hdfs_path, int flags,
                      int dummy1, short dummy2, tSize dummy3) {
  std::string local_path(hdfs_path);
  if (local_path.find(HDFS_PREFIX) == 0) {
    local_path.replace(0, strlen(HDFS_PREFIX), "");
  }
  int fd = open(local_path.c_str(), flags);
  if (fd < 0) return NULL;
  hdfsFile result = new hdfsFile_internal();
  // make sure we can store file descriptors in void*
  BOOST_STATIC_ASSERT(sizeof(fd) <= sizeof(void*));
  result->file = reinterpret_cast<void*>(fd);
  return result;
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  if (file == NULL) return -1;
  int fd = reinterpret_cast<intptr_t>(file->file);
  return close(fd);
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  if (file == NULL) return -1;
  int fd = reinterpret_cast<intptr_t>(file->file);
  return read(fd, buffer, length);
}
