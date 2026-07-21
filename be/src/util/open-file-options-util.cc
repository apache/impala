// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/open-file-options-util.h"

#include <string>

#include "util/hdfs-util.h"

#include "common/names.h"

namespace impala {

// Standard, filesystem-agnostic openFile builder option keys (HADOOP-15229).
static const char* FS_OPTION_OPENFILE_READ_POLICY =
    "fs.option.openfile.read.policy";
static const char* FS_OPTION_OPENFILE_LENGTH = "fs.option.openfile.length";

// Every file Impala reads through this path gets the same S3A read policy, 'random',
// regardless of format. The reason is Impala's own I/O layer rather than the formats:
// FileHandleCache::Accessor's destructor calls hdfsUnbufferFile() after every read
// (handle-cache.inline.h), so the long-lived streaming GET that 'sequential' exists for
// never materializes -- the stream is torn down every --read_size bytes no matter what
// the format reads like.
//
// Given that, the policy only decides the GET's upper bound
// (S3AInputStream::calculateRequestLimit): 'random' bounds it to the bytes actually
// requested, while 'sequential' and the default 'adaptive' both bound it to the end of
// the file. A GET to EOF leaves the remainder of the file unread, and unbuffer()
// hard-aborts the connection when more than fs.s3a.readahead.range (64KB by default) is
// left over, so each read pays a fresh connection. 'adaptive' self-corrects to random
// after the first abort; 'sequential' never does.
// That makes 'sequential' the worst of the three here, and bounded reads under 'random'
// avoid the aborts entirely.
//
// Specific format tokens ('parquet', 'avro', ...) are no help: Hadoop 3.4.2 recognizes
// them as plain synonyms of 'random' or 'sequential', and the first recognized token in
// the list wins, so 'avro, random' would resolve to 'sequential', not 'random'.
static const char* RANDOM_READ_POLICY = "random";

io::OpenFileOptions BuildOpenFileOptions(const char* path, int64_t file_length) {
  io::OpenFileOptions options;
  // The read policy only affects S3A; other filesystems ignore it, but there is
  // no reason to pay the string copy for them.
  if (path != nullptr && IsS3APath(path)) {
    options.emplace_back(FS_OPTION_OPENFILE_READ_POLICY, RANDOM_READ_POLICY);
  }
  // Passing the known length lets S3A skip the HEAD request on open. Harmless on
  // other filesystems. Guard against a stale/zero length truncating reads.
  if (file_length > 0) {
    options.emplace_back(FS_OPTION_OPENFILE_LENGTH, std::to_string(file_length));
  }
  return options;
}

}
