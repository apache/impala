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

#pragma once

#include "runtime/io/open-file-options.h"

namespace impala {

/// Builds the set of libhdfs openFile builder options for a file, based on its path
/// and length. All options are soft (ignored by filesystems that do not understand
/// them), so this is safe to call for every file on any filesystem.
///
/// - On S3A, sets 'fs.option.openfile.read.policy' to 'random'. This does not depend
///   on the file format: see open-file-options-util.cc for why every format Impala
///   reads through this path wants 'random'. Non-S3A paths get no read policy.
/// - When 'file_length' > 0, sets 'fs.option.openfile.length' so S3A can skip the
///   HEAD request on open. Set for all filesystems. Guarded on > 0 because a
///   stale/zero length would truncate reads.
io::OpenFileOptions BuildOpenFileOptions(const char* path, int64_t file_length);

}
