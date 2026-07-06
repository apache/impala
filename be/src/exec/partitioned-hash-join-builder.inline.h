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

#ifndef IMPALA_EXEC_PARTITIONED_HASH_JOIN_BUILDER_INLINE_H
#define IMPALA_EXEC_PARTITIONED_HASH_JOIN_BUILDER_INLINE_H

#include "exec/partitioned-hash-join-builder.h"

namespace impala {

inline bool PhjBuilder::AppendRow(
    BufferedTupleStream* stream, TupleRow* row, Status* status) {
  if (LIKELY(stream->AddRow(row, status))) return true;
  if (UNLIKELY(!status->ok())) return false;
  return AppendRowStreamFull(stream, row, status);
}

} // namespace impala

#endif