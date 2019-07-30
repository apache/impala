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

// Resource Profiles are computed by the planner. They are passed to backend ExecNodes
// and DataSinks to configure their memory usage. See ResourceProfile.java for more
// details.

namespace cpp impala
namespace java org.apache.impala.thrift

struct TBackendResourceProfile {
  // The minimum reservation for this plan node in bytes.
  1: required i64 min_reservation

  // The maximum reservation for this plan node in bytes. MAX_INT64 means effectively
  // unlimited.
  2: required i64 max_reservation

  // The spillable buffer size in bytes to use for this node, chosen by the planner.
  // Set iff the node uses spillable buffers.
  3: optional i64 spillable_buffer_size

  // The buffer size in bytes that is large enough to fit the largest row to be processed.
  // Set if the node allocates buffers for rows from the buffer pool.
  4: optional i64 max_row_buffer_size
}
