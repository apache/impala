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

namespace cpp impala
namespace java org.apache.impala.thrift

include "Types.thrift"

struct TVertexMetadata {
  // Table name.
  1: required string table_name

  // Type of the table (e.g. hive, iceberg, kudu, hbase)
  2: required string table_type

  // Create time of the table/view.
  3: required i64 table_create_time
}

struct TVertex {
  // Vertex id
  1: required i64 id

  // Column label
  2: required string label

  // Metadata of the vertex.
  3: optional TVertexMetadata metadata
}

enum TEdgeType {
  PROJECTION = 0
  PREDICATE = 1
}

struct TMultiEdge {
  // Set of source vertices
  1: list<TVertex> sources

  // Set of target vertices
  2: list<TVertex> targets

  // Connecting edge type
  3: TEdgeType edgetype
}

struct TLineageGraph {
  // Input SQL query
  1: required string query_text

  // 128-bit Murmur3 hash of the query string
  2: required string hash

  // Name of the user who issued this query
  3: required string user

  // Query start time as a unix epoch, set in planner
  4: required i64 started

  // Query end time as unix epoch.
  // Marked as optional as it is set in the backend
  5: optional i64 ended

  6: list<TMultiEdge> edges

  7: list<TVertex> vertices

  // Query id in TQueryCtx
  8: required Types.TUniqueId query_id

  // Set only for external tables to establish
  // lineage between the table and it's location.
  9: optional string table_location
}
