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

#ifndef IMPALA_UTIL_DEBUG_UTIL_H
#define IMPALA_UTIL_DEBUG_UTIL_H

#include <ostream>
#include <string>
#include <sstream>

#include <thrift/protocol/TDebugProtocol.h>

#include "gen-cpp/JniCatalog_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/RuntimeProfile_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/parquet_types.h"

#include "runtime/descriptors.h" // for SchemaPath

namespace impala {

class RowDescriptor;
class TableDescriptor;
class TupleDescriptor;
class Tuple;
class TupleRow;
class RowBatch;

std::ostream& operator<<(std::ostream& os, const TFunctionBinaryType::type& op);
std::ostream& operator<<(std::ostream& os, const TUniqueId& id);
std::ostream& operator<<(std::ostream& os, const THdfsFileFormat::type& type);
std::ostream& operator<<(std::ostream& os, const THdfsCompression::type& type);
std::ostream& operator<<(std::ostream& os, const TStmtType::type& type);
std::ostream& operator<<(std::ostream& os, const TUnit::type& type);
std::ostream& operator<<(std::ostream& os, const TMetricKind::type& type);
std::ostream& operator<<(std::ostream& os, const beeswax::QueryState::type& type);
std::ostream& operator<<(std::ostream& os, const parquet::Encoding::type& type);
std::ostream& operator<<(std::ostream& os, const parquet::CompressionCodec::type& type);
std::ostream& operator<<(std::ostream& os, const parquet::Type::type& type);

std::string PrintTuple(const Tuple* t, const TupleDescriptor& d);
std::string PrintRow(TupleRow* row, const RowDescriptor& d);
std::string PrintBatch(RowBatch* batch);
std::string PrintId(const TUniqueId& id, const std::string& separator = ":");
std::string PrintPlanNodeType(const TPlanNodeType::type& type);
std::string PrintTCatalogObjectType(const TCatalogObjectType::type& type);
std::string PrintTDdlType(const TDdlType::type& type);
std::string PrintTCatalogOpType(const TCatalogOpType::type& type);
std::string PrintTReplicaPreference(const TReplicaPreference::type& type);
std::string PrintTSessionType(const TSessionType::type& type);
std::string PrintTStmtType(const TStmtType::type& type);
std::string PrintQueryState(const beeswax::QueryState::type& type);
std::string PrintEncoding(const parquet::Encoding::type& type);
std::string PrintTMetricKind(const TMetricKind::type& type);
std::string PrintTUnit(const TUnit::type& type);
std::string PrintTImpalaQueryOptions(const TImpalaQueryOptions::type& type);

/// Returns the fully qualified path, e.g. "database.table.array_col.item.field"
std::string PrintPath(const TableDescriptor& tbl_desc, const SchemaPath& path);
/// Same as PrintPath(), but truncates the path after the given 'end_path_idx'.
std::string PrintSubPath(const TableDescriptor& tbl_desc, const SchemaPath& path,
    int end_path_idx);
/// Returns the numeric path without column/field names, e.g. "[0,1,2]"
std::string PrintNumericPath(const SchemaPath& path);

// Convenience wrapper around Thrift's debug string function
template<typename ThriftStruct> std::string PrintThrift(const ThriftStruct& t) {
  return apache::thrift::ThriftDebugString(t);
}

/// Parse 's' into a TUniqueId object.  The format of s needs to be the output format
/// from PrintId.  (<hi_part>:<low_part>)
/// Returns true if parse succeeded.
bool ParseId(const std::string& s, TUniqueId* id);

/// Returns a string "<product version number> (build <build hash>)"
/// If compact == false, this string is appended: "\nBuilt on <build time>"
/// This is used to set gflags build version
std::string GetBuildVersion(bool compact = false);

/// Returns "<program short name> version <GetBuildVersion(compact)>"
std::string GetVersionString(bool compact = false);

/// Returns the stack trace as a string from the current location.
/// Note: there is a libc bug that causes this not to work on 64 bit machines
/// for recursive calls.
std::string GetStackTrace();

/// Returns the backend name in "host:port" form suitable for human consumption.
std::string GetBackendString();

// FILE_CHECKs are conditions that we expect to be true but could fail due to a malformed
// input file. They differentiate these cases from DCHECKs, which indicate conditions that
// are true unless there's a bug in Impala. We would ideally always return a bad Status
// instead of failing a FILE_CHECK, but in many cases we use FILE_CHECK instead because
// there's a performance cost to doing the check in a release build, or just due to legacy
// code.
#define FILE_CHECK(a) DCHECK(a)
#define FILE_CHECK_EQ(a, b) DCHECK_EQ(a, b)
#define FILE_CHECK_NE(a, b) DCHECK_NE(a, b)
#define FILE_CHECK_GT(a, b) DCHECK_GT(a, b)
#define FILE_CHECK_LT(a, b) DCHECK_LT(a, b)
#define FILE_CHECK_GE(a, b) DCHECK_GE(a, b)
#define FILE_CHECK_LE(a, b) DCHECK_LE(a, b)

}

#endif
