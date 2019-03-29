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

#include "common/config.h"
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

// TODO: remove these functions and use operator << after upgrading to Thrift 0.11.0 or
// higher.
std::string PrintThriftEnum(const beeswax::QueryState::type& value);
std::string PrintThriftEnum(const parquet::Encoding::type& value);
std::string PrintThriftEnum(const TCatalogObjectType::type& value);
std::string PrintThriftEnum(const TCatalogOpType::type& value);
std::string PrintThriftEnum(const TDdlType::type& value);
std::string PrintThriftEnum(const TExplainLevel::type& value);
std::string PrintThriftEnum(const THdfsCompression::type& value);
std::string PrintThriftEnum(const THdfsFileFormat::type& value);
std::string PrintThriftEnum(const THdfsSeqCompressionMode::type& value);
std::string PrintThriftEnum(const TImpalaQueryOptions::type& value);
std::string PrintThriftEnum(const TJoinDistributionMode::type& value);
std::string PrintThriftEnum(const TKuduReadMode::type& value);
std::string PrintThriftEnum(const TMetricKind::type& value);
std::string PrintThriftEnum(const TParquetArrayResolution::type& value);
std::string PrintThriftEnum(const TParquetFallbackSchemaResolution::type& value);
std::string PrintThriftEnum(const TPlanNodeType::type& value);
std::string PrintThriftEnum(const TPrefetchMode::type& value);
std::string PrintThriftEnum(const TReplicaPreference::type& value);
std::string PrintThriftEnum(const TRuntimeFilterMode::type& value);
std::string PrintThriftEnum(const TSessionType::type& value);
std::string PrintThriftEnum(const TStmtType::type& value);
std::string PrintThriftEnum(const TUnit::type& value);
std::string PrintThriftEnum(const TParquetTimestampType::type& value);

std::string PrintTuple(const Tuple* t, const TupleDescriptor& d);
std::string PrintRow(TupleRow* row, const RowDescriptor& d);
std::string PrintBatch(RowBatch* batch);
/// Converts id to a string represantation. If necessary, the gdb equivalent is:
///    printf "%lx:%lx\n", id.hi, id.lo
std::string PrintId(const TUniqueId& id, const std::string& separator = ":");

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

#ifndef IMPALA_CMAKE_BUILD_TYPE
static_assert(false, "IMPALA_CMAKE_BUILD_TYPE is not defined");
#endif

/// Returns the value of CMAKE_BUILD_TYPE used to build the code
constexpr const char* GetCMakeBuildType() {
  return AS_STRING(IMPALA_CMAKE_BUILD_TYPE);
}

/// Returns whether the code was dynamically or statically linked, return
/// value is either STATIC or DYNAMIC.
constexpr const char* GetLibraryLinkType() {
  return AS_STRING(IMPALA_BUILD_SHARED_LIBS)[0] == 'O'
          && AS_STRING(IMPALA_BUILD_SHARED_LIBS)[1] == 'N' ?
      "DYNAMIC" :
      "STATIC";
}

/// Returns "<program short name> version <GetBuildVersion(compact)>"
std::string GetVersionString(bool compact = false);

/// Returns the stack trace as a string from the current location.
/// Note: there is a libc bug that causes this not to work on 64 bit machines
/// for recursive calls.
std::string GetStackTrace();

/// Returns the backend name in "host:port" form suitable for human consumption.
std::string GetBackendString();

/// Tokenize 'debug_actions' into a list of tokenized rows, where columns are separated
/// by ':' and rows by '|'. i.e. if debug_actions="a:b:c|x:y", then the returned
/// structure is {{"a", "b", "c"}, {"x", "y"}}
typedef std::list<std::vector<string>> DebugActionTokens;
DebugActionTokens TokenizeDebugActions(const string& debug_actions);

/// Tokenize 'action' which has an optional parameter separated by '@'. i.e. "x@y"
/// becomes {"x", "y"} and "x" becomes {"x"}.
std::vector<std::string> TokenizeDebugActionParams(const string& action);

/// Slow path implementing DebugAction() for the case where 'debug_action' is non-empty.
Status DebugActionImpl(const string& debug_action, const char* label) WARN_UNUSED_RESULT;

/// If debug_action query option has a "global action" (i.e. not exec-node specific)
/// and matches the given 'label', apply the the action. See ImpalaService.thrift for
/// details of the format and available global actions. For ExecNode code, use
/// ExecNode::ExecDebugAction() instead.
WARN_UNUSED_RESULT static inline Status DebugAction(
    const string& debug_action, const char* label) {
  if (LIKELY(debug_action.empty())) return Status::OK();
  return DebugActionImpl(debug_action, label);
}

WARN_UNUSED_RESULT static inline Status DebugAction(
    const TQueryOptions& query_options, const char* label) {
  return DebugAction(query_options.debug_action, label);
}

static inline void DebugActionNoFail(const string& debug_action, const char* label) {
  Status status = DebugAction(debug_action, label);
  if (!status.ok()) {
    LOG(ERROR) << "Ignoring debug action failure: " << status.GetDetail();
  }
}

/// Like DebugAction() but for use in contexts that can't safely propagate an error
/// status. Debug action FAIL should not be used in these contexts and will be logged
/// and ignored.
static inline void DebugActionNoFail(
    const TQueryOptions& query_options, const char* label) {
  DebugActionNoFail(query_options.debug_action, label);
}

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
