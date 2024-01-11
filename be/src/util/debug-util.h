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

#include <string>
#include <vector>

#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/Thrift.h>

#include "common/compiler-util.h"
#include "common/config.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/JniCatalog_types.h"
#include "gen-cpp/Metrics_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/beeswax_types.h"
#include "gen-cpp/common.pb.h"
#include "gen-cpp/parquet_types.h"
#include "gutil/macros.h"

namespace impala {

class RowBatch;
class RowDescriptor;
class TableDescriptor;
class TupleDescriptor;
class Tuple;
class TupleRow;

// Forward declaration to avoid including descriptors.h.
typedef std::vector<int> SchemaPath;

// Used to convert Thrift objects to strings. Thrift defines a 'to_string()' function for
// each type.
template<class T>
std::string PrintValue(const T& value) {
  return to_string(value);
}

std::string PrintTuple(const Tuple* t, const TupleDescriptor& d);
std::string PrintRow(TupleRow* row, const RowDescriptor& d);
std::string PrintBatch(RowBatch* batch);
/// Converts id to a string representation. If necessary, the gdb equivalent is:
///    printf "%lx:%lx\n", id.hi, id.lo
std::string PrintId(const TUniqueId& id, const std::string& separator = ":");
std::string PrintId(const UniqueIdPB& id, const std::string& separator = ":");
std::string PrintIdSet(const std::set<TUniqueId>& ids, std::string separator = ",");

/// Converts id to a string representation without using any shared library calls.
/// Follows Breakpad's guidance for compromised contexts, see
/// https://github.com/google/breakpad/blob/main/docs/linux_starter_guide.md
constexpr int TUniqueIdBufferSize = 33;
void PrintIdCompromised(const TUniqueId& id, char out[TUniqueIdBufferSize],
    const char separator = ':');

inline ostream& operator<<(ostream& os, const UniqueIdPB& id) {
  return os << PrintId(id);
}

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

/// Return a list of TTableName as a comma-separated string.
std::string PrintTableList(const std::vector<TTableName>& tbls);

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
typedef std::vector<std::vector<string>> DebugActionTokens;
DebugActionTokens TokenizeDebugActions(const string& debug_actions);

/// Tokenize 'action' which has an optional parameter separated by '@'. i.e. "x@y"
/// becomes {"x", "y"} and "x" becomes {"x"}.
std::vector<std::string> TokenizeDebugActionParams(const string& action);

/// Slow path implementing DebugAction() for the case where 'debug_action' is non-empty.
Status DebugActionImpl(const string& debug_action, const char* label,
    const std::vector<string>& args) WARN_UNUSED_RESULT;

/// If debug_action query option has a "global action" (i.e. not exec-node specific)
/// and matches the given 'label' and 'args', apply the the action. See
/// ImpalaService.thrift for details of the format and available global actions. For
/// ExecNode code, use ExecNode::ExecDebugAction() instead. Will return OK unless either
/// an invalid debug action is specified or the FAIL action is executed.
WARN_UNUSED_RESULT static inline Status DebugAction(const string& debug_action,
    const char* label, const std::vector<string>& args = std::vector<string>()) {
  if (LIKELY(debug_action.empty())) return Status::OK();
  return DebugActionImpl(debug_action, label, args);
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

/// Map of exception string to the exception throwing function which is used when
/// executing the EXCEPTION debug action.
static const std::unordered_map<std::string,std::function<void()>> EXCEPTION_STR_MAP {
        {"exception",   [](){ throw std::exception(); }},
        {"bad_alloc",   [](){ throw std::bad_alloc(); }},
        {"TException", [](){ throw apache::thrift::TException(); }}
};
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
