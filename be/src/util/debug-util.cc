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

#include "util/debug-util.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <utility>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/tokenizer.hpp>
#include <gflags/gflags.h>

#include "common/version.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/types.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/string-parser.h"
#include "util/time.h"

#include "common/names.h"

// / WARNING this uses a private API of GLog: DumpStackTraceToString().
namespace google {
namespace glog_internal_namespace_ {
extern void DumpStackTraceToString(std::string* s);
}
}

#include "common/names.h"

using boost::algorithm::iequals;
using boost::char_separator;
using boost::is_any_of;
using boost::split;
using boost::token_compress_on;
using boost::tokenizer;
using namespace beeswax;
using namespace parquet;

DECLARE_int32(krpc_port);
DECLARE_string(hostname);

namespace impala {

string PrintId(const TUniqueId& id, const string& separator) {
  stringstream out;
  // Outputting the separator string resets the stream width.
  out << hex << setfill('0') << setw(16) << id.hi << separator << setw(16) << id.lo;
  return out.str();
}

string PrintId(const UniqueIdPB& id, const string& separator) {
  stringstream out;
  // Outputting the separator string resets the stream width.
  out << hex << setfill('0') << setw(16) << id.hi() << separator << setw(16) << id.lo();
  return out.str();
}

static void my_i64tohex(int64_t w, char out[16]) {
  static const char* digits = "0123456789abcdef";
  for (size_t i = 0, j=60; i < 16; ++i, j -= 4) {
    out[i] = digits[(w>>j) & 0x0f];
  }
}

void PrintIdCompromised(const TUniqueId& id, char out[TUniqueIdBufferSize],
    const char separator) {
  my_i64tohex(id.hi, out);
  out[16] = separator;
  my_i64tohex(id.lo, out+17);
}

string PrintIdSet(const std::set<TUniqueId>& ids, std::string separator) {
  stringstream out;
  auto it = ids.begin();
  while (it != ids.end()) {
    out << PrintId(*it);
    if (++it != ids.end()) {
      out << separator;
    }
  }
  return out.str();
}

bool ParseId(const string& s, TUniqueId* id) {
  // For backwards compatibility, this method parses two forms of query ID from text:
  //  - <hex-int64_t><colon><hex-int64_t> - this format is the standard going forward
  //  - <decimal-int64_t><space><decimal-int64_t> - legacy compatibility with CDH4 CM
  DCHECK(id != NULL);

  const char* hi_part = s.c_str();
  char* separator = const_cast<char*>(strchr(hi_part, ':'));
  if (separator == NULL) {
    // Legacy compatibility branch
    char_separator<char> sep(" ");
    tokenizer< char_separator<char>> tokens(s, sep);
    int i = 0;
    for (const string& token: tokens) {
      StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
      int64_t component = StringParser::StringToInt<int64_t>(
          token.c_str(), token.length(), &parse_result);
      if (parse_result != StringParser::PARSE_SUCCESS) return false;
      if (i == 0) {
        id->hi = component;
      } else if (i == 1) {
        id->lo = component;
      } else {
        // Too many tokens, must be ill-formed.
        return false;
      }
      ++i;
    }
    return true;
  }

  // Parse an ID from <int64_t_as_hex><colon><int64_t_as_hex>
  const char* lo_part = separator + 1;
  *separator = '\0';

  char* error_hi = NULL;
  char* error_lo = NULL;
  id->hi = strtoul(hi_part, &error_hi, 16);
  id->lo = strtoul(lo_part, &error_lo, 16);

  bool valid = *error_hi == '\0' && *error_lo == '\0';
  *separator = ':';
  return valid;
}

string PrintTuple(const Tuple* t, const TupleDescriptor& d) {
  if (t == NULL) return "null";
  stringstream out;
  out << "(";
  bool first_value = true;
  for (int i = 0; i < d.slots().size(); ++i) {
    SlotDescriptor* slot_d = d.slots()[i];
    if (first_value) {
      first_value = false;
    } else {
      out << " ";
    }
    if (t->IsNull(slot_d->null_indicator_offset())) {
      out << "null";
    } else if (slot_d->type().IsCollectionType()) {
      const TupleDescriptor* item_d = slot_d->children_tuple_descriptor();
      const CollectionValue* coll_value =
          reinterpret_cast<const CollectionValue*>(t->GetSlot(slot_d->tuple_offset()));
      uint8_t* coll_buf = coll_value->ptr;
      out << "[";
      for (int j = 0; j < coll_value->num_tuples; ++j) {
        out << PrintTuple(reinterpret_cast<Tuple*>(coll_buf), *item_d);
        coll_buf += item_d->byte_size();
      }
      out << "]";
    } else {
      string value_str;
      RawValue::PrintValue(
          t->GetSlot(slot_d->tuple_offset()), slot_d->type(), -1, &value_str);
      out << value_str;
    }
  }
  out << ")";
  return out.str();
}

string PrintRow(TupleRow* row, const RowDescriptor& d) {
  stringstream out;
  out << "[";
  for (int i = 0; i < d.tuple_descriptors().size(); ++i) {
    if (i != 0) out << " ";
    out << PrintTuple(row->GetTuple(i), *d.tuple_descriptors()[i]);
  }
  out << "]";
  return out.str();
}

string PrintBatch(RowBatch* batch) {
  stringstream out;
  for (int i = 0; i < batch->num_rows(); ++i) {
    out << PrintRow(batch->GetRow(i), *batch->row_desc()) << "\n";
  }
  return out.str();
}

string PrintPath(const TableDescriptor& tbl_desc, const SchemaPath& path) {
  stringstream ss;
  ss << tbl_desc.database() << "." << tbl_desc.name();
  const ColumnType* type = NULL;
  if (path.size() > 0) {
    ss << "." << tbl_desc.col_descs()[path[0]].name();
    type = &tbl_desc.col_descs()[path[0]].type();
  }
  for (int i = 1; i < path.size(); ++i) {
    ss << ".";
    switch (type->type) {
      case TYPE_ARRAY:
        if (path[i] == 0) {
          ss << "item";
          type = &type->children[0];
        } else {
          DCHECK_EQ(path[i], 1);
          ss << "pos";
          type = NULL;
        }
        break;
      case TYPE_MAP:
        if (path[i] == 0) {
          ss << "key";
          type = &type->children[0];
        } else if (path[i] == 1) {
          ss << "value";
          type = &type->children[1];
        } else {
          DCHECK_EQ(path[i], 2);
          ss << "pos";
          type = NULL;
        }
        break;
      case TYPE_STRUCT:
        DCHECK_LT(path[i], type->children.size());
        ss << type->field_names[path[i]];
        type = &type->children[path[i]];
        break;
      default:
        DCHECK_EQ(path.size() - 1, i) << PrintNumericPath(path) << " "
                                      << i <<" " << type->DebugString();
        ss << "(" << type->DebugString() << ")";
    }
  }
  return ss.str();
}

string PrintSubPath(const TableDescriptor& tbl_desc, const SchemaPath& path,
    int end_path_idx) {
  DCHECK_GE(end_path_idx, 0);
  SchemaPath::const_iterator subpath_end = path.begin() + end_path_idx + 1;
  SchemaPath subpath(path.begin(), subpath_end);
  return PrintPath(tbl_desc, subpath);
}

string PrintNumericPath(const SchemaPath& path) {
  stringstream ss;
  ss << "[";
  if (path.size() > 0) ss << path[0];
  for (int i = 1; i < path.size(); ++i) {
    ss << " ";
    ss << path[i];
  }
  ss << "]";
  return ss.str();
}

string PrintTableList(const vector<TTableName>& tbls) {
  stringstream ss;
  for (int i = 0; i < tbls.size(); ++i) {
    if (i != 0) ss << ",";
    ss << tbls[i].db_name << "." << tbls[i].table_name;
  }
  return ss.str();
}

string GetBuildVersion(bool compact) {
  stringstream ss;
  ss << GetDaemonBuildVersion()
#ifdef NDEBUG
     << " RELEASE"
#else
     << " DEBUG"
#endif
     << " (build " << GetDaemonBuildHash()
     << ")";
  if (!compact) {
    ss << endl << "Built on " << GetDaemonBuildTime();
  }
  return ss.str();
}

string GetVersionString(bool compact) {
  stringstream ss;
  ss << google::ProgramInvocationShortName()
     << " version " << GetBuildVersion(compact);
  return ss.str();
}

string GetStackTrace() {
  string s;
  google::glog_internal_namespace_::DumpStackTraceToString(&s);
  return s;
}

string GetBackendString() {
  return Substitute("$0:$1", FLAGS_hostname, FLAGS_krpc_port);
}

DebugActionTokens TokenizeDebugActions(const string& debug_actions) {
  DebugActionTokens results;
  vector<string> actions;
  split(actions, debug_actions, is_any_of("|"), token_compress_on);
  for (const string& a : actions) {
    vector<string> components;
    split(components, a, is_any_of(":"), token_compress_on);
    results.push_back(components);
  }
  return results;
}

vector<string> TokenizeDebugActionParams(const string& action) {
  vector<string> tokens;
  split(tokens, action, is_any_of("@"), token_compress_on);
  return tokens;
}

/// Helper to DebugActionImpl(). Given a probability as a string (e.g. "0.3"),
/// determine whether the action should be executed, as returned in 'should_execute'.
/// Returns true if the parsing was successful.
static bool ParseProbability(const string& prob_str, bool* should_execute) {
  StringParser::ParseResult parse_result;
  double probability = StringParser::StringToFloat<double>(
      prob_str.c_str(), prob_str.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS ||
      probability < 0.0 || probability > 1.0) {
    return false;
  }
  // +1L ensures probability of 0.0 and 1.0 work as expected.
  *should_execute = rand() < probability * (RAND_MAX + 1L);
  return true;
}

/// The catalog java code also implements a equivalent method for processing the debug
/// actions in the Java code. See DebugUtils.java for more details. Any changes to the
/// implementation logic here like adding a new type of action, should make changes in
/// the DebugUtils.java too.
Status DebugActionImpl(
    const string& debug_action, const char* label, const std::vector<string>& args) {
  const DebugActionTokens& action_list = TokenizeDebugActions(debug_action);
  static const char ERROR_MSG[] = "Invalid debug_action $0:$1 ($2)";
  for (const vector<string>& components : action_list) {
    // 'components' should be of the form {label, arg_0, ..., arg_n, action}
    if (components.size() != 2 + args.size() || !iequals(components[0], label)) {
      continue;
    }
    // Check if the arguments match.
    bool matches = true;
    for (int i = 0; i < args.size(); ++i) {
      if (!iequals(components[i + 1], args[i])) {
        matches = false;
        break;
      }
    }
    if (!matches) continue;
    const string& action_str = components[args.size() + 1];
    // 'tokens' becomes {command, param0, param1, ... }
    vector<string> tokens = TokenizeDebugActionParams(action_str);
    DCHECK_GE(tokens.size(), 1);
    const string& cmd = tokens[0];
    int sleep_millis = 0;
    if (iequals(cmd, "SLEEP")) {
      // SLEEP@<millis>
      if (tokens.size() != 2) {
        return Status(
            Substitute(ERROR_MSG, components[0], action_str, "expected SLEEP@<ms>"));
      }
      sleep_millis = atoi(tokens[1].c_str());
    } else if (iequals(cmd, "JITTER")) {
      // JITTER@<millis>[@<probability>}
      if (tokens.size() < 2 || tokens.size() > 3) {
        return Status(Substitute(ERROR_MSG, components[0], action_str,
            "expected JITTER@<ms>[@<probability>]"));
      }
      int max_millis = atoi(tokens[1].c_str());
      if (tokens.size() == 3) {
        bool should_execute = true;
        if (!ParseProbability(tokens[2], &should_execute)) {
          return Status(
              Substitute(ERROR_MSG, components[0], action_str, "invalid probability"));
        }
        if (!should_execute) continue;
      }
      sleep_millis = rand() % (max_millis + 1);
    } else if (iequals(cmd, "FAIL")) {
      // FAIL[@<probability>][@<error message>]
      if (tokens.size() > 3) {
        return Status(Substitute(ERROR_MSG, components[0], action_str,
            "expected FAIL[@<probability>][@<error message>]"));
      }
      if (tokens.size() >= 2) {
        bool should_execute = true;
        if (!ParseProbability(tokens[1], &should_execute)) {
          return Status(
              Substitute(ERROR_MSG, components[0], action_str, "invalid probability"));
        }
        if (!should_execute) continue;
      }
      string error_msg = tokens.size() == 3 ?
          tokens[2] :
          Substitute("Debug Action: $0:$1", components[0], action_str);

      if (ImpaladMetrics::DEBUG_ACTION_NUM_FAIL != nullptr) {
        ImpaladMetrics::DEBUG_ACTION_NUM_FAIL->Increment(1l);
      }
      return Status(TErrorCode::INTERNAL_ERROR, error_msg);
    } else if (iequals(cmd, "EXCEPTION")) {
      //EXCEPTION@<exception_type>
      if (tokens.size() != 2) {
        return Status(Substitute(ERROR_MSG, components[0], action_str,
            "expected EXCEPTION@<exception_type>"));
      }
      static const auto end = EXCEPTION_STR_MAP.end();
      auto it = EXCEPTION_STR_MAP.find(tokens[1]);
      if (it != end) {
        it->second();
      } else {
        return Status(
            Substitute(ERROR_MSG, components[0], action_str, "Invalid exception type"));
      }
    } else {
      DCHECK(false) << "Invalid debug action";
      return Status(Substitute(ERROR_MSG, components[0], action_str, "invalid command"));
    }
    if (sleep_millis > 0) {
      VLOG(1) << Substitute("Debug Action: $0:$1 sleeping for $2 ms", components[0],
          action_str, sleep_millis);
      SleepForMs(sleep_millis);
    }
  }
  return Status::OK();
}

}
