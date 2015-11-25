// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/debug-util.h"

#include <iomanip>
#include <sstream>
#include <boost/foreach.hpp>

#include "common/version.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "util/cpu-info.h"
#include "util/string-parser.h"
#include "util/uid-util.h"

// / WARNING this uses a private API of GLog: DumpStackTraceToString().
namespace google {
namespace glog_internal_namespace_ {
extern void DumpStackTraceToString(std::string* s);
}
}

#include "common/names.h"

using boost::char_separator;
using boost::tokenizer;
using namespace beeswax;
using namespace parquet;

namespace impala {

#define THRIFT_ENUM_OUTPUT_FN_IMPL(E, MAP) \
  ostream& operator<<(ostream& os, const E::type& e) {\
    map<int, const char*>::const_iterator i;\
    i = MAP.find(e);\
    if (i != MAP.end()) {\
      os << i->second;\
    }\
    return os;\
  }

// Macro to stamp out operator<< for thrift enums.  Why doesn't thrift do this?
#define THRIFT_ENUM_OUTPUT_FN(E) THRIFT_ENUM_OUTPUT_FN_IMPL(E , _##E##_VALUES_TO_NAMES)

// Macro to implement Print function that returns string for thrift enums
#define THRIFT_ENUM_PRINT_FN(E) \
  string Print##E(const E::type& e) {\
    stringstream ss;\
    ss << e;\
    return ss.str();\
  }

THRIFT_ENUM_OUTPUT_FN(TFunctionBinaryType);
THRIFT_ENUM_OUTPUT_FN(TCatalogObjectType);
THRIFT_ENUM_OUTPUT_FN(TDdlType);
THRIFT_ENUM_OUTPUT_FN(TCatalogOpType);
THRIFT_ENUM_OUTPUT_FN(THdfsFileFormat);
THRIFT_ENUM_OUTPUT_FN(THdfsCompression);
THRIFT_ENUM_OUTPUT_FN(TSessionType);
THRIFT_ENUM_OUTPUT_FN(TStmtType);
THRIFT_ENUM_OUTPUT_FN(QueryState);
THRIFT_ENUM_OUTPUT_FN(Encoding);
THRIFT_ENUM_OUTPUT_FN(CompressionCodec);
THRIFT_ENUM_OUTPUT_FN(Type);
THRIFT_ENUM_OUTPUT_FN(TMetricKind);
THRIFT_ENUM_OUTPUT_FN(TUnit);

THRIFT_ENUM_PRINT_FN(TCatalogObjectType);
THRIFT_ENUM_PRINT_FN(TDdlType);
THRIFT_ENUM_PRINT_FN(TCatalogOpType);
THRIFT_ENUM_PRINT_FN(TSessionType);
THRIFT_ENUM_PRINT_FN(TStmtType);
THRIFT_ENUM_PRINT_FN(QueryState);
THRIFT_ENUM_PRINT_FN(Encoding);
THRIFT_ENUM_PRINT_FN(TMetricKind);
THRIFT_ENUM_PRINT_FN(TUnit);


ostream& operator<<(ostream& os, const TUniqueId& id) {
  os << PrintId(id);
  return os;
}

string PrintId(const TUniqueId& id, const string& separator) {
  stringstream out;
  out << hex << id.hi << separator << id.lo;
  return out.str();
}

string PrintAsHex(const char* bytes, int64_t len) {
  stringstream out;
  out << hex << std::setfill('0');
  for (int i = 0; i < len; ++i) {
    out << setw(2) << static_cast<uint16_t>(bytes[i]);
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
    tokenizer< char_separator<char> > tokens(s, sep);
    int i = 0;
    BOOST_FOREACH(const string& token, tokens) {
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

string PrintPlanNodeType(const TPlanNodeType::type& type) {
  map<int, const char*>::const_iterator i;
  i = _TPlanNodeType_VALUES_TO_NAMES.find(type);
  if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
    return i->second;
  }
  return "Invalid plan node type";
}

string PrintTuple(const Tuple* t, const TupleDescriptor& d) {
  if (t == NULL) return "null";
  stringstream out;
  out << "(";
  bool first_value = true;
  for (int i = 0; i < d.slots().size(); ++i) {
    SlotDescriptor* slot_d = d.slots()[i];
    if (!slot_d->is_materialized()) continue;
    if (first_value) {
      first_value = false;
    } else {
      out << " ";
    }
    if (t->IsNull(slot_d->null_indicator_offset())) {
      out << "null";
    } else if (slot_d->type().IsCollectionType()) {
      const TupleDescriptor* item_d = slot_d->collection_item_descriptor();
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
    out << PrintRow(batch->GetRow(i), batch->row_desc()) << "\n";
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
        DCHECK(false) << PrintNumericPath(path) << " " << i << " " << type->DebugString();
        return PrintNumericPath(path);
    }
  }
  return ss.str();
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

string GetBuildVersion(bool compact) {
  stringstream ss;
  ss << IMPALA_BUILD_VERSION
#ifdef NDEBUG
     << " RELEASE"
#else
     << " DEBUG"
#endif
     << " (build " << IMPALA_BUILD_HASH
     << ")";
  if (!compact) {
    ss << endl << "Built on " << IMPALA_BUILD_TIME;
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

}
