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

#include "common/logging.h"

#include "util/debug-util.h"

#include <iomanip>
#include <sstream>

#include "common/logging.h"
#include "common/version.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "util/cpu-info.h"
#include "gen-cpp/Opcodes_types.h"

#define PRECISION 2
#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)
#define GIGABYTE (1024 * 1024 * 1024)

#define SECOND (1000)
#define MINUTE (1000 * 60)
#define HOUR (1000 * 60 * 60)

#define THOUSAND (1000)
#define MILLION (THOUSAND * 1000)
#define BILLION (MILLION * 1000)

using namespace std;

namespace impala {

ostream& operator<<(ostream& os, const TExprOpcode::type& op) {
  map<int, const char*>::const_iterator i;
  i = _TExprOpcode_VALUES_TO_NAMES.find(op);
  if (i != _TExprOpcode_VALUES_TO_NAMES.end()) {
    os << i->second;
  }
  return os;
}

ostream& operator<<(ostream& os, const TAggregationOp::type& op) {
  map<int, const char*>::const_iterator i;
  i = _TAggregationOp_VALUES_TO_NAMES.find(op);
  if (i != _TAggregationOp_VALUES_TO_NAMES.end()) {
    os << i->second;
  }
  return os;
}

ostream& operator<<(ostream& os, const TUniqueId& id) {
  os << PrintId(id);
  return os;
}

string PrintId(const TUniqueId& id) {
  stringstream out;
  out << std::hex << id.hi << ":" << id.lo;
  return out.str();
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
    } else {
      string value_str;
      RawValue::PrintValue(
          t->GetSlot(slot_d->tuple_offset()), slot_d->type(), &value_str);
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

static double GetByteUnit(int64_t value, string* unit) {
  if (value == 0) {
    *unit = "";
    return value;
  } else if (value > GIGABYTE) {
    *unit = "GB";
    return value /(double) GIGABYTE;
  } else if (value > MEGABYTE ) {
    *unit = "MB";
    return value /(double) MEGABYTE;
  } else if (value > KILOBYTE)  {
    *unit = "KB";
    return value /(double) KILOBYTE;
  } else {
    *unit = "B";
    return value;
  }
}

static double GetUnit(int64_t value, string* unit) {
  if (value >= BILLION) {
    *unit = "B";
    return value / (1000*1000*1000.);
  } else if (value >= MILLION) {
    *unit = "M";
    return value / (1000*1000.);
  } else if (value >= THOUSAND) {
    *unit = "K";
    return value / (1000.);
  } else {
    *unit = "";
    return value;
  }
}

string PrettyPrinter::Print(int64_t value, TCounterType::type type) {
  stringstream ss;
  ss.flags(ios::fixed);
  switch (type) {
    case TCounterType::UNIT: {
      string unit;
      double output = GetUnit(value, &unit);
      if (unit.empty()) {
        ss << value;
      } else {
        ss << setprecision(PRECISION) << output << unit;
      }
      break;
    }

    case TCounterType::UNIT_PER_SECOND: {
      string unit;
      double output = GetUnit(value, &unit);
      if (output == 0) {
        ss << "0";
      } else {
        ss << setprecision(PRECISION) << output << " " << unit << "/sec";
      }
      break;
    }

    case TCounterType::CPU_TICKS:
      if (value < CpuInfo::cycles_per_ms()) {
        ss << (value / 1000) << "K clock cycles";
        break;
      } else {
        value /= CpuInfo::cycles_per_ms();
        // fall-through
      }
    case TCounterType::TIME_MS:
      if (value == 0 ) {
        ss << "0";
        break;
      } else {
        bool hour = false;
        bool minute = false;
        bool second = false;
        if (value >= HOUR) {
          ss << value / HOUR << "h";
          value %= HOUR;
          hour = true;
        }
        if (value >= MINUTE) {
          ss << value / MINUTE << "m";
          value %= MINUTE;
          minute = true;
        }
        if (!hour && value >= SECOND) {
          ss << value / SECOND << "s";
          value %= SECOND;
          second = true;
        }
        if (!hour && !minute) {
          if (second) ss << setw(3) << setfill('0');
          ss << value << "ms";
        }
      }
      break;

    case TCounterType::BYTES: {
      string unit;
      double output = GetByteUnit(value, &unit);
      ss << setprecision(PRECISION) << output << " " << unit;
      break;
    }

    case TCounterType::BYTES_PER_SECOND: {
      string unit;
      double output = GetByteUnit(value, &unit);
      ss << setprecision(PRECISION) << output << " " << unit << "/sec";
      break;
    }

    default:
      DCHECK(false);
      break;
  }
  return ss.str();
}

string PrintBatch(RowBatch* batch) {
  stringstream out;
  for (int i = 0; i < batch->num_rows(); ++i) {
    out << PrintRow(batch->GetRow(i), batch->row_desc()) << "\n";
  }
  return out.str();
}

string GetBuildVersion() {
  stringstream ss;
  ss << Version::BUILD_VERSION
#ifdef NDEBUG
     << " release"
#else
     << " debug"
#endif
     << " (build " << Version::BUILD_HASH
     << ")" << endl
     << "Built on " << Version::BUILD_TIME;
  return ss.str();
}

string GetVersionString() {
  stringstream ss;
  ss << google::ProgramInvocationShortName()
     << " version " << google::VersionString();
  return ss.str();
}

string GetStackTrace() {
  string s;
  google::glog_internal_namespace_::DumpStackTraceToString(&s);
  return s;
}

}
