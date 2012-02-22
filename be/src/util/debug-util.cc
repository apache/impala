// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "util/debug-util.h"

#include <iomanip>
#include <sstream>

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

#define MILLION (1000000)
#define THOUSAND (1000)

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

string PrintId(const TUniqueId& id) {
  stringstream out;
  out << id.hi << "|" << id.lo;
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
     out << PrintTuple(row->GetTuple(i), *d.tuple_descriptors()[i]);
  }
  out << "]";
  return out.str();
}

string PrettyPrinter::Print(int64_t value, TCounterType::type type) {
  stringstream ss;
  ss.flags(ios::fixed);
  switch (type) {
    case TCounterType::UNIT:
      if (value >= MILLION) {
        ss << setprecision(PRECISION) << value / 1000000. << "M";
      } else if (value >= THOUSAND) {
        ss << setprecision(PRECISION) << value / 1000. << "K";
      } else {
        ss << value;
      }
      break;

    case TCounterType::CPU_TICKS:
      value /= CpuInfo::Instance()->cycles_per_ms();
      // fall-through
    case TCounterType::TIME_MS:
      if (value == 0 ) {
        ss << "0";
        break;
      } else {
        bool hour = false;
        bool minute = false;
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
        }
        if (!hour && !minute) {
          ss << value << "ms";
        }
      }
      break;

    case TCounterType::BYTES:
      if (value == 0) {
        ss << value;
      } else if (value > GIGABYTE) {
        ss << setprecision(PRECISION) << value / (double) GIGABYTE << " GB";
      } else if (value > MEGABYTE ) {
        ss << setprecision(PRECISION) << value / (double) MEGABYTE << " MB";
      } else if (value > KILOBYTE)  {
        ss << setprecision(PRECISION) << value / (double) KILOBYTE << " KB";
      } else {
        ss << value << " B";
      }
      break;

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

}
