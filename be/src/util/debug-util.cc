// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "util/debug-util.h"

#include <sstream>

#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"

using namespace std;

namespace impala {

ostream& operator<<(ostream& os, const TExprOperator::type& op) {
  map<int, const char*>::const_iterator i = _TExprOperator_VALUES_TO_NAMES.find(op);
  if (i != _TExprOperator_VALUES_TO_NAMES.end()) {
    os << i->second;
  }
  return os;
}

string PrintTuple(Tuple* t, const TupleDescriptor& d) {
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

}
