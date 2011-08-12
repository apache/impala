// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "util/debug-string-util.h"

using namespace std;

namespace impala {

ostream& operator<<(ostream& os, const TExprOperator::type& op) {
  map<int, const char*>::const_iterator i = _TExprOperator_VALUES_TO_NAMES.find(op);
  if (i != _TExprOperator_VALUES_TO_NAMES.end()) {
    os << i->second;
  }
  return os;
}

}
