// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_PRIMITIVE_TYPE_H
#define IMPALA_RUNTIME_PRIMITIVE_TYPE_H

#include <string>

#include "gen-cpp/Types_types.h"  // for TPrimitiveType

namespace impala {

enum PrimitiveType {
  INVALID_TYPE = 0,
  TYPE_BOOLEAN,
  TYPE_TINYINT,
  TYPE_SMALLINT,
  TYPE_INT,
  TYPE_BIGINT,
  TYPE_FLOAT,
  TYPE_DOUBLE,
  TYPE_DATE,
  TYPE_DATETIME,
  TYPE_TIMESTAMP,
  TYPE_STRING
};

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
std::string TypeToString(PrimitiveType t);

}

#endif
