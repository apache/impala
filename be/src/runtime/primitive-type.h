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


#ifndef IMPALA_RUNTIME_PRIMITIVE_TYPE_H
#define IMPALA_RUNTIME_PRIMITIVE_TYPE_H

#include <string>

#include "common/logging.h"
#include "gen-cpp/Types_types.h"  // for TPrimitiveType
#include "gen-cpp/cli_service_types.h"  // for HiveServer2 Type

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
  TYPE_TIMESTAMP,
  TYPE_STRING,
  TYPE_DATE,        // Not implemented
  TYPE_DATETIME,    // Not implemented
};

// Returns the byte size of 'type'  Returns 0 for variable length types.
inline int GetByteSize(PrimitiveType type) {
  switch (type) {
    case TYPE_STRING:
      return 0;
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
      return 1;
    case TYPE_SMALLINT:
      return 2;
    case TYPE_INT:
    case TYPE_FLOAT:
      return 4;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
      return 8;
    case TYPE_TIMESTAMP:
      // This is the size of the slot, the actual size of the data is 12.
      return 16;
    case TYPE_DATE:
    case INVALID_TYPE:
    default:
      DCHECK(false); 
  }
  return 0;
}

PrimitiveType ThriftToType(TPrimitiveType::type ttype);
TPrimitiveType::type ToThrift(PrimitiveType ptype);
std::string TypeToString(PrimitiveType t);
std::string TypeToOdbcString(PrimitiveType t);
apache::hive::service::cli::thrift::TType::type TypeToHiveServer2Type(PrimitiveType t);
}

#endif
