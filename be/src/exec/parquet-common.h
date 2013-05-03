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


#ifndef IMPALA_EXEC_PARQUET_COMMON_H
#define IMPALA_EXEC_PARQUET_COMMON_H

#include "gen-cpp/parquet_types.h"

// This file contains common elements between the parquet Writer and Scanner.
namespace impala {

const uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
const uint32_t PARQUET_CURRENT_VERSION = 1;

// Mapping of impala types to parquet storage types.  This is indexed by
// PrimitiveType enum
const parquet::Type::type IMPALA_TO_PARQUET_TYPES[] = { 
  parquet::Type::BOOLEAN,     // Invalid
  parquet::Type::BOOLEAN,     // NULL type
  parquet::Type::BOOLEAN,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT64,
  parquet::Type::FLOAT,
  parquet::Type::DOUBLE,
  parquet::Type::INT96,       // Timestamp
  parquet::Type::BYTE_ARRAY,
};

// Mapping of parquet codec enums to Impala enums
const THdfsCompression::type PARQUET_TO_IMPALA_CODEC[] = {
  THdfsCompression::NONE,
  THdfsCompression::SNAPPY,
  THdfsCompression::GZIP,
  THdfsCompression::LZO
};

}

#endif
