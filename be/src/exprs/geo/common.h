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

#include "udf/udf.h"
#include "util/bit-util.h"

namespace impala::geo {

using impala_udf::FunctionContext;
using impala_udf::StringVal;

// see https://github.com/Esri/spatial-framework-for-hadoop/blob/v2.2.0/hive/src/main/java/com/esri/hadoop/hive/GeometryUtils.java#L21
enum OGCType {
    UNKNOWN = 0,
    ST_POINT = 1,
    ST_LINESTRING = 2,
    ST_POLYGON = 3,
    ST_MULTIPOINT = 4,
    ST_MULTILINESTRING = 5,
    ST_MULTIPOLYGON = 6
};

constexpr std::array<const char*, ST_MULTIPOLYGON + 1> OGCTypeToStr = {{
    "UNKNOWN",
    "ST_POINT",
    "ST_LINESTRING",
    "ST_POLYGON",
    "ST_MULTIPOINT",
    "ST_MULTILINESTRING",
    "ST_MULTIPOLYGON"
}};

} // namespace impala
