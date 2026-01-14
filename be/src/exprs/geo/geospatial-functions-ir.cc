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

#include "exprs/geo/geospatial-functions.h"

#include "exprs/geo/common.h"
#include "exprs/geo/shape-format.h"
#include "runtime/string-value.inline.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "common/names.h"

namespace impala::geo {

// Accessors

DoubleVal GeospatialFunctions::st_X(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null(); // Only valid for ST_POINT.
  return DoubleVal(getMinX(geom));
}

DoubleVal GeospatialFunctions::st_Y(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null();  // Only valid for ST_POINT.
  return DoubleVal(getMinY(geom));
}

DoubleVal GeospatialFunctions::st_MinX(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  return DoubleVal(getMinX(geom));
}

DoubleVal GeospatialFunctions::st_MinY(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  return DoubleVal(getMinY(geom));
}

DoubleVal GeospatialFunctions::st_MaxX(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(getMinX(geom));
  return DoubleVal(getMaxX(geom));
}

DoubleVal GeospatialFunctions::st_MaxY(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(getMinY(geom));
  return DoubleVal(getMaxY(geom));
}

StringVal GeospatialFunctions::st_GeometryType(FunctionContext* ctx,
    const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();
  const char* name = getGeometryType(ogc_type);

  return StringVal(name);
}

IntVal GeospatialFunctions::st_Srid(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return IntVal::null();
  return getSrid(geom);
}

StringVal GeospatialFunctions::st_SetSrid(FunctionContext* ctx, const StringVal& geom,
    const IntVal& srid) {
  if (srid.is_null) return geom;
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();

  StringVal res = StringVal::CopyFrom(ctx, geom.ptr, geom.len);
  setSrid(res, srid.val);
  return res;
}

// Constructors

StringVal GeospatialFunctions::st_Point(FunctionContext* ctx, const DoubleVal& x,
    const DoubleVal& y) {
  if (x.is_null) return StringVal::null();
  if (y.is_null) return StringVal::null();
  return createStPoint(ctx, x.val, y.val, 0);
}

// Predicates

BooleanVal GeospatialFunctions::st_EnvIntersects(
    FunctionContext* ctx, const StringVal& lhs_geom,const StringVal& rhs_geom) {
  OGCType lhs_type, rhs_type;
  // TODO: compare srid? The ESRI UDF does it, but it is not done in other relations:
  //   https://github.com/apache/hive/blob/rel/release-4.2.0/ql/src/java/org/apache/hadoop/hive/ql/udf/esri/ST_EnvIntersects.java#L63
  if (!ParseHeader(ctx, lhs_geom, &lhs_type) || !ParseHeader(ctx, rhs_geom, &rhs_type)) {
    return BooleanVal::null();
  }
  bool result = bBoxIntersects(lhs_geom, rhs_geom, lhs_type, rhs_type);
  return BooleanVal(result);
}

}
