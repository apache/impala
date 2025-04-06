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

#include <cstdint>

#include "exprs/geo/common.h"

namespace impala::geo {

// This file is responsible for handling the header of the "esri shape" format used for
// geometries encoded as BINARY. This format is fully compatible with Java framework
// https://github.com/Esri/spatial-framework-for-hadoop
// A 5 byte "OGC" header followed by the same format as the one used in shape files:
// https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf
//
// The OGC header contains:
// - 4 byte big endian SRID (reference system id)
// - 1 byte type id (OGCType)
// - no padding
// see https://github.com/Esri/spatial-framework-for-hadoop/blob/v2.2.0/hive/src/main/java/com/esri/hadoop/hive/GeometryUtils.java#L16
//
// The header of the shape file format ("ESRI header") contains:
// - 4 byte type id
// for POINT type store the coordinates:
//   - dimension * 8 byte to store x/y/z/m as doubles
// for other types store the bounding box:
//   - min coordinates: 2 * 8 byte to store x/y as doubles
//   - max coordinates: 2 * 8 byte to store x/y as doubles
//   - min/max z/m are stored later in the headers - this file doesn't access those
// - no padding
//
// For some types this is follewed by a variable length part, which is not handled here.
// A POC example for handling the a full type:
// https://gerrit.cloudera.org/#/c/20602/6/be/src/exprs/geo/poly-line-shape-format.cc
//
// Currently only 2 dimensions are handled (xy), min/max for z/m has to be accessed
// with Java functions. The xy bounding box has the same offset and format in
// xyz/xym/xyzm geometries so x/y accessors work in this case too.
//
// Functions are defined in the header to allow inlining bounding box check in codegen.

constexpr int SRID_SIZE = 4;
constexpr int OGC_TYPE_SIZE = 1;

constexpr int SRID_OFFSET = 0;
constexpr int OGC_TYPE_OFFSET = 4;

static_assert(OGC_TYPE_OFFSET == SRID_SIZE);


constexpr int ESRI_TYPE_SIZE = 4;
constexpr int ESRI_TYPE_OFFSET = 5;

constexpr int X1_OFFSET = 9;
constexpr int Y1_OFFSET = X1_OFFSET + sizeof(double);
constexpr int X2_OFFSET = Y1_OFFSET + sizeof(double);
constexpr int Y2_OFFSET = X2_OFFSET + sizeof(double);

constexpr int MIN_GEOM_SIZE = 9;
constexpr int MIN_POINT_SIZE = 25;
constexpr int MIN_NON_POINT_SIZE = 41;

static_assert(ESRI_TYPE_OFFSET == OGC_TYPE_OFFSET + OGC_TYPE_SIZE);
static_assert(X1_OFFSET == ESRI_TYPE_OFFSET + ESRI_TYPE_SIZE);
static_assert(MIN_GEOM_SIZE == SRID_SIZE + OGC_TYPE_SIZE + ESRI_TYPE_SIZE);
static_assert(MIN_POINT_SIZE ==  MIN_GEOM_SIZE + 2 * sizeof(double));
static_assert(MIN_NON_POINT_SIZE ==  MIN_POINT_SIZE + 2 * sizeof(double));

// See https://github.com/Esri/geometry-api-java/blob/v2.2.4/src/main/java/com/esri/core/geometry/ShapeType.java#L27
enum EsriType: uint32_t {
  ShapeNull = 0,
  ShapePoint = 1,
  ShapePointM = 21,
  ShapePointZM = 11,
  ShapePointZ = 9,
  ShapeMultiPoint = 8,
  ShapeMultiPointM = 28,
  ShapeMultiPointZM = 18,
  ShapeMultiPointZ = 20,
  ShapePolyline = 3,
  ShapePolylineM = 23,
  ShapePolylineZM = 13,
  ShapePolylineZ = 10,
  ShapePolygon = 5,
  ShapePolygonM = 25,
  ShapePolygonZM = 15,
  ShapePolygonZ = 19,
  ShapeMultiPatchM = 31,
  ShapeMultiPatch = 32,
  ShapeGeneralPolyline = 50,
  ShapeGeneralPolygon = 51,
  ShapeGeneralPoint = 52,
  ShapeGeneralMultiPoint = 53,
  ShapeGeneralMultiPatch = 54,
  ShapeTypeLast = 55
};

constexpr std::array<EsriType, ST_MULTIPOLYGON + 1> OGCTypeToEsriType = {{
  ShapeNull,       // UNKNOWN
  ShapePoint,      // ST_POINT
  ShapePolyline,   // ST_LINESTRING
  ShapePolygon,    // ST_POLYGON
  ShapeMultiPoint, // ST_MULTIPOINT
  ShapePolyline,   // ST_MULTILINESTRING
  ShapePolygon     // ST_MULTIPOLYGON
}};

template <class T>
T readFromGeom(const StringVal& geom, int offset) {
  DCHECK_GE(geom.len, offset + sizeof(T));
  return *reinterpret_cast<T*>(geom.ptr + offset);
}

template <class T>
void writeToGeom(const T& val, StringVal& geom, int offset) {
  DCHECK_GE(geom.len, offset + sizeof(T));
  T* ptr = reinterpret_cast<T*>(geom.ptr + offset);
  *ptr = val;
}

// getters/setters for OGC header:

inline uint32_t getSrid(const StringVal& geom) {
  static_assert(SRID_SIZE == sizeof(uint32_t));

  // SRID is in big endian format in 'geom', but Impala only supports little endian so we
  // have to convert it.
#ifndef IS_LITTLE_ENDIAN
  static_assert(false, "Only the little endian byte order is supported.");
#endif
  const uint32_t srid_bytes = readFromGeom<uint32_t>(geom, SRID_OFFSET);
  return BitUtil::ByteSwap(srid_bytes);
}

inline OGCType getOGCType(const StringVal& geom) {
  static_assert(OGC_TYPE_SIZE == sizeof(char));
  const char res = readFromGeom<char>(geom, OGC_TYPE_OFFSET);
  return static_cast<OGCType>(res);
}

inline constexpr const char* getGeometryType(OGCType ogc_type) {
  return OGCTypeToStr[ogc_type];
}

inline void setSrid(StringVal& geom, uint32_t srid) {
  static_assert(SRID_SIZE == sizeof(uint32_t));

  // SRID is in big endian format in 'geom', but Impala only supports little endian so we
  // have to convert it.
#ifndef IS_LITTLE_ENDIAN
  static_assert(false, "Only the little endian byte order is supported.");
#endif
  const uint32_t srid_bytes = BitUtil::ByteSwap(srid);
  writeToGeom<uint32_t>(srid_bytes, geom, SRID_OFFSET);
}

inline void setOGCType(StringVal& geom, OGCType ogc_type) {
  writeToGeom<char>(ogc_type, geom, OGC_TYPE_OFFSET);
}

// getters/setters for ESRI header:

inline EsriType getEsriType(const StringVal& geom) {
  static_assert(ESRI_TYPE_SIZE == sizeof(EsriType));
  return readFromGeom<EsriType>(geom, ESRI_TYPE_OFFSET);
}

inline double getMinX(const StringVal& geom) {
  return readFromGeom<double>(geom, X1_OFFSET);
}

inline double getMinY(const StringVal& geom) {
  return readFromGeom<double>(geom, Y1_OFFSET);
}

inline double getMaxX(const StringVal& geom) {
  return readFromGeom<double>(geom, X2_OFFSET);
}

inline double getMaxY(const StringVal& geom) {
  return readFromGeom<double>(geom, Y2_OFFSET);
}

inline void setEsriType(StringVal& geom, EsriType esri_type) {
  static_assert(ESRI_TYPE_SIZE == sizeof(EsriType));
  writeToGeom<EsriType>(esri_type, geom, ESRI_TYPE_OFFSET);
}

inline void setMinX(StringVal& geom, double x) {
  writeToGeom<double>(x, geom, X1_OFFSET);
}

inline void setMinY(StringVal& geom, double y) {
  writeToGeom<double>(y, geom, Y1_OFFSET);
}

inline void setMaxX(StringVal& geom, double x) {
  writeToGeom<double>(x, geom, X2_OFFSET);
}

inline void setMaxY(StringVal& geom, double y) {
  writeToGeom<double>(y, geom, Y2_OFFSET);
}

// Validate header and get type
inline bool ParseHeader(FunctionContext* ctx, const StringVal& geom, OGCType* ogc_type) {
  DCHECK(ogc_type != nullptr);

  if (UNLIKELY(geom.is_null)) return false;

  if (UNLIKELY(geom.len < MIN_GEOM_SIZE)) {
    ctx->AddWarning("Geometry size too small.");
    return false;
  }

  const OGCType unchecked_ogc_type = getOGCType(geom);
  if (UNLIKELY(unchecked_ogc_type < UNKNOWN || unchecked_ogc_type > ST_MULTIPOLYGON)) {
    ctx->AddWarning("Invalid geometry type.");
    return false;
  }

  if (UNLIKELY(unchecked_ogc_type == UNKNOWN)) {
    ctx->AddWarning("Geometry type UNKNOWN.");
    return false;
  }

  if (UNLIKELY(unchecked_ogc_type == ST_POINT)) {
    if (geom.len < MIN_POINT_SIZE) {
      ctx->AddWarning("Geometry size too small for ST_POINT type.");
      return false;
    }
  } else {
    if (UNLIKELY(geom.len < MIN_NON_POINT_SIZE)) {
      ctx->AddWarning("Geometry size too small for non ST_POINT type.");
      return false;
    }
  }

  // TODO: fix Z/M/ZM types and move to a function called from DCHECK
  // ogc vs ESRI type checking can be useful during development, but it
  // is unnecessary overhead in production
  /*const EsriType esri_type = getEsriType(geom);
  DCHECK_LT(unchecked_ogc_type, OGCTypeToEsriType.size());
  const EsriType expected_esri_type = OGCTypeToEsriType[unchecked_ogc_type];
  if (expected_esri_type != esri_type) {
    // TODO: To test it we need to create a table with 3D types, we cannot create them
    // with native constructors.
    ctx->SetError(strings::Substitute(
          "Invalid geometry: OGCType and EsriType do not match. "
          "Because the OGCType is $0, expected EsriType $1, found $2.",
          OGCTypeToStr[unchecked_ogc_type], expected_esri_type, esri_type).c_str());
  }*/

  *ogc_type = static_cast<OGCType>(unchecked_ogc_type);
  return true;
}

// Bounding box check for x/y coordinates of two geometries. z/m are ignored, which
// is consistent with the original Java functions.
inline bool bBoxIntersects(const StringVal& lhs_geom, const StringVal rhs_geom,
  OGCType lhs_type, OGCType rhs_type) {
  bool is_lhs_point = lhs_type == ST_POINT;
  double xmin1 = getMinX(lhs_geom);
  double ymin1 = getMinY(lhs_geom);
  double xmax1 = is_lhs_point ? xmin1 : getMaxX(lhs_geom);
  double ymax1 = is_lhs_point ? ymin1 : getMaxY(lhs_geom);

  bool is_rhs_point = rhs_type == ST_POINT;
  double xmin2 = getMinX(rhs_geom);
  double ymin2 = getMinY(rhs_geom);
  double xmax2 = is_rhs_point ? xmin2 : getMaxX(rhs_geom);
  double ymax2 = is_rhs_point ? ymin2 : getMaxY(rhs_geom);

  if (xmax1 < xmin2 || xmax2 < xmin1 || ymax1 < ymin2 || ymax2 < ymin1 ) return false;
  return true;
}

} // namespace impala
