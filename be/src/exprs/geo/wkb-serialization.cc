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

#include "exprs/geo/wkb-serialization.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/names.h"

namespace impala::geo {

using namespace std;

// --- Helper: write WKB header into buffer ---
static inline void writeWkbHeader(uint8_t*& ptr, uint32_t wkb_type) {
  *ptr++ = 0x01;  // LE
  WriteUint32(ptr, wkb_type);
}

// --- Read functions ---

template <typename Container>
static inline void readPoints(const uint8_t*& ptr, Container& dst, uint32_t count,
    bool big_endian) {
  dst.reserve(count);
  for (uint32_t i = 0; i < count; i++) {
    double x = ReadDouble(ptr, big_endian);
    double y = ReadDouble(ptr, big_endian);
    dst.emplace_back(x, y);
  }
}

template <typename Container>
static inline void writePoints(uint8_t*& ptr, const Container& src) {
  for (const auto& p : src) {
    WriteDouble(ptr, p.x());
    WriteDouble(ptr, p.y());
  }
}

bool ReadWkbPoint(const uint8_t* data, int len, point2d& result, bool big_endian) {
  if (len < WKB_POINT_SIZE) return false;
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  double x = ReadDouble(ptr, big_endian);
  double y = ReadDouble(ptr, big_endian);
  result = point2d(x, y);
  return true;
}

static bool ReadLineStringBody(const uint8_t*& ptr, const uint8_t* end,
    linestring2d& result, bool big_endian) {
  if (ptr + 4 > end) return false;
  uint32_t num_points = ReadUint32(ptr, big_endian);
  if (ptr + (uint64_t)num_points * 16 > end) return false;
  result.clear();
  readPoints(ptr, result, num_points, big_endian);
  return true;
}

static bool ReadPolygonBody(const uint8_t*& ptr, const uint8_t* end,
    polygon2d& result, bool big_endian) {
  if (ptr + 4 > end) return false;
  uint32_t num_rings = ReadUint32(ptr, big_endian);
  result.clear();
  for (uint32_t r = 0; r < num_rings; r++) {
    if (ptr + 4 > end) return false;
    uint32_t num_points = ReadUint32(ptr, big_endian);
    if (ptr + (uint64_t)num_points * 16 > end) return false;
    if (r == 0) {
      readPoints(ptr, result.outer(), num_points, big_endian);
    } else {
      result.inners().emplace_back();
      readPoints(ptr, result.inners().back(), num_points, big_endian);
    }
  }
  bg::correct(result);
  return true;
}

bool ReadWkbLineString(
    const uint8_t* data, int len, linestring2d& result, bool big_endian) {
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;
  return ReadLineStringBody(ptr, end, result, big_endian);
}

bool ReadWkbPolygon(const uint8_t* data, int len, polygon2d& result, bool big_endian) {
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;
  return ReadPolygonBody(ptr, end, result, big_endian);
}

bool ReadWkbMultiPoint(
    const uint8_t* data, int len, multipoint2d& result, bool big_endian) {
  if (len < WKB_HEADER_SIZE + 4) return false;
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;
  uint32_t num_geoms = ReadUint32(ptr, big_endian);
  if (ptr + (uint64_t)num_geoms * WKB_POINT_SIZE > end) return false;

  result.clear();
  result.reserve(num_geoms);
  for (uint32_t i = 0; i < num_geoms; i++) {
    if (ptr + WKB_POINT_SIZE > end) return false;
    ptr += WKB_HEADER_SIZE;
    double x = ReadDouble(ptr, big_endian);
    double y = ReadDouble(ptr, big_endian);
    result.emplace_back(x, y);
  }
  return true;
}

bool ReadWkbMultiLineString(const uint8_t* data, int len, multi_linestring2d& result,
    bool big_endian) {
  if (len < WKB_HEADER_SIZE + 4) return false;
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;
  uint32_t num_geoms = ReadUint32(ptr, big_endian);
  if (ptr + (uint64_t)num_geoms * (WKB_HEADER_SIZE + 4) > end) return false;

  result.clear();
  result.resize(num_geoms);
  for (uint32_t i = 0; i < num_geoms; i++) {
    if (ptr + WKB_HEADER_SIZE > end) return false;
    ptr += WKB_HEADER_SIZE;
    if (!ReadLineStringBody(ptr, end, result[i], big_endian)) return false;
  }
  return true;
}

bool ReadWkbMultiPolygon(const uint8_t* data, int len, multi_polygon2d& result,
    bool big_endian) {
  if (len < WKB_HEADER_SIZE + 4) return false;
  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;
  uint32_t num_geoms = ReadUint32(ptr, big_endian);
  if (ptr + (uint64_t)num_geoms * (WKB_HEADER_SIZE + 4) > end) return false;

  result.clear();
  result.resize(num_geoms);
  for (uint32_t i = 0; i < num_geoms; i++) {
    if (ptr + WKB_HEADER_SIZE > end) return false;
    ptr += WKB_HEADER_SIZE;
    if (!ReadPolygonBody(ptr, end, result[i], big_endian)) return false;
  }
  return true;
}

// --- Write functions ---

StringVal WriteWkbPoint(FunctionContext* ctx, const point2d& point) {
  StringVal result(ctx, WKB_POINT_SIZE);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  writeWkbHeader(ptr, WKB_POINT);
  WriteDouble(ptr, point.x());
  WriteDouble(ptr, point.y());
  return result;
}

static int LineStringSize(const linestring2d& ls) {
  return WKB_HEADER_SIZE + 4 + ls.size() * 16;
}

static int PolygonSize(const polygon2d& poly) {
  int size = WKB_HEADER_SIZE + 4;
  size += 4 + poly.outer().size() * 16;
  for (const auto& inner : poly.inners()) {
    size += 4 + inner.size() * 16;
  }
  return size;
}

static void WriteLineStringBody(uint8_t*& ptr, const linestring2d& ls) {
  writeWkbHeader(ptr, WKB_LINESTRING);
  WriteUint32(ptr, ls.size());
  writePoints(ptr, ls);
}

static void WritePolygonBody(uint8_t*& ptr, const polygon2d& poly) {
  uint32_t num_rings = 1 + poly.inners().size();
  writeWkbHeader(ptr, WKB_POLYGON);
  WriteUint32(ptr, num_rings);
  WriteUint32(ptr, poly.outer().size());
  writePoints(ptr, poly.outer());
  for (const auto& inner : poly.inners()) {
    WriteUint32(ptr, inner.size());
    writePoints(ptr, inner);
  }
}

StringVal WriteWkbLineString(FunctionContext* ctx, const linestring2d& ls) {
  StringVal result(ctx, LineStringSize(ls));
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  WriteLineStringBody(ptr, ls);
  return result;
}

StringVal WriteWkbPolygon(FunctionContext* ctx, const polygon2d& poly) {
  StringVal result(ctx, PolygonSize(poly));
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  WritePolygonBody(ptr, poly);
  return result;
}

StringVal WriteWkbMultiPoint(FunctionContext* ctx, const multipoint2d& mp) {
  uint32_t num_geoms = mp.size();
  int size = WKB_HEADER_SIZE + 4 + num_geoms * WKB_POINT_SIZE;
  StringVal result(ctx, size);
  if (UNLIKELY(result.is_null)) return StringVal::null();

  uint8_t* ptr = result.ptr;
  writeWkbHeader(ptr, WKB_MULTIPOINT);
  WriteUint32(ptr, num_geoms);
  for (const auto& p : mp) {
    writeWkbHeader(ptr, WKB_POINT);
    WriteDouble(ptr, p.x());
    WriteDouble(ptr, p.y());
  }
  return result;
}

StringVal WriteWkbMultiLineString(FunctionContext* ctx, const multi_linestring2d& mls) {
  int size = WKB_HEADER_SIZE + 4;
  for (const auto& ls : mls) size += LineStringSize(ls);

  StringVal result(ctx, size);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  writeWkbHeader(ptr, WKB_MULTILINESTRING);
  WriteUint32(ptr, (uint32_t)mls.size());
  for (const auto& ls : mls) WriteLineStringBody(ptr, ls);
  return result;
}

StringVal WriteWkbMultiPolygon(FunctionContext* ctx, const multi_polygon2d& mpoly) {
  int size = WKB_HEADER_SIZE + 4;
  for (const auto& poly : mpoly) size += PolygonSize(poly);

  StringVal result(ctx, size);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  writeWkbHeader(ptr, WKB_MULTIPOLYGON);
  WriteUint32(ptr, (uint32_t)mpoly.size());
  for (const auto& poly : mpoly) WritePolygonBody(ptr, poly);
  return result;
}

StringVal WriteWkbBox(FunctionContext* ctx, const box2d& box) {
  // Write as a 5-point closed polygon (rectangle).
  double xmin = box.min_corner().x();
  double ymin = box.min_corner().y();
  double xmax = box.max_corner().x();
  double ymax = box.max_corner().y();

  polygon2d poly;
  poly.outer().reserve(5);
  // CW order: matches polygon2d convention (bg::model::polygon<…, ClockWise=true>).
  poly.outer().emplace_back(xmin, ymin);
  poly.outer().emplace_back(xmin, ymax);
  poly.outer().emplace_back(xmax, ymax);
  poly.outer().emplace_back(xmax, ymin);
  poly.outer().emplace_back(xmin, ymin);
  DCHECK(bg::is_valid(poly));
  return WriteWkbPolygon(ctx, poly);
}

// --- Bounding box computation ---

template <bool SWAP>
static bool updateBBox(const uint8_t*& ptr, const uint8_t* end, box2d& bbox) {
  if (UNLIKELY(ptr + 16 > end)) return false;
  double x = ReadDouble(ptr, SWAP);
  double y = ReadDouble(ptr, SWAP);
  bbox.min_corner().x(min(bbox.min_corner().x(), x));
  bbox.min_corner().y(min(bbox.min_corner().y(), y));
  bbox.max_corner().x(max(bbox.max_corner().x(), x));
  bbox.max_corner().y(max(bbox.max_corner().y(), y));
  return true;
}

// Reads the uint32 point count from 'ptr', validates it fits in [ptr, end),
// updates bbox, and advances ptr. Returns false on truncation.
template <bool SWAP>
static bool updateBBoxWithPoints(const uint8_t*& ptr, const uint8_t* end, box2d& bbox) {
  if (UNLIKELY(ptr + 4 > end)) return false;
  uint32_t count = ReadUint32(ptr, SWAP);
  if (UNLIKELY(ptr + (uint64_t)count * 16 > end)) return false;
  for (uint32_t i = 0; i < count; i++) {
    updateBBox<SWAP>(ptr, end, bbox);  // bounds already verified above
  }
  return true;
}

static box2d invalidBox() {
  return box2d(
      point2d(numeric_limits<double>::max(), numeric_limits<double>::max()),
      point2d(numeric_limits<double>::lowest(), numeric_limits<double>::lowest()));
}

static bool skipWKBHeader(const uint8_t*& ptr, const uint8_t* end) {
  if (UNLIKELY(ptr + WKB_HEADER_SIZE > end)) return false;
  ptr += WKB_HEADER_SIZE;
  return true;
}

template <bool SWAP>
static box2d ComputeWkbBBoxInner(const uint8_t* data, int len, OGCType ogc_type) {
  box2d result = invalidBox();

  const uint8_t* ptr = data + WKB_HEADER_SIZE;
  const uint8_t* end = data + len;

  if (ogc_type == ST_POINT) {
    if (UNLIKELY(!updateBBox<SWAP>(ptr, end, result))) return invalidBox();
    return result;
  }

  // All non-point types start with a 4-byte count field.
  if (UNLIKELY(ptr + 4 > end)) return invalidBox();

  switch (ogc_type) {
    case ST_LINESTRING:
      if (UNLIKELY(!updateBBoxWithPoints<SWAP>(ptr, end, result))) return invalidBox();
      break;
    case ST_POLYGON: {
      uint32_t num_rings = ReadUint32(ptr, SWAP);
      for (uint32_t r = 0; r < num_rings; r++) {
        if (UNLIKELY(!updateBBoxWithPoints<SWAP>(ptr, end, result))) return invalidBox();
      }
      break;
    }
    case ST_MULTIPOINT: {
      // Each sub-geometry is a full WKB point (header + coords); no count field.
      uint32_t num_geoms = ReadUint32(ptr, SWAP);
      for (uint32_t i = 0; i < num_geoms; i++) {
        if (UNLIKELY(!skipWKBHeader(ptr, end))) return invalidBox();
        if (UNLIKELY(!updateBBox<SWAP>(ptr, end, result))) return invalidBox();
      }
      break;
    }
    case ST_MULTILINESTRING: {
      uint32_t num_geoms = ReadUint32(ptr, SWAP);
      for (uint32_t i = 0; i < num_geoms; i++) {
        if (UNLIKELY(!skipWKBHeader(ptr, end))) return invalidBox();
        if (UNLIKELY(!updateBBoxWithPoints<SWAP>(ptr, end, result))) return invalidBox();
      }
      break;
    }
    case ST_MULTIPOLYGON: {
      uint32_t num_geoms = ReadUint32(ptr, SWAP);
      for (uint32_t i = 0; i < num_geoms; i++) {
        if (UNLIKELY(!skipWKBHeader(ptr, end))) return invalidBox();
        uint32_t num_rings = ReadUint32(ptr, SWAP);
        for (uint32_t r = 0; r < num_rings; r++) {
          if (UNLIKELY(!updateBBoxWithPoints<SWAP>(ptr, end, result))) {
            return invalidBox();
          }
        }
      }
      break;
    }
    default:
      DCHECK(false) << "ComputeWkbBBox called with unrecognized OGCType: " << ogc_type;
      break;
  }
  return result;
}

box2d ComputeWkbBBox(const uint8_t* data, int len, OGCType ogc_type, bool big_endian) {
  return big_endian
      ? ComputeWkbBBoxInner<true>(data, len, ogc_type)
      : ComputeWkbBBoxInner<false>(data, len, ogc_type);
}

} // namespace impala::geo
