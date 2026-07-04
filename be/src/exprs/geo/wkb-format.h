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

#include <cstring>

#include "exprs/geo/common.h"

namespace impala::geo {

// WKB (Well Known Binary) format constants and inline parsing functions.
// Supports both little-endian (0x01) and big-endian (0x00) input.
// Output is always little-endian (native x86_64).
//
// WKB layout:
//   Byte 0:    byte order (0x01 = LE, 0x00 = BE)
//   Bytes 1-4: geometry type (uint32, byte order per byte 0)
//   Bytes 5+:  coordinate data (type-dependent)

constexpr int WKB_BYTE_ORDER_OFFSET = 0;
constexpr int WKB_TYPE_OFFSET = 1;
constexpr int WKB_HEADER_SIZE = 5;
constexpr int WKB_POINT_SIZE = 21;  // 5 + 2*8

constexpr uint8_t WKB_LITTLE_ENDIAN = 0x01;
constexpr uint8_t WKB_BIG_ENDIAN = 0x00;

// WKB geometry type codes (ISO 13249).
enum WkbType : uint32_t {
  WKB_POINT = 1,
  WKB_LINESTRING = 2,
  WKB_POLYGON = 3,
  WKB_MULTIPOINT = 4,
  WKB_MULTILINESTRING = 5,
  WKB_MULTIPOLYGON = 6,
};

inline uint32_t ReadUint32(const uint8_t*& ptr, bool big_endian) {
  uint32_t val;
  memcpy(&val, ptr, sizeof(val));
  if (big_endian) val = __builtin_bswap32(val);
  ptr += 4;
  return val;
}

inline double ReadDouble(const uint8_t*& ptr, bool big_endian) {
  uint64_t raw;
  memcpy(&raw, ptr, sizeof(raw));
  if (big_endian) raw = __builtin_bswap64(raw);
  double val;
  memcpy(&val, &raw, sizeof(val));
  ptr += 8;
  return val;
}

inline void WriteUint32(uint8_t*& ptr, uint32_t val) {
  memcpy(ptr, &val, sizeof(val));
  ptr += 4;
}

inline void WriteDouble(uint8_t*& ptr, double val) {
  memcpy(ptr, &val, sizeof(val));
  ptr += 8;
}

inline bool GetByteOrderSwap(const StringVal& geom) {
  return geom.ptr[WKB_BYTE_ORDER_OFFSET] == WKB_BIG_ENDIAN;
}

inline WkbType GetWkbType(const StringVal& geom) {
  const uint8_t* ptr = geom.ptr + WKB_TYPE_OFFSET;
  return static_cast<WkbType>(ReadUint32(ptr, GetByteOrderSwap(geom)));
}

inline OGCType WkbTypeToOgcType(WkbType wkb_type) {
  switch (wkb_type) {
    case WKB_POINT: return ST_POINT;
    case WKB_LINESTRING: return ST_LINESTRING;
    case WKB_POLYGON: return ST_POLYGON;
    case WKB_MULTIPOINT: return ST_MULTIPOINT;
    case WKB_MULTILINESTRING: return ST_MULTILINESTRING;
    case WKB_MULTIPOLYGON: return ST_MULTIPOLYGON;
    default: return UNKNOWN;
  }
}

inline WkbType OgcTypeToWkbType(OGCType ogc_type) {
  switch (ogc_type) {
    case ST_POINT: return WKB_POINT;
    case ST_LINESTRING: return WKB_LINESTRING;
    case ST_POLYGON: return WKB_POLYGON;
    case ST_MULTIPOINT: return WKB_MULTIPOINT;
    case ST_MULTILINESTRING: return WKB_MULTILINESTRING;
    case ST_MULTIPOLYGON: return WKB_MULTIPOLYGON;
    default: return static_cast<WkbType>(0);
  }
}

inline double GetWkbPointX(const StringVal& geom) {
  const uint8_t* ptr = geom.ptr + WKB_HEADER_SIZE;
  return ReadDouble(ptr, GetByteOrderSwap(geom));
}

inline double GetWkbPointY(const StringVal& geom) {
  const uint8_t* ptr = geom.ptr + WKB_HEADER_SIZE + 8;
  return ReadDouble(ptr, GetByteOrderSwap(geom));
}

// Validates WKB geometry and extracts the OGC type from raw bytes.
// Returns false on invalid input. 'big_endian_out' is set to true if byte-swapping
// is needed.
inline bool ParseWkbHeader(const uint8_t* data, int len,
    OGCType* ogc_type, bool* big_endian_out = nullptr) {
  if (data == nullptr || len < WKB_HEADER_SIZE) return false;
  uint8_t byte_order = data[WKB_BYTE_ORDER_OFFSET];
  if (byte_order != WKB_LITTLE_ENDIAN && byte_order != WKB_BIG_ENDIAN) return false;
  bool big_endian = (byte_order == WKB_BIG_ENDIAN);
  if (big_endian_out) *big_endian_out = big_endian;
  const uint8_t* type_ptr = data + WKB_TYPE_OFFSET;
  WkbType wkb_type = static_cast<WkbType>(ReadUint32(type_ptr, big_endian));
  *ogc_type = WkbTypeToOgcType(wkb_type);
  if (*ogc_type == UNKNOWN) return false;
  if (*ogc_type == ST_POINT && len < WKB_POINT_SIZE) return false;
  return true;
}

// Convenience wrapper for the UDF path: validates 'geom' and reports a user-facing
// error via 'ctx' on failure.
inline bool ParseWkbHeader(FunctionContext* ctx, const StringVal& geom,
    OGCType* ogc_type, bool* big_endian_out = nullptr) {
  if (geom.is_null) return false;
  if (!ParseWkbHeader(geom.ptr, geom.len, ogc_type, big_endian_out)) {
    ctx->SetError("Invalid WKB geometry");
    return false;
  }
  return true;
}

// Creates a WKB point (21 bytes: header + x + y).
inline StringVal CreateWkbPoint(FunctionContext* ctx, double x, double y) {
  StringVal result(ctx, WKB_POINT_SIZE);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  uint8_t* ptr = result.ptr;
  *ptr++ = WKB_LITTLE_ENDIAN;
  WriteUint32(ptr, WKB_POINT);
  WriteDouble(ptr, x);
  WriteDouble(ptr, y);
  return result;
}

} // namespace impala::geo
