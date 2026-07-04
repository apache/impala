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

#include "exprs/geo/boost-common.h"
#include "exprs/geo/wkb-format.h"

namespace impala::geo {

// Read WKB bytes into boost geometry types. 'big_endian' indicates big-endian input
// that needs byte-swapping. 'data' points to the start of the WKB (including
// the 5-byte header).
bool ReadWkbPoint(const uint8_t* data, int len, point2d& result, bool big_endian = false);
bool ReadWkbLineString(const uint8_t* data, int len, linestring2d& result,
    bool big_endian = false);
bool ReadWkbPolygon(const uint8_t* data, int len, polygon2d& result,
    bool big_endian = false);
bool ReadWkbMultiPoint(const uint8_t* data, int len, multipoint2d& result,
    bool big_endian = false);
bool ReadWkbMultiLineString(const uint8_t* data, int len, multi_linestring2d& result,
    bool big_endian = false);
bool ReadWkbMultiPolygon(const uint8_t* data, int len, multi_polygon2d& result,
    bool big_endian = false);

// Write boost geometry types to WKB StringVal. All produce LE output.
StringVal WriteWkbPoint(FunctionContext* ctx, const point2d& point);
StringVal WriteWkbLineString(FunctionContext* ctx, const linestring2d& ls);
StringVal WriteWkbPolygon(FunctionContext* ctx, const polygon2d& poly);
StringVal WriteWkbMultiPoint(FunctionContext* ctx, const multipoint2d& mp);
StringVal WriteWkbMultiLineString(FunctionContext* ctx, const multi_linestring2d& mls);
StringVal WriteWkbMultiPolygon(FunctionContext* ctx, const multi_polygon2d& mpoly);
StringVal WriteWkbBox(FunctionContext* ctx, const box2d& box);

// Compute bounding box from WKB without full geometry construction.
// 'data' includes the 5-byte WKB header. Returns an invalid sentinel box
// (IsBboxInvalid() == true) if the geometry is malformed or too short.
// Note: do NOT use bg::is_valid() to check the result — it uses strict <
// and therefore rejects degenerate point bboxes where min == max.
box2d ComputeWkbBBox(const uint8_t* data, int len, OGCType ogc_type,
    bool big_endian = false);

// Returns true if bbox is the sentinel value returned by ComputeWkbBBox on error.
// Uses > (not >=) so degenerate point bboxes (min == max) are accepted as valid.
inline bool IsBboxInvalid(const box2d& bbox) {
  return bbox.min_corner().x() > bbox.max_corner().x()
      || bbox.min_corner().y() > bbox.max_corner().y();
}

} // namespace impala::geo
