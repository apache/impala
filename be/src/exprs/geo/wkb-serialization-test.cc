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

#include <cstring>
#include <memory>
#include <vector>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "udf/udf-test-harness.h"

namespace impala::geo {

// Helper to build a minimal WKB buffer with a given type and body.
static std::vector<uint8_t> MakeWkb(uint32_t type, const std::vector<uint8_t>& body) {
  std::vector<uint8_t> buf(WKB_HEADER_SIZE + body.size());
  buf[0] = 0x01;  // LE
  memcpy(&buf[1], &type, 4);
  if (!body.empty()) memcpy(&buf[WKB_HEADER_SIZE], body.data(), body.size());
  return buf;
}

// Helper to encode a uint32 in LE.
static std::vector<uint8_t> U32(uint32_t val) {
  std::vector<uint8_t> v(4);
  memcpy(v.data(), &val, 4);
  return v;
}

// Concatenate byte vectors.
static std::vector<uint8_t> Cat(std::initializer_list<std::vector<uint8_t>> parts) {
  std::vector<uint8_t> result;
  for (const auto& p : parts) result.insert(result.end(), p.begin(), p.end());
  return result;
}

class WkbMalformedTest : public testing::Test {};

#define EXPECT_INVALID(geom) EXPECT_TRUE(IsBboxInvalid(geom))
// Note: bg::is_valid for box uses strict < (min_corner < max_corner), so it
// rejects degenerate point bboxes where min == max. IsBboxInvalid uses > instead.
#define EXPECT_BBOX(bbox, xmin, ymin, xmax, ymax) \
  do { \
    EXPECT_FALSE(IsBboxInvalid(bbox)); \
    EXPECT_EQ((bbox).min_corner().x(), xmin); \
    EXPECT_EQ((bbox).min_corner().y(), ymin); \
    EXPECT_EQ((bbox).max_corner().x(), xmax); \
    EXPECT_EQ((bbox).max_corner().y(), ymax); \
  } while (false)

// --- ReadWkbPoint ---

TEST_F(WkbMalformedTest, PointTooShort) {
  // Valid header but truncated coordinate data.
  auto buf = MakeWkb(WKB_POINT, {});  // 5 bytes, need 21
  point2d pt;
  EXPECT_FALSE(ReadWkbPoint(buf.data(), buf.size(), pt));
}

// --- ReadWkbLineString ---

TEST_F(WkbMalformedTest, LineStringTruncatedCount) {
  // Header only, no num_points field.
  auto buf = MakeWkb(WKB_LINESTRING, {});
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
}

TEST_F(WkbMalformedTest, LineStringInflatedCount) {
  // num_points says 1000 but buffer only has the count field.
  auto buf = MakeWkb(WKB_LINESTRING, U32(1000));
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
}

TEST_F(WkbMalformedTest, LineStringOverflowCount) {
  // num_points = 0x10000000 — would overflow uint32 * 16 to zero.
  auto buf = MakeWkb(WKB_LINESTRING, U32(0x10000000));
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
}

// --- ReadWkbPolygon ---

TEST_F(WkbMalformedTest, PolygonTruncatedRingCount) {
  auto buf = MakeWkb(WKB_POLYGON, {0x01, 0x02});  // partial uint32
  polygon2d poly;
  EXPECT_FALSE(ReadWkbPolygon(buf.data(), buf.size(), poly));
}

TEST_F(WkbMalformedTest, PolygonInflatedPointCount) {
  // 1 ring, num_points = 9999 but no actual point data.
  auto buf = MakeWkb(WKB_POLYGON, Cat({U32(1), U32(9999)}));
  polygon2d poly;
  EXPECT_FALSE(ReadWkbPolygon(buf.data(), buf.size(), poly));
}

TEST_F(WkbMalformedTest, PolygonOverflowPointCount) {
  // 1 ring, num_points = 0x10000001 — overflows * 16.
  auto buf = MakeWkb(WKB_POLYGON, Cat({U32(1), U32(0x10000001)}));
  polygon2d poly;
  EXPECT_FALSE(ReadWkbPolygon(buf.data(), buf.size(), poly));
}

// --- ReadWkbMultiPoint ---

TEST_F(WkbMalformedTest, MultiPointInflatedCount) {
  // Claims 1000 sub-geometries but buffer is tiny.
  auto buf = MakeWkb(WKB_MULTIPOINT, U32(1000));
  multipoint2d mp;
  EXPECT_FALSE(ReadWkbMultiPoint(buf.data(), buf.size(), mp));
}

TEST_F(WkbMalformedTest, MultiPointOverflowCount) {
  // num_geoms = 0xFFFFFFFF — upfront check should reject before allocating.
  auto buf = MakeWkb(WKB_MULTIPOINT, U32(0xFFFFFFFF));
  multipoint2d mp;
  EXPECT_FALSE(ReadWkbMultiPoint(buf.data(), buf.size(), mp));
}

// --- ReadWkbMultiLineString ---

TEST_F(WkbMalformedTest, MultiLineStringInflatedCount) {
  auto buf = MakeWkb(WKB_MULTILINESTRING, U32(5000));
  multi_linestring2d mls;
  EXPECT_FALSE(ReadWkbMultiLineString(buf.data(), buf.size(), mls));
}

TEST_F(WkbMalformedTest, MultiLineStringOverflowCount) {
  auto buf = MakeWkb(WKB_MULTILINESTRING, U32(0xFFFFFFFF));
  multi_linestring2d mls;
  EXPECT_FALSE(ReadWkbMultiLineString(buf.data(), buf.size(), mls));
}

// --- ReadWkbMultiPolygon ---

TEST_F(WkbMalformedTest, MultiPolygonInflatedCount) {
  auto buf = MakeWkb(WKB_MULTIPOLYGON, U32(5000));
  multi_polygon2d mp;
  EXPECT_FALSE(ReadWkbMultiPolygon(buf.data(), buf.size(), mp));
}

TEST_F(WkbMalformedTest, MultiPolygonOverflowCount) {
  auto buf = MakeWkb(WKB_MULTIPOLYGON, U32(0xFFFFFFFF));
  multi_polygon2d mp;
  EXPECT_FALSE(ReadWkbMultiPolygon(buf.data(), buf.size(), mp));
}

// --- ComputeWkbBBox ---

TEST_F(WkbMalformedTest, BBoxPointTooShort) {
  auto buf = MakeWkb(WKB_POINT, {});  // need 16 bytes of coords, have 0
  point2d pt;
  EXPECT_FALSE(ReadWkbPoint(buf.data(), buf.size(), pt));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_POINT));
}

TEST_F(WkbMalformedTest, BBoxLineStringTruncated) {
  // Header + num_points=100 but no point data.
  auto buf = MakeWkb(WKB_LINESTRING, U32(100));
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_LINESTRING));
}

TEST_F(WkbMalformedTest, BBoxLineStringOverflow) {
  // num_points = 0x10000000 — overflow if using 32-bit multiply.
  auto buf = MakeWkb(WKB_LINESTRING, U32(0x10000000));
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_LINESTRING));
}

TEST_F(WkbMalformedTest, BBoxPolygonTruncated) {
  // 1 ring, num_points=500 but no data.
  auto buf = MakeWkb(WKB_POLYGON, Cat({U32(1), U32(500)}));
  polygon2d poly;
  EXPECT_FALSE(ReadWkbPolygon(buf.data(), buf.size(), poly));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_POLYGON));
}

TEST_F(WkbMalformedTest, BBoxMultiPointTruncated) {
  // Claims 10 sub-geometries, buffer too short.
  auto buf = MakeWkb(WKB_MULTIPOINT, U32(10));
  multipoint2d mp;
  EXPECT_FALSE(ReadWkbMultiPoint(buf.data(), buf.size(), mp));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_MULTIPOINT));
}

TEST_F(WkbMalformedTest, BBoxMultiLineStringTruncated) {
  auto buf = MakeWkb(WKB_MULTILINESTRING, U32(5));
  multi_linestring2d mls;
  EXPECT_FALSE(ReadWkbMultiLineString(buf.data(), buf.size(), mls));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_MULTILINESTRING));
}

TEST_F(WkbMalformedTest, BBoxMultiPolygonTruncated) {
  auto buf = MakeWkb(WKB_MULTIPOLYGON, U32(5));
  multi_polygon2d mp;
  EXPECT_FALSE(ReadWkbMultiPolygon(buf.data(), buf.size(), mp));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_MULTIPOLYGON));
}

TEST_F(WkbMalformedTest, BBoxNoCountField) {
  // Just the 5-byte header, no count field at all.
  auto buf = MakeWkb(WKB_LINESTRING, {});
  linestring2d ls;
  EXPECT_FALSE(ReadWkbLineString(buf.data(), buf.size(), ls));
  EXPECT_INVALID(ComputeWkbBBox(buf.data(), buf.size(), ST_LINESTRING));
}

// --- OgcTypeToWkbType ---

TEST_F(WkbMalformedTest, OgcTypeToWkbType) {
  EXPECT_EQ(OgcTypeToWkbType(ST_POINT),           WKB_POINT);
  EXPECT_EQ(OgcTypeToWkbType(ST_LINESTRING),      WKB_LINESTRING);
  EXPECT_EQ(OgcTypeToWkbType(ST_POLYGON),         WKB_POLYGON);
  EXPECT_EQ(OgcTypeToWkbType(ST_MULTIPOINT),      WKB_MULTIPOINT);
  EXPECT_EQ(OgcTypeToWkbType(ST_MULTILINESTRING), WKB_MULTILINESTRING);
  EXPECT_EQ(OgcTypeToWkbType(ST_MULTIPOLYGON),    WKB_MULTIPOLYGON);
}

// --- Write -> Read round trips (one per geometry type) ---
class WkbRoundTripTest : public testing::Test {
 protected:
  void SetUp() override {
    FunctionContext::TypeDesc no_type;
    ctx_.reset(impala_udf::UdfTestHarness::CreateTestContext(no_type, {}, nullptr,
        &mem_pool_));
  }
  void TearDown() override {
    impala_udf::UdfTestHarness::CloseContext(ctx_.get());
    mem_pool_.FreeAll();
  }
  FunctionContext* ctx() { return ctx_.get(); }

 private:
  MemTracker mem_tracker_;
  MemPool mem_pool_{&mem_tracker_};
  std::unique_ptr<FunctionContext> ctx_;
};

TEST_F(WkbRoundTripTest, Point) {
  // Value-initialize the coordinates: point_xy's default ctor leaves them
  // uninitialized, which the static analyzer flags as an undefined read in the
  // bg::equals() call below (it can't see that read_wkt/ReadWkbPoint fill them).
  point2d in(0, 0), out(0, 0);
  bg::read_wkt("POINT(1 2)", in);
  StringVal wkb = WriteWkbPoint(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  EXPECT_EQ(wkb.len, WKB_POINT_SIZE);
  ASSERT_TRUE(ReadWkbPoint(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_POINT);
  EXPECT_BBOX(bbox, 1.0, 2.0, 1.0, 2.0);
}

TEST_F(WkbRoundTripTest, LineString) {
  linestring2d in, out;
  bg::read_wkt("LINESTRING(0 0, 1 2, 3 4)", in);
  StringVal wkb = WriteWkbLineString(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  ASSERT_TRUE(ReadWkbLineString(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_LINESTRING);
  EXPECT_BBOX(bbox, 0.0, 0.0, 3.0, 4.0);
}

TEST_F(WkbRoundTripTest, Polygon) {
  // A closed 4x4 square with a triangular hole, to exercise inner rings.
  polygon2d in, out;
  bg::read_wkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 1 2, 1 1))", in);
  bg::correct(in);
  StringVal wkb = WriteWkbPolygon(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  ASSERT_TRUE(ReadWkbPolygon(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_POLYGON);
  EXPECT_BBOX(bbox, 0.0, 0.0, 4.0, 4.0);
}

TEST_F(WkbRoundTripTest, MultiPoint) {
  multipoint2d in, out;
  bg::read_wkt("MULTIPOINT((1 1), (2 3))", in);
  StringVal wkb = WriteWkbMultiPoint(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  ASSERT_TRUE(ReadWkbMultiPoint(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_MULTIPOINT);
  EXPECT_BBOX(bbox, 1.0, 1.0, 2.0, 3.0);
}

TEST_F(WkbRoundTripTest, MultiLineString) {
  multi_linestring2d in, out;
  bg::read_wkt("MULTILINESTRING((0 0, 1 1), (2 2, 3 4))", in);
  StringVal wkb = WriteWkbMultiLineString(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  ASSERT_TRUE(ReadWkbMultiLineString(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_MULTILINESTRING);
  EXPECT_BBOX(bbox, 0.0, 0.0, 3.0, 4.0);
}

TEST_F(WkbRoundTripTest, MultiPolygon) {
  multi_polygon2d in, out;
  bg::read_wkt(
      "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))", in);
  bg::correct(in);
  StringVal wkb = WriteWkbMultiPolygon(ctx(), in);
  ASSERT_FALSE(wkb.is_null);
  ASSERT_TRUE(ReadWkbMultiPolygon(wkb.ptr, wkb.len, out));
  EXPECT_TRUE(bg::equals(in, out));

  box2d bbox = ComputeWkbBBox(wkb.ptr, wkb.len, ST_MULTIPOLYGON);
  EXPECT_BBOX(bbox, 0.0, 0.0, 6.0, 6.0);
}

// --- StringVal-based format getters and CreateWkbPoint ---

TEST_F(WkbRoundTripTest, WkbFormatGetters) {
  StringVal wkb = CreateWkbPoint(ctx(), 1.0, 2.0);
  ASSERT_FALSE(wkb.is_null);
  EXPECT_EQ(wkb.len, WKB_POINT_SIZE);
  EXPECT_FALSE(GetByteOrderSwap(wkb));
  EXPECT_EQ(GetWkbType(wkb), WKB_POINT);
  EXPECT_EQ(GetWkbPointX(wkb), 1.0);
  EXPECT_EQ(GetWkbPointY(wkb), 2.0);

  OGCType ogc_type;
  bool big_endian;
  EXPECT_TRUE(ParseWkbHeader(ctx(), wkb, &ogc_type, &big_endian));
  EXPECT_EQ(ogc_type, ST_POINT);
  EXPECT_FALSE(big_endian);
}

// --- WriteWkbBox ---

TEST_F(WkbRoundTripTest, WriteWkbBox) {
  box2d box(point2d(1.0, 2.0), point2d(3.0, 4.0));
  StringVal wkb = WriteWkbBox(ctx(), box);
  ASSERT_FALSE(wkb.is_null);
  polygon2d poly;
  ASSERT_TRUE(ReadWkbPolygon(wkb.ptr, wkb.len, poly));
  EXPECT_EQ(poly.outer().size(), 5u);  // closed 5-point rectangle
  EXPECT_TRUE(bg::is_valid(poly));     // outer ring must be CW (polygon2d convention)
  box2d result = ComputeWkbBBox(wkb.ptr, wkb.len, ST_POLYGON);
  EXPECT_BBOX(result, 1.0, 2.0, 3.0, 4.0);
}

} // namespace impala::geo
