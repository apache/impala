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

package org.apache.impala.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.IcebergTable;
import org.junit.Test;

/**
 * Unit tests for the Iceberg-native Parquet Bloom filter helpers in IcebergUtil
 * (IMPALA-12700): optimal size computation, property validation and building the
 * column -> bitset-size map.
 */
public class IcebergBloomFilterUtilTest {
  private static final long MIN_BYTES = FeFsTable.PARQUET_BLOOM_FILTER_MIN_BYTES;
  private static final long MAX_BYTES = FeFsTable.PARQUET_BLOOM_FILTER_MAX_BYTES;
  private static final long ONE_MIB = IcebergTable.DEFAULT_PARQUET_BLOOM_FILTER_MAX_BYTES;

  private static final String ENABLED = IcebergTable.PARQUET_BLOOM_FILTER_ENABLED_PREFIX;
  private static final String MAX_BYTES_PROP =
      IcebergTable.PARQUET_BLOOM_FILTER_MAX_BYTES;
  private static final String NDV = IcebergTable.PARQUET_BLOOM_FILTER_NDV_PREFIX;
  private static final String FPP = IcebergTable.PARQUET_BLOOM_FILTER_FPP_PREFIX;

  // A Bloom filter column resolver that never has NDV statistics.
  private static final Function<String, Long> NO_STATS = col -> null;

  private static Map<String, Long> build(Map<String, String> props) {
    return build(props, NO_STATS);
  }

  private static Map<String, Long> build(
      Map<String, String> props, Function<String, Long> ndvStats) {
    StringBuilder errMsg = new StringBuilder();
    Map<String, Long> result =
        IcebergUtil.getIcebergParquetBloomFilterColumns(props, ndvStats, errMsg);
    assertTrue("Unexpected error: " + errMsg, result != null);
    return result;
  }

  private static void assertInvalid(Map<String, String> props) {
    StringBuilder errMsg = new StringBuilder();
    assertNull(IcebergUtil.getIcebergParquetBloomFilterColumns(props, NO_STATS, errMsg));
    assertTrue("Expected a non-empty error message", errMsg.length() > 0);
    assertFalse(IcebergUtil.validateParquetBloomFilterProperties(props,
        new StringBuilder()));
  }

  @Test
  public void testOptimalByteSize() {
    // Reference values match tests/query_test/test_parquet_bloom_filter.py
    // (_optimal_bitset_size) and parquet-java's BlockSplitBloomFilter sizing.
    assertEquals(2097152L, IcebergUtil.optimalParquetBloomFilterByteSize(1000000, 0.01));
    assertEquals(1048576L, IcebergUtil.optimalParquetBloomFilterByteSize(1000000, 0.05));
    assertEquals(131072L, IcebergUtil.optimalParquetBloomFilterByteSize(100000, 0.01));
    assertEquals(2048L, IcebergUtil.optimalParquetBloomFilterByteSize(1000, 0.01));
    assertEquals(67108864L,
        IcebergUtil.optimalParquetBloomFilterByteSize(50000000, 0.01));
    // Tiny NDV is clamped up to MIN_BYTES.
    assertEquals(MIN_BYTES, IcebergUtil.optimalParquetBloomFilterByteSize(10, 0.01));
    assertEquals(MIN_BYTES, IcebergUtil.optimalParquetBloomFilterByteSize(1, 0.01));
  }

  @Test
  public void testEnabledColumnDefaultsToMaxBytes() {
    // Enabled column with no NDV and no stats -> default max-bytes (1 MiB).
    Map<String, Long> res = build(ImmutableMap.of(ENABLED + "a", "true"));
    assertEquals(ImmutableMap.of("a", ONE_MIB), res);
  }

  @Test
  public void testDisabledColumnExcluded() {
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "false",
        ENABLED + "b", "true"));
    assertEquals(ImmutableMap.of("b", ONE_MIB), res);
  }

  @Test
  public void testCustomMaxBytes() {
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        MAX_BYTES_PROP, "131072"));
    assertEquals(ImmutableMap.of("a", 131072L), res);
  }

  @Test
  public void testMaxBytesRoundedUpToPowerOfTwo() {
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        MAX_BYTES_PROP, "1000"));
    // 1000 is rounded up to the next power of two.
    assertEquals(ImmutableMap.of("a", 1024L), res);
  }

  @Test
  public void testMaxBytesClampedToMin() {
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        MAX_BYTES_PROP, "10"));
    assertEquals(ImmutableMap.of("a", MIN_BYTES), res);
  }

  @Test
  public void testMaxBytesClampedToMax() {
    // A positive int larger than MAX_BYTES is clamped down to MAX_BYTES.
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        MAX_BYTES_PROP, "999999999"));
    assertEquals(ImmutableMap.of("a", MAX_BYTES), res);
  }

  @Test
  public void testMaxBytesClampedToMaxAtBoundary() {
    // Any in-range value at or above Impala's 128 MiB limit clamps to MAX_BYTES, so the
    // size handed to the BE never exceeds ParquetBloomFilter::MAX_BYTES (enforced by the
    // DCHECK_LE in hdfs-parquet-table-writer.cc). IMPALA-12700.
    for (long bytes : new long[] {MAX_BYTES, MAX_BYTES + 1, Integer.MAX_VALUE}) {
      Map<String, Long> res = build(ImmutableMap.of(
          ENABLED + "a", "true", MAX_BYTES_PROP, Long.toString(bytes)));
      assertEquals("max-bytes " + bytes + " should clamp to MAX_BYTES",
          ImmutableMap.of("a", MAX_BYTES), res);
    }
  }

  @Test
  public void testMaxBytesExceedingIntRangeRejected() {
    // 'write.parquet.bloom-filter-max-bytes' is an int property (matching Iceberg), so a
    // value that does not fit a signed 32-bit int is rejected rather than wrapping.
    assertInvalid(ImmutableMap.of(
        ENABLED + "a", "true", MAX_BYTES_PROP, Long.toString(Integer.MAX_VALUE + 1L)));
    assertInvalid(ImmutableMap.of(
        ENABLED + "a", "true", MAX_BYTES_PROP, "3000000000"));
  }

  @Test
  public void testExplicitNdvSizesFilter() {
    // maxBytes is large enough not to cap the optimal size.
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        NDV + "a", "100000",
        MAX_BYTES_PROP, "8388608"));
    assertEquals(ImmutableMap.of("a", 131072L), res);
  }

  @Test
  public void testNdvCappedByMaxBytes() {
    // optimal(1000000, 0.01) = 2 MiB, capped to the 1 MiB default max-bytes.
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        NDV + "a", "1000000"));
    assertEquals(ImmutableMap.of("a", ONE_MIB), res);
  }

  @Test
  public void testCustomFpp() {
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        NDV + "a", "1000000",
        FPP + "a", "0.05",
        MAX_BYTES_PROP, "8388608"));
    // optimal(1000000, 0.05) = 1 MiB.
    assertEquals(ImmutableMap.of("a", ONE_MIB), res);
  }

  @Test
  public void testNdvFromStatsFallback() {
    Function<String, Long> stats = col -> "a".equals(col) ? 100000L : null;
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        MAX_BYTES_PROP, "8388608"), stats);
    // NDV comes from stats -> optimal(100000, 0.01) = 128 KiB.
    assertEquals(ImmutableMap.of("a", 131072L), res);
  }

  @Test
  public void testExplicitNdvOverridesStats() {
    Function<String, Long> stats = col -> 100000L;
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "a", "true",
        NDV + "a", "1000",
        MAX_BYTES_PROP, "8388608"), stats);
    // Explicit NDV (1000) wins over the stats NDV (100000) -> optimal(1000, 0.01).
    assertEquals(ImmutableMap.of("a", 2048L), res);
  }

  @Test
  public void testNonPositiveStatsNdvIgnored() {
    // Unknown stats NDV (-1) is treated as "no NDV" -> default max-bytes.
    Function<String, Long> stats = col -> -1L;
    Map<String, Long> res = build(ImmutableMap.of(ENABLED + "a", "true"), stats);
    assertEquals(ImmutableMap.of("a", ONE_MIB), res);
  }

  @Test
  public void testColumnNameLowercased() {
    // The enabled column name is lowercased to match the BE column writer; the ndv
    // property is matched case-sensitively against the enabled column name.
    Map<String, Long> res = build(ImmutableMap.of(
        ENABLED + "MyCol", "true",
        NDV + "MyCol", "1000",
        MAX_BYTES_PROP, "8388608"));
    assertEquals(ImmutableMap.of("mycol", 2048L), res);
  }

  @Test
  public void testEmptyWhenNothingEnabled() {
    assertTrue(build(ImmutableMap.of(MAX_BYTES_PROP, "131072")).isEmpty());
    assertTrue(build(new HashMap<>()).isEmpty());
  }

  @Test
  public void testInvalidMaxBytes() {
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", MAX_BYTES_PROP, "abc"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", MAX_BYTES_PROP, "-5"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", MAX_BYTES_PROP, "0"));
  }

  @Test
  public void testInvalidNdv() {
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", NDV + "a", "-1"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", NDV + "a", "0"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", NDV + "a", "abc"));
  }

  @Test
  public void testInvalidFpp() {
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", FPP + "a", "1.5"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", FPP + "a", "0"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", FPP + "a", "1.0"));
    assertInvalid(ImmutableMap.of(ENABLED + "a", "true", FPP + "a", "abc"));
  }

  @Test
  public void testHasParquetBloomFilterProperties() {
    assertTrue(IcebergUtil.hasParquetBloomFilterProperties(
        ImmutableMap.of(ENABLED + "a", "true")));
    assertTrue(IcebergUtil.hasParquetBloomFilterProperties(
        ImmutableMap.of(MAX_BYTES_PROP, "131072")));
    assertTrue(IcebergUtil.hasParquetBloomFilterProperties(
        ImmutableMap.of(NDV + "a", "10")));
    assertTrue(IcebergUtil.hasParquetBloomFilterProperties(
        ImmutableMap.of(FPP + "a", "0.01")));
    assertFalse(IcebergUtil.hasParquetBloomFilterProperties(
        ImmutableMap.of("write.parquet.compression-codec", "zstd")));
    assertFalse(IcebergUtil.hasParquetBloomFilterProperties(new HashMap<>()));
  }
}
