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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.impala.thrift.THash128;
import org.junit.Test;

/**
 * Unit tests for the Hash128 class which stores 128-bit hash values
 * as two longs for memory efficiency.
 */
public class Hash128Test {

  @Test
  public void testBasicConstruction() {
    Hash128 hash = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    assertEquals(0x1234567890ABCDEFL, hash.getHigh());
    assertEquals(0xFEDCBA0987654321L, hash.getLow());
  }

  @Test
  public void testEquality() {
    Hash128 hash1 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 hash2 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 hash3 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654320L);
    Hash128 hash4 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);

    // Test equals
    assertEquals(hash1, hash2);
    assertEquals(hash1, hash4);
    assertNotEquals(hash1, hash3);
    assertNotEquals(hash1, null);
    assertNotEquals(hash1, "not a hash");

    // Test hashCode consistency
    assertEquals(hash1.hashCode(), hash2.hashCode());
    assertEquals(hash1.hashCode(), hash4.hashCode());
  }

  @Test
  public void testHashMapUsage() {
    // Hash128 should work correctly as a HashMap key
    Map<Hash128, String> map = new HashMap<>();
    Hash128 key1 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 key2 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 key3 = new Hash128(0xAAAAAAAAAAAAAAAAL, 0xBBBBBBBBBBBBBBBBL);

    map.put(key1, "value1");
    map.put(key3, "value3");

    // key2 should retrieve the same value as key1 since they're equal
    assertEquals("value1", map.get(key2));
    assertEquals("value3", map.get(key3));
    assertNull(map.get(new Hash128(0x0L, 0x0L)));
  }

  @Test
  public void testToString() {
    Hash128 hash = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    String str = hash.toString();

    // Should be 32 hex characters (16 for each long)
    assertEquals(32, str.length());
    assertEquals("1234567890abcdeffedcba0987654321", str);

    // Test with zero values
    Hash128 zeroHash = new Hash128(0x0L, 0x0L);
    assertEquals("00000000000000000000000000000000", zeroHash.toString());

    // Test with max values
    Hash128 maxHash = new Hash128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL);
    assertEquals("ffffffffffffffffffffffffffffffff", maxHash.toString());
  }

  @Test
  public void testThriftRoundTrip() {
    Hash128 original = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);

    // Convert to Thrift and back
    THash128 thrift = original.toThrift();
    Hash128 restored = Hash128.fromThrift(thrift);

    assertEquals(original, restored);
    assertEquals(original.getHigh(), restored.getHigh());
    assertEquals(original.getLow(), restored.getLow());
  }

  @Test
  public void testThriftStructFields() {
    Hash128 hash = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    THash128 thrift = hash.toThrift();

    // Verify Thrift struct has correct values
    assertEquals(0x1234567890ABCDEFL, thrift.getHigh());
    assertEquals(0xFEDCBA0987654321L, thrift.getLow());
  }

  @Test
  public void testThriftWithEdgeCases() {
    // Test with zero
    Hash128 zero = new Hash128(0x0L, 0x0L);
    THash128 thriftZero = zero.toThrift();
    Hash128 restoredZero = Hash128.fromThrift(thriftZero);
    assertEquals(zero, restoredZero);

    // Test with all ones
    Hash128 ones = new Hash128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL);
    THash128 thriftOnes = ones.toThrift();
    Hash128 restoredOnes = Hash128.fromThrift(thriftOnes);
    assertEquals(ones, restoredOnes);

    // Test with high bit patterns
    Hash128 pattern = new Hash128(0x8000000000000000L, 0x8000000000000001L);
    THash128 thriftPattern = pattern.toThrift();
    Hash128 restoredPattern = Hash128.fromThrift(thriftPattern);
    assertEquals(pattern, restoredPattern);
  }

  @Test(expected = NullPointerException.class)
  public void testNullThriftInput() {
    Hash128.fromThrift(null);
  }

  @Test
  public void testConsistentHashing() {
    // Multiple Hash128 objects with same values should have consistent hashCode
    Hash128 hash1 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 hash2 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);

    // Create and recreate through serialization
    Hash128 hash3 = Hash128.fromThrift(hash1.toThrift());

    assertEquals(hash1.hashCode(), hash2.hashCode());
    assertEquals(hash1.hashCode(), hash3.hashCode());
  }

  @Test
  public void testDifferentHashesDifferentHashCodes() {
    // Different Hash128 objects should (likely) have different hashCodes
    Hash128 hash1 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    Hash128 hash2 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654320L);
    Hash128 hash3 = new Hash128(0x1234567890ABCDEEL, 0xFEDCBA0987654321L);

    // Note: hashCode collision is possible but unlikely
    assertNotEquals(hash1.hashCode(), hash2.hashCode());
    assertNotEquals(hash1.hashCode(), hash3.hashCode());
  }

  @Test
  public void testThriftAsMapKey() {
    // Verify that Hash128 can be used as a map key through Thrift round-trip
    Map<Hash128, String> map = new HashMap<>();
    Hash128 key1 = new Hash128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    map.put(key1, "value1");

    // Serialize and deserialize
    THash128 thriftKey = key1.toThrift();
    Hash128 key2 = Hash128.fromThrift(thriftKey);

    // Should retrieve the same value
    assertEquals("value1", map.get(key2));
  }
}
