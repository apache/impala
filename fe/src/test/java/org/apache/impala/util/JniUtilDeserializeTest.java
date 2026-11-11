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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCacheJarParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Test;

/** Regression tests for the in-memory JNI message-size boundary. */
public class JniUtilDeserializeTest {
  private static final TProtocolFactory[] FACTORIES = {
      new TBinaryProtocol.Factory(), new TCompactProtocol.Factory()};

  @Test
  public void testLargeMessage() throws ImpalaException {
    // Many individually small fields reproduce a statistics-update request without
    // allocating one enormous source string. Both wire encodings exceed 100 MiB.
    String value = String.join("", Collections.nCopies(8192, "x"));
    TGetTablesResult original = new TGetTablesResult(Collections.nCopies(12801, value));
    for (TProtocolFactory factory : FACTORIES) {
      byte[] bytes = JniUtil.serializeToThrift(original, factory);
      assertTrue(bytes.length > TConfiguration.DEFAULT_MAX_MESSAGE_SIZE);
      TGetTablesResult decoded = new TGetTablesResult();
      JniUtil.deserializeThrift(factory, decoded, bytes);
      assertEquals(original, decoded);
    }
  }

  @Test
  public void testSmallAndEmptyValues() throws ImpalaException {
    for (TProtocolFactory factory : FACTORIES) {
      for (String value : new String[] {"", "small"}) {
        TCacheJarParams original = new TCacheJarParams(value);
        byte[] bytes = JniUtil.serializeToThrift(original, factory);
        TCacheJarParams decoded = new TCacheJarParams();
        JniUtil.deserializeThrift(factory, decoded, bytes);
        assertEquals(original, decoded);
      }
    }
    TCacheJarParams original = new TCacheJarParams("default factory");
    byte[] bytes = JniUtil.serializeToThrift(original, new TBinaryProtocol.Factory());
    TCacheJarParams decoded = new TCacheJarParams();
    JniUtil.deserializeThrift(decoded, bytes);
    assertEquals(original, decoded);
  }

  @Test
  public void testTruncatedMessages() throws ImpalaException {
    for (TProtocolFactory factory : FACTORIES) {
      byte[] bytes = JniUtil.serializeToThrift(new TCacheJarParams("value"), factory);
      // End inside the field, rather than merely removing the struct stop marker.
      for (int length : new int[] {0, 1, bytes.length / 2}) {
        assertInvalid(factory, Arrays.copyOf(bytes, length));
      }
    }
  }

  @Test
  public void testInvalidBinaryLengths() {
    // A string length must not exceed the supplied input or be negative.
    assertInvalid(new TBinaryProtocol.Factory(),
        new byte[] {11, 0, 1, 0, 0, 4, 0, 0});
    assertInvalid(new TBinaryProtocol.Factory(),
        new byte[] {11, 0, 1, -1, -1, -1, -1, 0});
  }

  private static void assertInvalid(TProtocolFactory factory, byte[] bytes) {
    try {
      JniUtil.deserializeThrift(factory, new TCacheJarParams(), bytes);
      fail("Malformed input was accepted");
    } catch (ImpalaException expected) {
      // Keep malformed input on the existing checked-exception path.
    }
  }
}
