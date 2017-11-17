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

import org.apache.impala.thrift.TUniqueId;
import org.junit.Test;

import static org.apache.impala.util.TUniqueIdUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TUniqueIdUtilTest {
  @Test
  public void testUniqueId() {
    TUniqueId unique_id = new TUniqueId();
    unique_id.hi = 0xfeedbeeff00d7777L;
    unique_id.lo = 0x2020202020202020L;
    String str = "feedbeeff00d7777:2020202020202020";
    assertEquals(str, PrintId(unique_id));
    unique_id.lo = 0x20L;
    assertEquals("feedbeeff00d7777:20", PrintId(unique_id));
  }

  @Test
  public void QueryIdParsing() {
    try {
      ParseId("abcd");
      fail();
    } catch (NumberFormatException e) {}
    try {
      ParseId("abcdabcdabcdabcdabcdabcdabcdabcda");
      fail();
    } catch (NumberFormatException e) {}
    try {
      ParseId("zbcdabcdabcdabcd:abcdabcdabcdabcd");
      fail();
    } catch (NumberFormatException e) {}
    try {
      ParseId("~bcdabcdabcdabcd:abcdabcdabcdabcd");
      fail();
    } catch (NumberFormatException e) {}
    try {
      ParseId("abcdabcdabcdabcd:!bcdabcdabcdabcd");
      fail();
    } catch (NumberFormatException e) {}

    TUniqueId id = ParseId("abcdabcdabcdabcd:abcdabcdabcdabcd");
    assertEquals(id.hi, 0xabcdabcdabcdabcdL);
    assertEquals(id.lo, 0xabcdabcdabcdabcdL);

    id = ParseId("abcdabcdabcdabcd:1234abcdabcd5678");
    assertEquals(id.hi, 0xabcdabcdabcdabcdL);
    assertEquals(id.lo, 0x1234abcdabcd5678L);

    id = ParseId("cdabcdabcdabcd:1234abcdabcd5678");
    assertEquals(id.hi, 0xcdabcdabcdabcdL);
    assertEquals(id.lo, 0x1234abcdabcd5678L);

    id = ParseId("cdabcdabcdabcd:abcdabcd5678");
    assertEquals(id.hi, 0xcdabcdabcdabcdL);
    assertEquals(id.lo, 0xabcdabcd5678L);
  }
}
