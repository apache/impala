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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import org.apache.impala.common.ImpalaException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for JMXJsonUtil functionality.
 */
public class JMXJsonUtilTest {

  // Validates the JSON string returned by JMXJsonUtil.getJMXJson()
  @Test
  public void testJMXMetrics() throws ImpalaException {
    String jmxJson = JMXJsonUtil.getJMXJson();
    JsonNode rootNode = null;
    // Validate the JSON.
    try {
      rootNode = new ObjectMapper().readTree(jmxJson);
    } catch (IOException e) {
      fail("Invalid JSON returned by getMxJson(): " + jmxJson);
    }
    Preconditions.checkNotNull(rootNode);
    assertTrue("Invalid JSON: "  + jmxJson, rootNode.hasNonNull("beans"));
    List<String> values = rootNode.get("beans").findValuesAsText("name");
    assertTrue("Invalid JSON: "  + jmxJson,
        values.contains("java.lang:type=MemoryPool,name=Metaspace") ||
        values.contains("java.lang:name=Metaspace,type=MemoryPool"));
    assertTrue("Invalid JSON: "  + jmxJson, values.contains("java.lang:type=Runtime"));
  }
}
