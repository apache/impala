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

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCacheJarParams;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

/**
 * Unit tests for JniUtil functions.
 */
public class JniUtilTest {

  private static TBinaryProtocol.Factory protocolFactory_ = new TBinaryProtocol.Factory();

  // Unit test for JniUtil.serializetoThrift().
  @Test
  public void testSerializeToThrift() throws ImpalaException {
    // Serialize and deserialize an simple thrift object.
    TCacheJarParams testObject = new TCacheJarParams("test string");
    byte[] testObjBytes = JniUtil.serializeToThrift(testObject, protocolFactory_);

    TCacheJarParams deserializedTestObj = new TCacheJarParams();
    JniUtil.deserializeThrift(protocolFactory_, deserializedTestObj, testObjBytes);
    assertEquals(deserializedTestObj.hdfs_location, "test string");
  }
}
