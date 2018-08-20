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

import com.google.common.base.Strings;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCacheJarParams;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

public class CompressionUtilTest {
  private static TBinaryProtocol.Factory protocolFactory_ = new TBinaryProtocol.Factory();

  @Test
  public void testCompressionUtil() throws Exception {
    // Compress and decompress simple strings.
    String[] stringsToTest ={"", "TestString", Strings.repeat("x", 100 * 1024 * 1024)};
    for (String test: stringsToTest) {
      byte[] compressed = CompressionUtil.deflateCompress(test.getBytes());
      byte[] decompressed = CompressionUtil.deflateDecompress(compressed);
      Assert.assertEquals(new String(decompressed), test);
    }

    // Compress and decompress a thrift struct.
    TCacheJarParams testObject = new TCacheJarParams("test string");
    byte[] testObjBytes = JniUtil.serializeToThrift(testObject, protocolFactory_);

    byte[] compressed = CompressionUtil.deflateCompress(testObjBytes);
    byte[] decompressed = CompressionUtil.deflateDecompress(compressed);

    TCacheJarParams deserializedTestObj = new TCacheJarParams();
    JniUtil.deserializeThrift(protocolFactory_, deserializedTestObj, decompressed);
    Assert.assertEquals(deserializedTestObj.hdfs_location, "test string");
  }

}
