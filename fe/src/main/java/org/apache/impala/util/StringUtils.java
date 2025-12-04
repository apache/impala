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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;


public class StringUtils {

  /**
   * Converts String to utf8 byte array. Assumes that this conversion is successful.
   */
  public static byte[] toUtf8Array(String str) {
    ByteBuffer buf = StandardCharsets.UTF_8.encode(str);
    byte[] arr = new byte[buf.limit()];
    buf.get(arr);
    return arr;
  }

  /**
   * Converts utf8 ByteBuffer to String.
   * Handling decoding errors depends on 'canfail':
   * if true: return null
   * if false: hit precondition
   */
  public static String fromUtf8Buffer(ByteBuffer buf, boolean canFail) {
    try {
      return StandardCharsets.UTF_8.newDecoder().decode(buf).toString();
    } catch (CharacterCodingException ex) {
      Preconditions.checkState(canFail, "UTF8 decoding failed: %s", ex.getMessage());
      return null;
    }
  }

  /**
   * Converts a ByteBuffer to a String using the given charset.
  */
  public static String fromByteBuffer(ByteBuffer buf, Charset charset) {
    byte[] bytes = new byte[buf.remaining()];
    buf.duplicate().get(bytes);
    return new String(bytes, charset);
  }

}
