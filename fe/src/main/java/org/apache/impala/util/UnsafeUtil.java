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

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
/**
 * Utility class to access unsafe methods.
 */
public class UnsafeUtil {
  // object to allow us to use unsafe APIs. This lets us read native memory without
  // copies and not have to switch back and forth between little endian and big endian.
  public static final Unsafe UNSAFE;

  // This is the offset to the start of the array data. (There's some bytes
  // before the array data like the size and other java stuff).
  private static final int BYTE_ARRAY_DATA_OFFSET;

  static {
    UNSAFE = (Unsafe) AccessController.doPrivileged(
        new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            try {
              Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (NoSuchFieldException e) {
              throw new Error();
            } catch (IllegalAccessException e) {
              throw new Error();
            }
          }
        });

    BYTE_ARRAY_DATA_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  }

  // Copies src[srcOffset, srcOffset + len) into dst.
  public static void Copy(long dst, byte[] src, int srcOffset, int len) {
    UNSAFE.copyMemory(src, BYTE_ARRAY_DATA_OFFSET + srcOffset, null, dst, len);
  }

  // Copies src[0, len) into dst[dstOffset, dstOffset + len).
  public static void Copy(byte[] dst, int dstOffset, long src, int len) {
    UNSAFE.copyMemory(null, src, dst, dstOffset + BYTE_ARRAY_DATA_OFFSET, len);
  }
}
