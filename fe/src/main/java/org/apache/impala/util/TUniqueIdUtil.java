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
import com.google.common.primitives.UnsignedLong;

/**
 * Utility functions for working with TUniqueId objects.
 */
public class TUniqueIdUtil {
  public static String PrintId(TUniqueId id) {
    return Long.toHexString(id.hi) + ":" + Long.toHexString(id.lo);
  }

  public static TUniqueId ParseId(String id) {
    String[] splitted = id.split(":");
    if (splitted.length != 2) throw new NumberFormatException(
        "Invalid unique id format: " + id);
    return new TUniqueId(UnsignedLong.valueOf(splitted[0], 16).longValue(),
        UnsignedLong.valueOf(splitted[1], 16).longValue());
  }
}
