/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.impala.util.paimon;

import org.apache.arrow.memory.RootAllocator;

/*
 *
 * Arrow Root Allocation singleton
 * Note: all Arrow code should use this as root allocator.
 *
 * */
public class ArrowRootAllocation {
  private static RootAllocator ROOT_ALLOCATOR;

  private ArrowRootAllocation() {}

  public static RootAllocator rootAllocator() {
    synchronized (ArrowRootAllocation.class) {
      if (ROOT_ALLOCATOR == null) {
        ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> ROOT_ALLOCATOR.close()));
      }
    }
    return ROOT_ALLOCATOR;
  }
}
