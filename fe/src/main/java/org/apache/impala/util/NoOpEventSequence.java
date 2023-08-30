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

/**
 * An EventSequence instance that won't be used. Some code paths (e.g. event-processor)
 * don't have catalog profiles so don't provide a timeline to update. Use this to avoid
 * passing in a null value.
 */
public class NoOpEventSequence extends EventSequence {

  public static final NoOpEventSequence INSTANCE = new NoOpEventSequence();

  public NoOpEventSequence() {
    super("unused");
  }

  public long markEvent(String label) {
    return 0;
  }
}
