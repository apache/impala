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

package org.apache.impala.common;

import org.apache.impala.planner.TupleCacheInfo;

/**
 * The Thrift serialization functions need to adjust output based on whether the
 * Thrift serialization is happening for tuple caching or not. This context is
 * passed to Thrift serialization functions like initThrift() or toThrift().
 * The context currently provides a way to determine whether serialization is happening
 * for tuple caching. It will be expanded to provide other methods that the Thrift
 * serialization functions can call to translate the Thrift output or register
 * non-deterministic operations.
 */
public class ThriftSerializationCtx {
  private TupleCacheInfo tupleCacheInfo_;

  /**
   * Constructor for tuple caching serialization
   */
  public ThriftSerializationCtx(TupleCacheInfo tupleCacheInfo) {
    tupleCacheInfo_ = tupleCacheInfo;
  }

  /**
   * Constructor for normal serialization
   */
  public ThriftSerializationCtx() {
    tupleCacheInfo_ = null;
  }

  /**
   * Returns whether serialization is on behalf of the tuple cache or not. This is
   * intended to be used to mask out unnecessary fields.
   */
  public boolean isTupleCache() {
    return tupleCacheInfo_ != null;
  }
}
