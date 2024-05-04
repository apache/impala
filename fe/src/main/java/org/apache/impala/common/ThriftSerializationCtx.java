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

import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.planner.TupleCacheInfo;

/**
 * The Thrift serialization functions need to adjust output based on whether the
 * Thrift serialization is happening for tuple caching or not. This context is
 * passed to Thrift serialization functions like initThrift() or toThrift().
 * The context currently provides a way to determine whether serialization is happening
 * for tuple caching.
 *
 * It also provides several functions that adjust their behavior based on whether
 * this is for tuple caching or not (e.g. translateTupleId()). This allows
 * caller to run the function unconditionally without needing extra if/else checks
 * for tuple caching. e.g.
 *
 * struct.id = serialCtx.translateTupleId(id_).asInt();
 *
 * rather than:
 *
 * if (serialCtx.isTupleCache()) {
 *   struct.id = serialCtx.translateTupleId(id_).asInt();
 * } else {
 *   struct.id = id_.asInt();
 * }
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

  /**
   * registerTuple() should be called for each TupleId that is referenced from a PlanNode.
   * This only has an effect for tuple caching.
   */
  public void registerTuple(TupleId id) {
    if (isTupleCache()) {
      tupleCacheInfo_.registerTuple(id);
    }
  }

  /**
   * registerTable() should be called for any table that is referenced from a PlanNode
   * that participates in tuple caching. In practice, this is only used for HDFS tables
   * at the moment.
   */
  public void registerTable(FeTable table) {
    if (isTupleCache()) {
      tupleCacheInfo_.registerTable(table);
    }
  }

  /**
   * translateTupleId() is designed to be applied to every TupleId incorporated into a
   * Thrift structure. For tuple caching, translateTupleId() translates the passed in
   * global TupleId into a local TupleId. All other use cases use the global id, so this
   * simply returns the globalId passed in. Any TupleId passed into translateTupleId()
   * must already be registered via registerTuple().
   */
  public TupleId translateTupleId(TupleId globalId) {
    if (isTupleCache()) {
      return tupleCacheInfo_.getLocalTupleId(globalId);
    } else {
      return globalId;
    }
  }

  /**
   * translateSlotId() is designed to be applied to every SlotId incorporated into a
   * Thrift structure. For tuple caching, translateSlotId() translates the passed in
   * global SlotId into a local SlotId. All other use cases use the global id, so this
   * simply returns the globalId passed in.
   */
  public SlotId translateSlotId(SlotId globalId) {
    if (isTupleCache()) {
      return tupleCacheInfo_.getLocalSlotId(globalId);
    } else {
      return globalId;
    }
  }
}
