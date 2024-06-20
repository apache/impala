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

package org.apache.impala.catalog.local;

import org.apache.impala.thrift.CatalogLookupStatus;

/**
 * If this is thrown, it indicates that the catalog implementation in the Impalad
 * has identified that the metadata it read was not a proper snapshot of the source
 * metadata. In other words, the resulting plan may be incorrect and so the plan
 * in progress should be discarded and retried. It should be assumed that, if this
 * exception is thrown, the catalog has already taken appropriate steps to ensure that
 * a retry will not encounter the same inconsistency.
 *
 * Note that the above does not guarantee that a retry will succeed, only that it will
 * not encounter the _same_ conflict.
 */
public class InconsistentMetadataFetchException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final CatalogLookupStatus reason_;

  public InconsistentMetadataFetchException(CatalogLookupStatus reason, String msg) {
    super(msg);
    reason_ = reason;
  }

  public CatalogLookupStatus getReason() { return reason_; }
}
