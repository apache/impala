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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.impala.thrift.TUniqueId;
import org.apache.kudu.client.KuduTransaction;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Object of this class manages opened KuduTransaction objects.
 * KuduTransaction is created on Coordinator daemon. It's also aborted or committed
 * on Coordinator daemon. Each KuduTransaction object automatically heartbeat. It's
 * important for each KuduTransaction object to be kept around until Impala is ready
 * to commit or rollback the transaction.
 */
public class KuduTransactionManager {
  public static final Logger LOG = Logger.getLogger(KuduTransactionManager.class);

  // Map of Kudu transactions.
  // It's thread safe.
  private ConcurrentHashMap<TUniqueId, KuduTransaction> transactions_;

  /**
   * Creates KuduTransactionManager object.
   */
  public KuduTransactionManager() {
    transactions_ = new ConcurrentHashMap<TUniqueId, KuduTransaction>();
  }

  /**
   * Add a transaction to the KuduTransaction manager.
   */
  public void addTransaction(TUniqueId queryId, KuduTransaction txn) {
    Preconditions.checkNotNull(queryId);
    Preconditions.checkNotNull(txn);
    transactions_.put(queryId, txn);
  }

  /**
   * Delete a transaction from the KuduTransaction manager.
   */
  public KuduTransaction deleteTransaction(TUniqueId queryId) {
    Preconditions.checkNotNull(queryId);
    KuduTransaction txn = transactions_.remove(queryId);
    if (txn == null) {
      LOG.info("Kudu transaction with query-id " + queryId + " was already removed "
          + "from KuduTransactionManager object or never existed.");
    };
    return txn;
  }
}
