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

package org.apache.impala.catalog;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.impala.common.TransactionException;
import org.apache.impala.common.TransactionKeepalive;
import org.apache.impala.common.TransactionKeepalive.HeartbeatContext;
import org.apache.impala.compat.MetastoreShim;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Transaction class that implements the AutoCloseable interface and hence the callers
 * should use the try-with-resources statement while creating an instance. In its
 * constructor it creates a transaction and also registers it for heartbeating.
 * In close() it aborts the transaction if it wasn't committed earlier.
 */
public class Transaction implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Transaction.class);

  private long transactionId_ = -1;
  private IMetaStoreClient hmsClient_;
  private TransactionKeepalive keepalive_;

  public Transaction(IMetaStoreClient hmsClient, TransactionKeepalive keepalive,
      String user, HeartbeatContext ctx)
      throws TransactionException {
    Preconditions.checkNotNull(hmsClient);
    Preconditions.checkNotNull(keepalive);
    hmsClient_ = hmsClient;
    keepalive_ = keepalive;
    transactionId_ = MetastoreShim.openTransaction(hmsClient_);
    LOG.info(String.format("Opened transaction %d by user '%s' ", transactionId_, user));
    keepalive_.addTransaction(transactionId_, ctx);
  }

  /**
   * Constructor for short-running transactions that we don't want to heartbeat.
   */
  public Transaction(IMetaStoreClient hmsClient, String user, String context)
      throws TransactionException {
    Preconditions.checkNotNull(hmsClient);
    hmsClient_ = hmsClient;
    transactionId_ = MetastoreShim.openTransaction(hmsClient_);
    LOG.info(String.format("Opened transaction %d by user '%s' in context: %s",
        transactionId_, user, context));
  }

  public long getId() { return transactionId_; }

  public void commit() throws TransactionException {
    Preconditions.checkState(transactionId_ > 0);
    if (keepalive_ != null) keepalive_.deleteTransaction(transactionId_);
    MetastoreShim.commitTransaction(hmsClient_, transactionId_);
    transactionId_ = -1;
  }

  @Override
  public void close() {
    // Return early if transaction was committed successfully.
    if (transactionId_ <= 0) return;

    if (keepalive_ != null) keepalive_.deleteTransaction(transactionId_);
    try {
      MetastoreShim.abortTransaction(hmsClient_, transactionId_);
    } catch (TransactionException e) {
      LOG.error("Cannot abort transaction with id " + String.valueOf(transactionId_), e);
    }
    transactionId_ = -1;
  }
}
