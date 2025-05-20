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

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.log4j.Logger;
import org.apache.paimon.table.Table;

import java.io.IOException;
import java.util.Set;

/**
 * Paimon table for LocalCatalog
 */
public class LocalPaimonTable extends LocalTable implements FePaimonTable {
  private static final Logger LOG = Logger.getLogger(LocalPaimonTable.class);
  private Table table_;

  public static LocalPaimonTable load(LocalDb db,
      org.apache.hadoop.hive.metastore.api.Table msTbl, MetaProvider.TableMetaRef ref)
      throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTbl);
    Preconditions.checkNotNull(ref);
    try {
      LocalPaimonTable localPaimonTable = new LocalPaimonTable(db, msTbl, ref);
      return localPaimonTable;
    } catch (MetaException ex) {
      throw new TableLoadingException("Failed to load table" + msTbl.getTableName(), ex);
    }
  }

  protected LocalPaimonTable(LocalDb db, org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaProvider.TableMetaRef ref) throws MetaException {
    super(db, msTbl, ref);
    table_ = PaimonUtil.createFileStoreTable(msTbl);
    applyPaimonTableStatsIfPresent();
  }

  @Override
  public Table getPaimonApiTable() {
    return table_;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor = new TTableDescriptor(tableId,
        TTableType.PAIMON_TABLE, getTColumnDescriptors(), 0, name_, db_.getName());
    try {
      tableDescriptor.setPaimonTable(PaimonUtil.getTPaimonTable(this));
    } catch (IOException e) { throw new RuntimeException(e); }
    return tableDescriptor;
  }
}
