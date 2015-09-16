// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog.delegates;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.cloudera.impala.thrift.TDistributeParam;
import com.cloudera.impala.thrift.TDistributeType;
import com.cloudera.impala.util.KuduUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.CreateTableBuilder;
import org.kududb.client.KuduClient;
import org.kududb.client.PartialRow;

import com.cloudera.impala.catalog.KuduTable;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.google.common.collect.Lists;

import static com.cloudera.impala.util.KuduUtil.compareSchema;
import static com.cloudera.impala.util.KuduUtil.fromImpalaType;
import static com.cloudera.impala.util.KuduUtil.parseKeyColumns;
import static com.cloudera.impala.util.KuduUtil.parseSplits;
import static org.kududb.client.KuduClient.KuduClientBuilder;


/**
 * Implementation of the Kudu DDL Delegate. Propagates create and drop table statements to
 * Kudu.
 */
public class KuduDdlDelegate extends DdlDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(KuduDdlDelegate.class);

  public KuduDdlDelegate(Table msTbl) {
    setMsTbl(msTbl);
  }

  /**
   * Creates the Kudu table if it does not exist and returns true. If the table exists and
   * the table is not a managed table ignore and return false, otherwise throw an
   * exception.
   */
  @Override
  public void createTable()
      throws ImpalaRuntimeException {

    String kuduTableName = msTbl_.getParameters().get(KuduTable.KEY_TABLE_NAME);
    String kuduMasters = msTbl_.getParameters().get(KuduTable.KEY_MASTER_ADDRESSES);

    // Can be optional for un-managed tables
    String kuduKeyCols = msTbl_.getParameters().get(KuduTable.KEY_KEY_COLUMNS);

    String replication = msTbl_.getParameters().get(KuduTable.KEY_TABLET_REPLICAS);

    KuduClientBuilder builder = new KuduClientBuilder(kuduMasters);
    KuduClient client = builder.build();
    try {
      // TODO should we throw if the table does not exist when its an external table?
      if (client.tableExists(kuduTableName)) {
        if (msTbl_.getTableType().equals(TableType.MANAGED_TABLE.toString())) {
          throw new ImpalaRuntimeException(String.format(
              "Table %s already exists in Kudu master %s.", kuduTableName, kuduMasters));
        }

        // Check if the external table matches the schema
        org.kududb.client.KuduTable kuduTable = client.openTable(kuduTableName);
        if (!compareSchema(msTbl_, kuduTable)) {
          throw new ImpalaRuntimeException(String.format(
              "Table %s (%s) has a different schema in Kudu than in Hive.",
              msTbl_.getTableName(), kuduTableName));
        }
        return;
      }

      HashSet<String> keyColNames = parseKeyColumns(kuduKeyCols);
      List<ColumnSchema> keyColSchemas = new ArrayList<>();

      // Create a new Schema and map the types accordingly
      ArrayList<ColumnSchema> columns = Lists.newArrayList();
      for (FieldSchema fieldSchema: msTbl_.getSd().getCols()) {
        com.cloudera.impala.catalog.Type catalogType = com.cloudera.impala.catalog.Type
            .parseColumnType(fieldSchema);
        if (catalogType == null) {
          throw new ImpalaRuntimeException(String.format(
              "Could not parse column type %s.", fieldSchema.getType()));
        }
        Type t = fromImpalaType(catalogType);
        // Create the actual column and check if the column is a key column
        ColumnSchemaBuilder csb = new ColumnSchemaBuilder(
            fieldSchema.getName(), t);
        boolean isKeyColumn = keyColNames.contains(fieldSchema.getName());
        csb.key(isKeyColumn);
        csb.nullable(!isKeyColumn);
        ColumnSchema cs = csb.build();
        columns.add(cs);
        if (isKeyColumn) keyColSchemas.add(cs);
      }

      Schema schema = new Schema(columns);
      CreateTableBuilder ctb = new CreateTableBuilder();

      // Handle auto-partitioning of the Kudu table
      if (distributeParams_ != null) {
        for (TDistributeParam dP : distributeParams_) {
          if (dP.getType() == TDistributeType.HASH) {
            ctb.addHashPartitions(dP.getBy_hash_param().getColumns(),
                dP.getBy_hash_param().getBuckets());
          } else {
            Preconditions.checkState(dP.getType() == TDistributeType.RANGE);
            ctb.setRangePartitionColumns(dP.getBy_range_param().getColumns());
            for (PartialRow p : KuduUtil.parseSplits(schema, dP.getBy_range_param())) {
              ctb.addSplitRow(p);
            }
          }
        }
      }

      if (Strings.isNullOrEmpty(replication)) {
        ctb.setNumReplicas(1);
      } else {
        int r = Integer.parseInt(replication);
        if (r <= 0) {
          throw new ImpalaRuntimeException(
              "Number of tablet replicas must be greater than zero. " +
              "Given number of replicas is: " + Integer.toString(r));
        }
        ctb.setNumReplicas(r);
      }

      client.createTable(kuduTableName, schema, ctb);
    } catch (ImpalaRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new ImpalaRuntimeException("Error creating Kudu table", e);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Error closing Kudu client", e);
      }
    }
  }

  @Override
  public void dropTable() throws ImpalaRuntimeException {
    // If table is an external table, do not delete the data
    if (msTbl_.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) return;

    String kuduTableName = msTbl_.getParameters().get(KuduTable.KEY_TABLE_NAME);
    String kuduMasters = msTbl_.getParameters().get(KuduTable.KEY_MASTER_ADDRESSES);

    KuduClientBuilder builder = new KuduClientBuilder(kuduMasters);
    KuduClient client = builder.build();
    try {
      if (!client.tableExists(kuduTableName)) {
        LOG.warn("Table: %s is in inconsistent state. It does not exist in Kudu master(s)"
            + " %s, but it exists in Hive metastore. Deleting from metastore only.",
            kuduTableName, kuduMasters);
        return;
      }
      client.deleteTable(kuduTableName);
      return;
    } catch (ImpalaRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new ImpalaRuntimeException("Error dropping Kudu table", e);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Could not close Kudu client.", e);
      }
    }
  }

  public static boolean canHandle(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return KuduTable.isKuduTable(msTbl);
  }

  @Override
  public boolean alterTable() throws ImpalaRuntimeException {
    throw new ImpalaRuntimeException(
        "Alter table operations are not supported for Kudu tables.");
  }
}
