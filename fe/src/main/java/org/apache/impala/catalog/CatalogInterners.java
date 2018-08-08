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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TTable;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import com.google.common.base.Preconditions;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Static utility methods for interning various objects used in the catalog. In many
 * cases, there are common strings that show up in lots of objects (eg class names,
 * database names, user names, property names), and interning these strings can result
 * in significant memory savings.
 */
public abstract class CatalogInterners {
  private static Interner<TNetworkAddress> NETWORK_ADDRESS_INTERNER =
      Interners.newWeakInterner();

  /**
   * Interner used for relatively-low-cardinality strings that aren't quite
   * low enough cardinality to be safe to use the JVM interner. The JVM's interner
   * uses a hashtable with a fixed number of buckets, so if we put anything
   * with unbounded cardinality in there, we risk a high number of collisions.
   *
   * See https://shipilev.net/jvm-anatomy-park/10-string-intern/ for more details
   * on those risks.
   */
  private static Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  // Do not instantiate
  private CatalogInterners() {}

  /**
   * Intern low-cardinality fields of a metastore table object in-place.
   */
  public static void internFieldsInPlace(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    if (msTable == null) return;
    // Database name is typically low cardinality, but still potentially risky
    // to put in the JVM string table, so we'll use our own interner instead.
    if (msTable.isSetDbName()) {
      msTable.setDbName(STRING_INTERNER.intern(msTable.getDbName()));
    }
    if (msTable.isSetOwner()) {
      msTable.setOwner(STRING_INTERNER.intern(msTable.getOwner()));
    }
    if (msTable.isSetParameters()) {
      msTable.setParameters(internParameters(msTable.getParameters()));
    }
    if (msTable.isSetTableType()) {
      msTable.setTableType(STRING_INTERNER.intern(msTable.getTableType()));
    }
    if (msTable.isSetSd()) {
      internFieldsInPlace(msTable.getSd());
    }
    if (msTable.isSetPartitionKeys()) {
      for (FieldSchema fs : msTable.getPartitionKeys()) internFieldsInPlace(fs);
    }
  }

  public static void internFieldsInPlace(TTable table) {
    if (table == null) return;
    internFieldsInPlace(table.getClustering_columns());
    internFieldsInPlace(table.getColumns());
    if (table.isSetHdfs_table()) {
      table.hdfs_table.setNullPartitionKeyValue(
          table.hdfs_table.getNullPartitionKeyValue().intern());
      table.hdfs_table.setNullColumnValue(
          table.hdfs_table.getNullColumnValue().intern());
      table.hdfs_table.setNetwork_addresses(internAddresses(
          table.hdfs_table.getNetwork_addresses()));
    }
  }

  private static void internFieldsInPlace(List<TColumn> cols) {
    if (cols == null) return;
    for (TColumn col : cols) {
      if (col.isSetColumnName()) {
        col.setColumnName(STRING_INTERNER.intern(col.getColumnName()));
      }
    }
  }

  /**
   * Intern low-cardinality fields in the given storage descriptor.
   */
  public static void internFieldsInPlace(StorageDescriptor sd) {
    if (sd == null) return;
    if (sd.isSetCols()) {
      for (FieldSchema fs : sd.getCols()) internFieldsInPlace(fs);
    }
    if (sd.isSetInputFormat()) {
      sd.setInputFormat(STRING_INTERNER.intern(sd.getInputFormat()));
    }
    if (sd.isSetOutputFormat()) {
      sd.setOutputFormat(STRING_INTERNER.intern(sd.getOutputFormat()));
    }
    if (sd.isSetParameters()) {
      sd.setParameters(internParameters(sd.getParameters()));
    }
  }

  /**
   * Intern low-cardinality fields in the given FieldSchema.
   */
  private static void internFieldsInPlace(FieldSchema fs) {
    if (fs == null) return;
    if (fs.isSetName()) {
      fs.setName(STRING_INTERNER.intern(fs.getName()));
    }
    if (fs.isSetType()) {
      fs.setType(STRING_INTERNER.intern(fs.getType()));
    }
  }

  /**
   * Transform the given parameters map (for table or partition parameters) to intern
   * its keys and commonly-used values.
   */
  public static Map<String, String> internParameters(Map<String, String> parameters) {
    if (parameters == null) return null;
    Map<String, String> ret = Maps.newHashMapWithExpectedSize(parameters.size());
    for (Map.Entry<String, String> e : parameters.entrySet()) {
      // Intern values which we know will show up quite often. This is based on
      // the results of the following SQL query against an HMS database:
      //
      //   select PARAM_VALUE, count(*) from PARTITION_PARAMS where PARAM_KEY
      //   not like 'impala_%' group by PARAM_VALUE order by count(*) desc limit 100;
      //
      // In a large catalog from a production install, these represented about 68% of the
      // entries.
      String val = e.getValue();
      if (val.isEmpty() ||
          "-1".equals(val) ||
          "0".equals(val) ||
          "true".equalsIgnoreCase(val) ||
          "false".equalsIgnoreCase(val) ||
          "TASK".equals(val) ||
          val.startsWith("impala_")) {
        val = val.intern();
      } else if (val.length() <= 2) {
        // Very short values tend to be quite common -- for example most partitions
        // have less than 99 files. But, potential cardinality is high enough that
        // we avoid the JVM interner.
        val = STRING_INTERNER.intern(val);
      }

      // Assume that the keys used in the HMS have a low cardinality, even if technically
      // custom properties are allowed.
      ret.put(STRING_INTERNER.intern(e.getKey()), val);
    }
    Preconditions.checkState(ret.size() == parameters.size());
    return ret;
  }

  private static List<TNetworkAddress> internAddresses(List<TNetworkAddress> addrs) {
    if (addrs == null) return null;
    List<TNetworkAddress> ret = Lists.newArrayListWithCapacity(addrs.size());
    for (TNetworkAddress addr : addrs) {
      ret.add(CatalogInterners.internNetworkAddress(addr));
    }
    return ret;
  }

  /**
   * Intern a shared string. Use this only for strings reasonably expected to be
   * reused, such as an HBase column family, which are usually shared across many
   * columns.
   */
  public static String internString(String value) {
    if (value == null) return null;
    return STRING_INTERNER.intern(value);
  }

  /**
   * Intern the given network address object, and return an immutable version.
   */
  public static TNetworkAddress internNetworkAddress(TNetworkAddress addr) {
    if (addr == null) return null;
    // Intern an immutable subclass of the network address so that we don't
    // accidentally modify them. This doesn't override every mutating method
    // but it's likely someone would trip over one of these.
    return NETWORK_ADDRESS_INTERNER.intern(new TNetworkAddress(addr) {
      private static final long serialVersionUID = 1L;

      @Override
      public void clear() {
        throw new UnsupportedOperationException("immutable");
      }

      @Override
      public TNetworkAddress setHostname(String hostname) {
        throw new UnsupportedOperationException("immutable");
      }

      @Override
      public TNetworkAddress setPort(int port) {
        throw new UnsupportedOperationException("immutable");
      }

      @Override
      public void read(TProtocol iprot) throws TException {
        throw new UnsupportedOperationException("immutable");
      }
    });
  }
}
