// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Internal representation of table-related metadata. Owned by Catalog instance.
 */
public class Table {
  private final Db db;
  private final String name;
  private final String owner;
  private int numPartitionKeys;

  /**  the first 'numPartitionKeys' cols are partition keys */
  private final ArrayList<Column> colsByPos;

  /**  map from lowercase col. name to Column */
  private final Map<String, Column> colsByName;

  /**
   * Query-relevant information for one table partition.
   *
   */
  public static class Partition {
    public List<LiteralExpr> keyValues;
    public List<String> filePaths;  // paths of hdfs data files

    Partition() {
      this.keyValues = Lists.newArrayList();
      this.filePaths = Lists.newArrayList();
    }
  }

  private final List<Partition> partitions;

  private final static Logger LOG = LoggerFactory.getLogger(Table.class);

  private Table(Db db, String name, String owner) {
    this.db = db;
    this.name = name;
    this.owner = owner;
    this.colsByPos = Lists.newArrayList();
    this.colsByName = Maps.newHashMap();
    this.partitions = Lists.newArrayList();
  }

  public Db getDb() {
    return db;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  /**
   * Create columns corresponding to fieldSchemas.
   * @param fieldSchemas
   * @return true if success, false otherwise
   */
  private boolean loadColumns(List<FieldSchema> fieldSchemas) {
    int pos = 0;
    for (FieldSchema s : fieldSchemas) {
      // catch currently unsupported hive schema elements
      if (!Constants.PrimitiveTypes.contains(s.getType())) {
        LOG.warn("Ignoring table {} because column {} " +
            "contains a field of unsupported type {}. " +
            "Only primitive types are currently supported.",
            new Object[] {getName(), s.getName(), s.getType()});
        return false;
      }
      Column col = new Column(s.getName(), getPrimitiveType(s.getType()), pos);
      colsByPos.add(col);
      colsByName.put(s.getName(), col);
      ++pos;
    }
    return true;
  }

  /**
   * Create Partition objects corresponding to 'partitions'.
   * @param partitions
   * @param msTbl
   * @return true if successful, false otherwise.
   */
  private boolean loadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    try {
      for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
        Partition p = new Partition();

        // load key values
        int numPartitionKey = 0;
        for (String partitionKey: msPartition.getValues()) {
          p.keyValues.add(
              LiteralExpr.create(partitionKey, colsByPos.get(numPartitionKey).getType()));
          ++numPartitionKey;
        }

        // load file paths
        Path path = new Path(msPartition.getSd().getLocation());
        FileSystem fs = path.getFileSystem(new Configuration());
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i = 0; i < fileStatus.length; ++i) {
          p.filePaths.add(fileStatus[i].getPath().toString());
        }

        partitions.add(p);

      }
    } catch (AnalysisException e) {
      // couldn't parse one of the partition key values, something wrong with the md
      // TODO: tell the user which particular value failed to parse
      LOG.warn("couldn't parse a partition key value: " + e.getMessage());
      return false;
    } catch (IOException e) {
      // one of the path lookup calls failed
      // TODO: tell the user for which partition? (or is that implicit in e.getMessage()?)
      LOG.warn("path lookup failed: " + e.getMessage());
      return false;
    }
    return true;
  }

  public static Table loadTable(HiveMetaStoreClient client, Db db,
                                 String tblName) {
    // turn all exceptions into unchecked exception
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl = client.getTable(db.getName(), tblName);
      Table table = new Table(db, tblName, msTbl.getOwner());

      // populate with both partition keys and regular columns
      List<FieldSchema> fieldSchemas = msTbl.getPartitionKeys();
      table.numPartitionKeys = fieldSchemas.size();
      fieldSchemas.addAll(client.getFields(db.getName(), tblName));
      if (!table.loadColumns(fieldSchemas)) {
        return null;
      }

      if (!table.loadPartitions(
          client.listPartitions(db.getName(), tblName, Short.MAX_VALUE), msTbl)) {
        return null;
      }
      return table;
    } catch (TException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (NoSuchObjectException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (UnknownDBException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (MetaException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (UnknownTableException e) {
      throw new UnsupportedOperationException(e.toString());
    }
  }

  private static PrimitiveType getPrimitiveType(String typeName) {
    if (typeName.toLowerCase().equals("tinyint")) {
      return PrimitiveType.TINYINT;
    } else if (typeName.toLowerCase().equals("smallint")) {
      return PrimitiveType.SMALLINT;
    } else if (typeName.toLowerCase().equals("int")) {
      return PrimitiveType.INT;
    } else if (typeName.toLowerCase().equals("bigint")) {
      return PrimitiveType.BIGINT;
    } else if (typeName.toLowerCase().equals("boolean")) {
      return PrimitiveType.BOOLEAN;
    } else if (typeName.toLowerCase().equals("float")) {
      return PrimitiveType.FLOAT;
    } else if (typeName.toLowerCase().equals("double")) {
      return PrimitiveType.DOUBLE;
    } else if (typeName.toLowerCase().equals("date")) {
      return PrimitiveType.DATE;
    } else if (typeName.toLowerCase().equals("datetime")) {
      return PrimitiveType.DATETIME;
    } else if (typeName.toLowerCase().equals("timestamp")) {
      return PrimitiveType.TIMESTAMP;
    } else if (typeName.toLowerCase().equals("string")) {
      return PrimitiveType.STRING;
    } else {
      return PrimitiveType.INVALID_TYPE;
    }
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return db.getName() + "." + name;
  }

  public String getOwner() {
    return owner;
  }

  public List<Column> getColumns() {
    return colsByPos;
  }

  /**
   * Case-insensitive lookup.
   */
  public Column getColumn(String name) {
    return colsByName.get(name.toLowerCase());
  }
}
