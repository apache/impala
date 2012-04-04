// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.google.common.collect.Lists;

/**
 * Internal representation of table-related metadata of an hdfs-resident table.
 * Owned by Catalog instance.
 * The partition keys constitute the clustering columns.
 */
public abstract class HdfsTable extends Table {

  private static final String DEFAULT_LINE_DELIM = "\n";
  // hive by default uses ctrl-a as field delim
  private static final String DEFAULT_FIELD_DELIM = "\u0001";
  // hive by default has no escape char
  private static final String DEFAULT_ESCAPE_CHAR = "\u0000";

  private String lineDelim;
  private String fieldDelim;
  private String collectionDelim;
  private String mapKeyDelim;
  private String escapeChar;

  // Hive uses this string for NULL partition keys. Set in load().
  private String nullPartitionKeyValue;

  /**
   * Query-relevant information for one table partition.
   *
   */
  public static class Partition {
    public List<LiteralExpr> keyValues; // same size as
                                        // org.apache.hadoop.hive.metastore.api.Table.getPartitionKeysSize()
    public List<String> filePaths;  // paths of hdfs data files
    public List<Long> fileLengths;  // length as returned by FileStatus.getLen()

    Partition() {
      this.keyValues = Lists.newArrayList();
      this.filePaths = Lists.newArrayList();
      this.fileLengths = Lists.newArrayList();
    }
  }

  private final List<Partition> partitions; // these are only non-empty partitions

  // Base Hdfs directory where files of this table are stored.
  // For unpartitioned tables it is simply the path where all files live.
  // For partitioned tables it is the root directory
  // under which partition dirs are placed.
  protected String hdfsBaseDir;

  private final static Logger LOG = LoggerFactory.getLogger(HdfsTable.class);

  protected HdfsTable(TableId id, Db db, String name, String owner) {
    super(id, db, name, owner);
    this.partitions = Lists.newArrayList();
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  public boolean isClusteringColumn(Column col) {
    return col.getPosition() < getNumClusteringCols();
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
  public boolean loadPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> msPartitions,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    partitions.clear();
    try {
      hdfsBaseDir = msTbl.getSd().getLocation();
      // This table has no partition key, which means it has no declared partitions.
      if (msTbl.getPartitionKeysSize() == 0) {
        Partition p = new Partition();
        loadFilePaths(msTbl.getSd().getLocation(), p);
        return true;
      }
      for (org.apache.hadoop.hive.metastore.api.Partition msPartition: msPartitions) {
        Partition p = new Partition();
        // load key values
        int numPartitionKey = 0;
        for (String partitionKey: msPartition.getValues()) {
          // Deal with Hive's special NULL partition key.
          if (partitionKey.equals(nullPartitionKeyValue)) {
            p.keyValues.add(new NullLiteral());
          } else {
            PrimitiveType type = colsByPos.get(numPartitionKey).getType();
            Expr expr = LiteralExpr.create(partitionKey, type);
            // Force the literal to be of type declared in the metadata.
            expr = expr.castTo(type);
            p.keyValues.add((LiteralExpr)expr);
          }
          ++numPartitionKey;
        }

        // load file paths
        loadFilePaths(msPartition.getSd().getLocation(), p);
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

  private void loadFilePaths(String location, Partition p) throws IOException {
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(new Configuration());
    FileStatus[] fileStatus = fs.listStatus(path);
    for (int i = 0; i < fileStatus.length; ++i) {
      p.filePaths.add(fileStatus[i].getPath().toString());
      p.fileLengths.add(fileStatus[i].getLen());
    }

    // Don't add empty partitions.
    if (p.filePaths.size() > 0) {
      partitions.add(p);
    }
  }

  @Override
  public Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // turn all exceptions into unchecked exception
    try {
      // set nullPartitionKeyValue from the hive conf.
      nullPartitionKeyValue =
          client.getConfigValue("hive.exec.default.partition.name",
          "__HIVE_DEFAULT_PARTITION__");

      // we only support single-character delimiters,
      // ignore this table if we find a multi-character delimiter
      try {
        setDelimiters(msTbl.getSd().getSerdeInfo());
      } catch (Exception e) {
        LOG.warn("Ignoring table {} because setting delimiters failed, " +
            "with exception message:\n{}",
            new Object[] {name, e.getMessage()});
        return null;
      }

      // populate with both partition keys and regular columns
      List<FieldSchema> partKeys = msTbl.getPartitionKeys();
      List<FieldSchema> tblFields = client.getFields(db.getName(), name);
      List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>(
          partKeys.size() + tblFields.size());
      fieldSchemas.addAll(partKeys);
      fieldSchemas.addAll(tblFields);
      if (!loadColumns(fieldSchemas)) {
        return null;
      }

      // The number of clustering columns is the number of partition keys.
      numClusteringCols = partKeys.size();

      if (!loadPartitions(
          client.listPartitions(db.getName(), name, Short.MAX_VALUE), msTbl)) {
        return null;
      }
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
    } catch (ConfigValSecurityException e) {
      throw new UnsupportedOperationException(e.toString());
    }

    return this;
  }

  // The metastore may return null for delimiter parameters,
  // which means we need to use a default instead.
  // We tried long and hard to find default values for delimiters in Hive,
  // but could not find them.
  private void setDelimiters(SerDeInfo serdeInfo) throws Exception {
    // For reporting all exceptions.
    ArrayList<String> exceptionMessages = new ArrayList<String>();
    // Hive currently only supports newline.
    lineDelim = serdeInfo.getParameters().get(Constants.LINE_DELIM);
    if (lineDelim != null) {
      if (lineDelim.length() != 1) {
        exceptionMessages.add("Line delimiter found: '" + lineDelim + "'");
      }
    } else {
      // default value
      lineDelim = DEFAULT_LINE_DELIM;
    }
    fieldDelim = serdeInfo.getParameters().get(Constants.FIELD_DELIM);
    if (fieldDelim != null) {
      if (fieldDelim.length() != 1) {
        exceptionMessages.add("Field delimiter found: '" + fieldDelim + "'");
      }
    } else {
      // default value
      fieldDelim = DEFAULT_FIELD_DELIM;
    }
    collectionDelim = serdeInfo.getParameters().get(Constants.COLLECTION_DELIM);
    if (collectionDelim != null) {
      if (collectionDelim.length() != 1) {
        exceptionMessages.add("Collection-item delimiter found: '" + collectionDelim + "'");
      }
    } else {
      // default value
      collectionDelim = fieldDelim;
    }
    mapKeyDelim = serdeInfo.getParameters().get(Constants.MAPKEY_DELIM);
    if (mapKeyDelim != null) {
      if (mapKeyDelim.length() != 1) {
        exceptionMessages.add("MapKey delimiter found: '" + mapKeyDelim + "'");
      }
    } else {
      // default value
      mapKeyDelim = fieldDelim;
    }
    escapeChar = serdeInfo.getParameters().get(Constants.ESCAPE_CHAR);
    if (escapeChar != null) {
      if (escapeChar.length() != 1) {
        exceptionMessages.add("Escape character found: '" + escapeChar + "'");
      }
    } else {
      // default value
      escapeChar = DEFAULT_ESCAPE_CHAR;
    }

    // Throw exception if we failed to set at least one delimiter/quote/escape char.
    if (!exceptionMessages.isEmpty()) {
      StringBuilder strBuilder = new StringBuilder();
      strBuilder.append(
          "We only support single-character delimiters, quotes, and escape chars. " +
          "Found the following properties:\n");
      for (String s : exceptionMessages) {
        strBuilder.append(s);
        strBuilder.append('\n');
      }
      // Remove trailing newline.
      strBuilder.deleteCharAt(strBuilder.length()-1);
      throw new Exception(strBuilder.toString());
    }
  }

  @Override
  public TTableDescriptor toThrift() {
    TTableDescriptor TTableDescriptor =
        new TTableDescriptor(
            id.asInt(), TTableType.HBASE_TABLE, colsByPos.size(), numClusteringCols);
    List<String> partitionKeyNames = new ArrayList<String>();
    for (int i = 0; i < numClusteringCols; ++i) {
      partitionKeyNames.add(colsByPos.get(i).getName());
    }
    // We only support single-byte characters as delimiters.
    THdfsTable tHdfsTable = new THdfsTable(hdfsBaseDir,
        partitionKeyNames, nullPartitionKeyValue,
        (byte) lineDelim.charAt(0), (byte) fieldDelim.charAt(0),
        (byte) collectionDelim.charAt(0), (byte) mapKeyDelim.charAt(0),
        (byte) escapeChar.charAt(0));
    TTableDescriptor.setHdfsTable(tHdfsTable);
    return TTableDescriptor;
  }

  public String getLineDelim() {
    return lineDelim;
  }

  public String getFieldDelim() {
    return fieldDelim;
  }

  public String getCollectionDelim() {
    return collectionDelim;
  }

  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  public String getEscapeChar() {
    return escapeChar;
  }

  /**
   * Return locations for all blocks in all files in filePaths.
   * @return list of (file path, BlockLocation) pairs
   */
  public static List<Pair<String, BlockLocation>>
      getBlockLocations(List<String> filePaths) {
    List<Pair<String, BlockLocation>> result = Lists.newArrayList();
    Configuration conf = new Configuration();
    for (String path: filePaths) {
      Path p = new Path(path);
      BlockLocation[] locations = null;
      try {
        FileSystem fs = p.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(p);
        // Ignore directories (and files in them) - if a directory is erroneously created
        // as a subdirectory of a partition dir we should ignore it and move on
        // (getFileBlockLocations will throw when
        // called on a directory). Hive will not recurse into directories.
        if (!fileStatus.isDirectory()) {
          locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
          for (int i = 0; i < locations.length; ++i) {
            result.add(Pair.create(path, locations[i]));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "couldn't determine block locations for path '" + path + "':\n"
            + e.getMessage());
      }
    }
    return result;
  }

  public String getHdfsBaseDir() {
    return hdfsBaseDir;
  }
}
