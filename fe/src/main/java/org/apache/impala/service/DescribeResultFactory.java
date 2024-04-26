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

package org.apache.impala.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TResultRow;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Builds results for DESCRIBE DATABASE statements by constructing and
 * populating a TDescribeResult object.
 */
public class DescribeResultFactory {
  // Number of columns in each row of the DESCRIBE FORMATTED|EXTENDED result set.
  private final static int NUM_DESC_FORMATTED_RESULT_COLS = 3;
  // Empty column used to format description output table.
  private final static TColumnValue EMPTY = new TColumnValue().setString_val("");

  public static TDescribeResult buildDescribeDbResult(FeDb db,
    TDescribeOutputStyle outputFormat) {
    switch (outputFormat) {
      case MINIMAL: return describeDbMinimal(db);
      case FORMATTED:
      case EXTENDED:
        return describeDbExtended(db);
      default: throw new UnsupportedOperationException(
          "Unknown TDescribeOutputStyle value for describe database: " + outputFormat);
    }
  }

  /*
   * Builds results for a DESCRIBE DATABASE <db> command. This consists of the database
   * location and comment.
   */
  private static TDescribeResult describeDbMinimal(FeDb db) {
    TDescribeResult descResult = new TDescribeResult();

    org.apache.hadoop.hive.metastore.api.Database msDb = db.getMetaStoreDb();
    descResult.results = Lists.newArrayList();
    String location = null;
    String managedLocation = null;
    String comment = null;
    if(msDb != null) {
      location = msDb.getLocationUri();
      managedLocation = MetastoreShim.getManagedLocationUri(msDb);
      comment = msDb.getDescription();
    }

    TColumnValue dbNameCol = new TColumnValue();
    dbNameCol.setString_val(db.getName());
    TColumnValue dbLocationCol = new TColumnValue();
    dbLocationCol.setString_val(Objects.toString(location, ""));
    TColumnValue commentCol = new TColumnValue();
    commentCol.setString_val(Objects.toString(comment, ""));
    descResult.results.add(new TResultRow(
        Lists.newArrayList(dbNameCol, dbLocationCol, commentCol)));

    if (managedLocation != null) {
      TColumnValue keyCol = new TColumnValue();
      keyCol.setString_val("managedlocation:");
      TColumnValue dbManagedLocationCol = new TColumnValue();
      dbManagedLocationCol.setString_val(Objects.toString(managedLocation, ""));
      descResult.results.add(new TResultRow(
          Lists.newArrayList(keyCol, dbManagedLocationCol, EMPTY)));
    }
    return descResult;
  }

  /*
   * Helper function used to build privilege results.
   */
  private static void buildPrivilegeResult(
      TDescribeResult descResult, Map<String, List<PrivilegeGrantInfo>> privilegeMap) {
    if (privilegeMap == null) return;

    for (Map.Entry<String, List<PrivilegeGrantInfo>> privilegeEntry:
        privilegeMap.entrySet()) {
      TColumnValue title = new TColumnValue();
      title.setString_val("Privileges for " + privilegeEntry.getKey() + ": ");
      descResult.results.add(
          new TResultRow(Lists.newArrayList(title, EMPTY, EMPTY)));
      for (PrivilegeGrantInfo privilegeInfo: privilegeEntry.getValue()) {
        TColumnValue privilege = new TColumnValue();
        privilege.setString_val(
            privilegeInfo.getPrivilege() + " " + privilegeInfo.isGrantOption());
        TColumnValue grantor = new TColumnValue();
        grantor.setString_val(
            privilegeInfo.getGrantor() + " " + privilegeInfo.getGrantorType());
        TColumnValue grantTime = new TColumnValue();
        grantTime.setString_val(privilegeInfo.getCreateTime() + "");
        descResult.results.add(
            new TResultRow(Lists.newArrayList(privilege, grantor, grantTime)));
      }
    }
  }

  /*
   * Builds a TDescribeResult that contains the result of a DESCRIBE FORMATTED|EXTENDED
   * DATABASE <db> command. Output all the database's properties.
   */
  private static TDescribeResult describeDbExtended(FeDb db) {
    TDescribeResult descResult = describeDbMinimal(db);
    org.apache.hadoop.hive.metastore.api.Database msDb = db.getMetaStoreDb();
    String ownerName = null;
    PrincipalType ownerType = null;
    Map<String, String> params = null;
    PrincipalPrivilegeSet privileges = null;
    if(msDb != null) {
      ownerName = msDb.getOwnerName();
      ownerType = msDb.getOwnerType();
      params = msDb.getParameters();
      privileges = msDb.getPrivileges();
    }

    if (ownerName != null && ownerType != null) {
      TColumnValue owner = new TColumnValue();
      owner.setString_val("Owner: ");
      TResultRow ownerRow =
          new TResultRow(Lists.newArrayList(owner, EMPTY, EMPTY));
      descResult.results.add(ownerRow);

      TColumnValue ownerNameCol = new TColumnValue();
      ownerNameCol.setString_val(Objects.toString(ownerName, ""));
      TColumnValue ownerTypeCol = new TColumnValue();
      ownerTypeCol.setString_val(Objects.toString(ownerType, ""));
      descResult.results.add(
          new TResultRow(Lists.newArrayList(EMPTY, ownerNameCol, ownerTypeCol)));
    }

    if (params != null && params.size() > 0) {
      TColumnValue parameter = new TColumnValue();
      parameter.setString_val("Parameter: ");
      TResultRow parameterRow =
          new TResultRow(Lists.newArrayList(parameter, EMPTY, EMPTY));
      descResult.results.add(parameterRow);
      for (Map.Entry<String, String> param: params.entrySet()) {
        TColumnValue key = new TColumnValue();
        key.setString_val(Objects.toString(param.getKey(), ""));
        TColumnValue val = new TColumnValue();
        val.setString_val(Objects.toString(param.getValue(), ""));
        descResult.results.add(
            new TResultRow(Lists.newArrayList(EMPTY, key, val)));
      }
    }

    // Currently we only retrieve privileges stored in hive metastore.
    // TODO: Retrieve privileges from Catalog
    if (privileges != null) {
      buildPrivilegeResult(descResult, privileges.getUserPrivileges());
      buildPrivilegeResult(descResult, privileges.getGroupPrivileges());
      buildPrivilegeResult(descResult, privileges.getRolePrivileges());
    }
    return descResult;
  }

  /**
   * Builds a TDescribeResult that contains the result of a DESCRIBE FORMATTED|EXTENDED
   * <table> command. For the formatted describe output the goal is to be exactly the
   * same as what Hive (via HiveServer2) outputs, for compatibility reasons. To do this,
   * Hive's MetadataFormatUtils class is used to build the results.  filteredColumns is a
   * list of columns the user is authorized to view.
   */
  public static TDescribeResult buildDescribeFormattedResult(
      FeTable table, List<Column> filteredColumns) throws ImpalaRuntimeException {
    TDescribeResult result = new TDescribeResult();
    result.results = Lists.newArrayList();

    org.apache.hadoop.hive.metastore.api.Table msTable =
        table.getMetaStoreTable().deepCopy();
    // For some table formats (e.g. Avro) the column list in the table can differ from the
    // one returned by the Hive metastore. To handle this we use the column list from the
    // table which has already reconciled those differences.
    // Need to create a new list since if the columns are filtered, this will
    // affect the original list.
    List<Column> nonClustered = new ArrayList<Column>();
    List<Column> clustered = new ArrayList<Column>();
    for (Column col: filteredColumns) {
      if (table.isClusteringColumn(col)) {
        clustered.add(col);
      } else {
        nonClustered.add(col);
      }
    }
    msTable.getSd().setCols(Column.toFieldSchemas(nonClustered));
    msTable.setPartitionKeys(Column.toFieldSchemas(clustered));

    org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo pki =
        new org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo(
            table.getSqlConstraints().getPrimaryKeys(), table.getName(),
            table.getDb().getName());
    org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo fki =
        new org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo(
            table.getSqlConstraints().getForeignKeys(), table.getName(),
            table.getDb().getName());
    StringBuilder sb = new StringBuilder();
    // First add all the columns (includes partition columns).
    sb.append(MetastoreShim.getAllColumnsInformation(msTable.getSd().getCols(),
        msTable.getPartitionKeys(), true, false, true));
    if (table instanceof FeIcebergTable) {
      sb.append(MetastoreShim.getPartitionTransformInformation(
          FeIcebergTable.Utils.getPartitionTransformKeys((FeIcebergTable) table)));
    }
    // Add the extended table metadata information.
    sb.append(MetastoreShim.getTableInformation(msTable));
    sb.append(MetastoreShim.getConstraintsInformation(pki, fki));

    for (String line: sb.toString().split("\n")) {
      // To match Hive's HiveServer2 output, split each line into multiple column
      // values based on the field delimiter.
      String[] columns = line.split("\t");
      TResultRow resultRow = new TResultRow();
      for (int i = 0; i < NUM_DESC_FORMATTED_RESULT_COLS; ++i) {
        TColumnValue colVal = new TColumnValue();
        colVal.setString_val(null);
        if (columns.length > i) {
          // Add the column value.
          colVal.setString_val(columns[i]);
        }
        resultRow.addToColVals(colVal);
      }
      result.results.add(resultRow);
    }
    return result;
  }

  /**
   * Builds a TDescribeResult for a nested collection whose fields are represented
   * by the given StructType.
   */
  public static TDescribeResult buildDescribeMinimalResult(StructType type) {
    TDescribeResult descResult = new TDescribeResult();
    descResult.results = Lists.newArrayList();
    for (StructField field: type.getFields()) {
      TColumnValue colNameCol = new TColumnValue();
      colNameCol.setString_val(field.getName());
      TColumnValue dataTypeCol = new TColumnValue();
      dataTypeCol.setString_val(field.getType().prettyPrint().toLowerCase());
      TColumnValue commentCol = new TColumnValue();
      commentCol.setString_val(Strings.nullToEmpty(field.getComment()));
      descResult.results.add(
          new TResultRow(Lists.newArrayList(colNameCol, dataTypeCol, commentCol)));
    }
    return descResult;
  }

  /**
   * Builds a TDescribeResult for a kudu table from a list of columns.
   */
  public static TDescribeResult buildKuduDescribeMinimalResult(List<Column> columns) {
    TDescribeResult descResult = new TDescribeResult();
    descResult.results = Lists.newArrayList();
    for (Column c: columns) {
      Preconditions.checkState(c instanceof KuduColumn);
      KuduColumn kuduColumn = (KuduColumn) c;
      // General describe info.
      TColumnValue colNameCol = new TColumnValue();
      colNameCol.setString_val(kuduColumn.getName());
      TColumnValue dataTypeCol = new TColumnValue();
      dataTypeCol.setString_val(kuduColumn.getType().prettyPrint().toLowerCase());
      TColumnValue commentCol = new TColumnValue();
      commentCol.setString_val(Strings.nullToEmpty(kuduColumn.getComment()));
      // Kudu-specific describe info.
      TColumnValue pkCol = new TColumnValue();
      pkCol.setString_val(Boolean.toString(kuduColumn.isKey()));
      TColumnValue pkUniqueCol = new TColumnValue();
      if (kuduColumn.isKey()) {
        pkUniqueCol.setString_val(Boolean.toString(kuduColumn.isPrimaryKeyUnique()));
      } else {
        pkUniqueCol.setString_val("");
      }
      TColumnValue nullableCol = new TColumnValue();
      nullableCol.setString_val(Boolean.toString(kuduColumn.isNullable()));
      TColumnValue defaultValCol = new TColumnValue();
      if (kuduColumn.hasDefaultValue()) {
        defaultValCol.setString_val(kuduColumn.getDefaultValueString());
      } else {
        defaultValCol.setString_val("");
      }
      TColumnValue encodingCol = new TColumnValue();
      encodingCol.setString_val(kuduColumn.getEncoding().toString());
      TColumnValue compressionCol = new TColumnValue();
      compressionCol.setString_val(kuduColumn.getCompression().toString());
      TColumnValue blockSizeCol = new TColumnValue();
      blockSizeCol.setString_val(Integer.toString(kuduColumn.getBlockSize()));
      descResult.results.add(new TResultRow(
          Lists.newArrayList(colNameCol, dataTypeCol, commentCol, pkCol, pkUniqueCol,
              nullableCol, defaultValCol, encodingCol, compressionCol, blockSizeCol)));
    }
    return descResult;
  }

  /**
   * Builds a TDescribeResult for a Iceberg table from a list of columns.
   */
  public static TDescribeResult buildIcebergDescribeMinimalResult(List<Column> columns) {
    TDescribeResult descResult = new TDescribeResult();
    descResult.results = Lists.newArrayList();
    for (Column c: columns) {
      Preconditions.checkState(c instanceof IcebergColumn);
      IcebergColumn icebergColumn = (IcebergColumn) c;
      // General describe info.
      TColumnValue colNameCol = new TColumnValue();
      colNameCol.setString_val(icebergColumn.getName());
      TColumnValue dataTypeCol = new TColumnValue();
      dataTypeCol.setString_val(icebergColumn.getType().prettyPrint().toLowerCase());
      TColumnValue commentCol = new TColumnValue();
      commentCol.setString_val(Strings.nullToEmpty(icebergColumn.getComment()));
      // Iceberg-specific describe info.
      TColumnValue nullableCol = new TColumnValue();
      nullableCol.setString_val(Boolean.toString(icebergColumn.isNullable()));
      descResult.results.add(new TResultRow(
          Lists.newArrayList(colNameCol, dataTypeCol, commentCol, nullableCol)));
    }
    return descResult;
  }

  /**
   * Builds a TDescribeResult for an Iceberg metadata table from an IcebergTable and a
   * metadata table name.
   *
   * This describe request is issued against a VirtualTable which only exists in the
   * Analyzer's StmtTableCache. Therefore, to get the columns of an IcebergMetadataTable
   * it is simpler to re-create this object than to extract those from a new
   * org.apache.iceberg.Table object or to send it over.
   */
  public static TDescribeResult buildIcebergMetadataDescribeMinimalResult(
      FeIcebergTable table, String vTableName) throws ImpalaRuntimeException {
    IcebergMetadataTable metadataTable = new IcebergMetadataTable(table, vTableName);
    return buildIcebergDescribeMinimalResult(metadataTable.getColumns());
  }
}
