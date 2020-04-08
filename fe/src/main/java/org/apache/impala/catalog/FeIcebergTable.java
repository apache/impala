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

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.TResultRowBuilder;

/**
 * Frontend interface for interacting with an Iceberg-backed table.
 */
public interface FeIcebergTable extends FeTable {
  /**
   * Return the name of iceberg table name, usually a hdfs location path
   */
  String getIcebergTableLocation();

  /**
   * Return the Iceberg partition spec info
   */
  List<IcebergPartitionSpec> getPartitionSpec();

  /**
   * Utility functions
   */
  public static abstract class Utils {
    public static TResultSet getPartitionSpecs(FeIcebergTable table)
        throws TableLoadingException {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);

      resultSchema.addToColumns(new TColumn("Partition Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Source Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Name", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Partition Transform",
          Type.STRING.toThrift()));

      TableMetadata metadata = IcebergUtil.
          getIcebergTableMetadata(table.getIcebergTableLocation());
      if (!metadata.specs().isEmpty()) {
        // Just show the latest PartitionSpec from iceberg table metadata
        PartitionSpec latestSpec = metadata.specs().get(metadata.specs().size() - 1);
        for(PartitionField field : latestSpec.fields()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(latestSpec.specId());
          builder.add(field.sourceId());
          builder.add(field.fieldId());
          builder.add(field.name());
          builder.add(IcebergUtil.getPartitionTransform(field).toString());
          result.addToRows(builder.get());
        }
      }
      return result;
    }

    public static List<IcebergPartitionSpec> loadPartitionSpecByIceberg(
        TableMetadata metadata) throws TableLoadingException {
      List<IcebergPartitionSpec> ret = new ArrayList<>();
      for (PartitionSpec spec : metadata.specs()) {
        List<IcebergPartitionField> fields = new ArrayList<>();;
        for (PartitionField field : spec.fields()) {
          fields.add(new IcebergPartitionField(field.sourceId(), field.fieldId(),
              field.name(), IcebergUtil.getPartitionTransform(field)));
        }
        ret.add(new IcebergPartitionSpec(spec.specId(), fields));
      }
      return ret;
    }
  }
}
