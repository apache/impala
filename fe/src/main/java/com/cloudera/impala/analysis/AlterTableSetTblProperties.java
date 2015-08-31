// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.analysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.SchemaParseException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.*;
import com.cloudera.impala.util.AvroSchemaParser;
import com.cloudera.impala.util.AvroSchemaUtils;
import com.cloudera.impala.util.MetaStoreUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
* Represents an ALTER TABLE SET [PARTITION ('k1'='a', 'k2'='b'...)]
* TBLPROPERTIES|SERDEPROPERTIES ('p1'='v1', ...) statement.
*/
public class AlterTableSetTblProperties extends AlterTableSetStmt {
  private final TTablePropertyType targetProperty_;
  private final HashMap<String, String> tblProperties_;

  public AlterTableSetTblProperties(TableName tableName, PartitionSpec partitionSpec,
      TTablePropertyType targetProperty, HashMap<String, String> tblProperties) {
    super(tableName, partitionSpec);
    Preconditions.checkNotNull(tblProperties);
    Preconditions.checkNotNull(targetProperty);
    targetProperty_ = targetProperty;
    tblProperties_ = tblProperties;
    CreateTableStmt.unescapeProperties(tblProperties_);
  }

  public HashMap<String, String> getTblProperties() { return tblProperties_; }

  @Override
  public TAlterTableParams toThrift() {
   TAlterTableParams params = super.toThrift();
   params.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
   TAlterTableSetTblPropertiesParams tblPropertyParams =
       new TAlterTableSetTblPropertiesParams();
   tblPropertyParams.setTarget(targetProperty_);
   tblPropertyParams.setProperties(tblProperties_);
   if (partitionSpec_ != null) {
     tblPropertyParams.setPartition_spec(partitionSpec_.toThrift());
   }
   params.setSet_tbl_properties_params(tblPropertyParams);
   return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    MetaStoreUtil.checkShortPropertyMap("Property", tblProperties_);

    // Check avro schema when it is set in avro.schema.url or avro.schema.literal to
    // avoid potential metadata corruption (see IMPALA-2042).
    // If both properties are set then only check avro.schema.literal and ignore
    // avro.schema.url.
    if (tblProperties_.containsKey(
            AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()) ||
        tblProperties_.containsKey(
            AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName())) {
      analyzeAvroSchema(analyzer);
    }
  }

  /**
   * Check that Avro schema provided in avro.schema.url or avro.schema.literal is valid
   * Json and contains only supported Impala types. If both properties are set, then
   * avro.schema.url is ignored.
   */
  private void analyzeAvroSchema(Analyzer analyzer)
      throws AnalysisException {
    List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
    schemaSearchLocations.add(tblProperties_);

    String avroSchema = AvroSchemaUtils.getAvroSchema(schemaSearchLocations);
    avroSchema = Strings.nullToEmpty(avroSchema);
    if (avroSchema.isEmpty()) {
      throw new AnalysisException("Avro schema is null or empty: " +
          table_.getFullName());
    }

    // Check if the schema is valid and is supported by Impala
    try {
      AvroSchemaParser.parse(avroSchema);
    } catch (SchemaParseException e) {
      throw new AnalysisException(String.format(
          "Error parsing Avro schema for table '%s': %s", table_.getFullName(),
          e.getMessage()));
    }
  }
}
