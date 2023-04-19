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

package org.apache.impala.infra.tableflattener;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class SchemaFlattener {

  // The dir to write the flat datasets to. The dir should either not exist or be
  // empty. The URI can either point to a local dir or an HDFS dir.
  URI outputDir_;

  public SchemaFlattener(URI outputDir) { outputDir_ = outputDir; }

  // Creates a flattened schema but does not migrate any data.
  public FlattenedSchema flatten(Schema srcSchema) {
    Preconditions.checkState(srcSchema.getType() == Type.RECORD);
    FlattenedSchema dstDataset = new FlattenedSchema(srcSchema.getName());
    LinkedList<Field> fields = Lists.newLinkedList();
    addRecordFields(srcSchema, dstDataset, fields, "");
    finishCreatingDataset(fields, dstDataset);
    return dstDataset;
  }

  private void addRecordFields(Schema srcSchema, FlattenedSchema dstDataset,
      LinkedList<Field> dstSchemaFields, String fieldNamePrefix) {
    Preconditions.checkState(srcSchema.getType() == Type.RECORD);
    for (Field field : srcSchema.getFields()) {
      Schema fieldSchema = field.schema();
      if (SchemaUtil.isSimpleType(fieldSchema)) {
        dstSchemaFields.add(SchemaUtil.createField(fieldNamePrefix + field.name(),
            fieldSchema, field.doc(), field.defaultVal()));
        continue;
      }
      if (SchemaUtil.isNullable(fieldSchema)) {
        dstSchemaFields.add(SchemaUtil.createField(
            fieldNamePrefix + dstDataset.getIsNullFieldName(field.name()), Type.BOOLEAN));
        fieldSchema = SchemaUtil.reduceUnionToNonNull(fieldSchema);
      }
      if (SchemaUtil.requiresChildDataset(fieldSchema)) {
        createChildDataset(dstDataset.getChildOfRecordName(field.name()), fieldSchema,
            dstSchemaFields, dstDataset);
      } else {
        addRecordFields(fieldSchema, dstDataset, dstSchemaFields,
            fieldNamePrefix + field.name() + dstDataset.getNameSeparator());
      }
    }
  }

  private void createChildDataset(String name, Schema srcSchema,
      LinkedList<Field> parentFields, FlattenedSchema parentDataset) {
    // Ensure that the parent schema has an id field so the child can reference the
    // parent. A single id field is sufficient.
    if (parentFields.isEmpty()
        || !parentFields.getFirst().name().equals(parentDataset.getIdFieldName())) {
      parentFields.addFirst(SchemaUtil.createField(
          parentDataset.getIdFieldName(), Type.LONG));
    }
    FlattenedSchema childDataset = new FlattenedSchema(name, parentDataset);
    LinkedList<Field> fields = Lists.newLinkedList();
    String parentIdFieldName = parentDataset.getName() + childDataset.getNameSeparator()
        + childDataset.getIdFieldName();
    Field parentIdField = SchemaUtil.createField(parentIdFieldName, Type.LONG);
    childDataset.setParentIdField(parentIdField);
    fields.add(parentIdField);
    Schema valueSchema;
    if (srcSchema.getType() == Type.ARRAY) {
      fields.add(
          SchemaUtil.createField(childDataset.getArrayIdxFieldName(), Type.LONG));
      valueSchema = srcSchema.getElementType();
    } else {
      Preconditions.checkState(srcSchema.getType() == Type.MAP);
      fields.add(
          SchemaUtil.createField(childDataset.getMapKeyFieldName(), Type.STRING));
      valueSchema = srcSchema.getValueType();
    }

    if (SchemaUtil.isSimpleType(valueSchema)) {
      fields.add(SchemaUtil.createField(
          childDataset.getCollectionValueFieldName(), valueSchema));
    } else {
      if (SchemaUtil.isNullable(valueSchema)) {
        fields.add(SchemaUtil.createField(childDataset.getIsNullFieldName(
            childDataset.getCollectionValueFieldName()), Type.BOOLEAN));
        valueSchema = SchemaUtil.reduceUnionToNonNull(valueSchema);
      }
      if (SchemaUtil.requiresChildDataset(valueSchema)) {
        createChildDataset(childDataset.getChildOfCollectionName(), valueSchema, fields,
            childDataset);
      } else {
        addRecordFields(valueSchema, childDataset, fields,
            childDataset.getCollectionValueFieldName() + childDataset.getNameSeparator());
      }
    }

    finishCreatingDataset(fields, childDataset);
  }

  private void finishCreatingDataset(List<Field> fields, FlattenedSchema dataset) {
    Schema childSchema = Schema.createRecord(dataset.getName(), null, null, false);
    for (Field field : fields) {
      Preconditions.checkState(!SchemaUtil.schemaHasNesting(field.schema()));
    }
    childSchema.setFields(fields);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .format(Formats.PARQUET)
        .schema(childSchema)
        .build();
    dataset.setDataset((Dataset<GenericRecord>)Datasets.create(
        "dataset:" + createDir(dataset.getName()), descriptor));
  }

  private URI createDir(String name) {
    try {
      switch (outputDir_.getScheme().toUpperCase()) {
        case "FILE": {
          Path datasetPath = Paths.get(outputDir_).resolve(name);
          datasetPath.toFile().mkdirs();
          return datasetPath.toUri();
        }
        case "HDFS": {
          org.apache.hadoop.fs.Path outputDirPath
              = new org.apache.hadoop.fs.Path(outputDir_);
          org.apache.hadoop.fs.Path datasetPath
              = new org.apache.hadoop.fs.Path(outputDirPath, name);
          outputDirPath.getFileSystem(new Configuration()).mkdirs(datasetPath);
          return datasetPath.toUri();
        }
        default:
          throw new NotImplementedException(String.format(
              "Unexpected output dir scheme: %s", outputDir_.getScheme()));
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
