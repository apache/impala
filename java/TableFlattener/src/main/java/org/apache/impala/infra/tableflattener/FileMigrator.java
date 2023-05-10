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
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class FileMigrator {

  // Migrates data from a nested "src" to a flat "dst".
  public void migrate(Dataset<GenericRecord> src, FlattenedSchema dst) {
    dst.open();
    try {
      for (GenericRecord record : src.newReader()) {
        writeRecord(record, dst);
      }
    } finally {
      dst.close();
    }
  }

  private void writeRecord(GenericRecord srcRecord, FlattenedSchema dstDataset) {
    Record dstRecord = createRecord(null, dstDataset);
    writeRecordFields(srcRecord, dstRecord, dstDataset, "");
    dstDataset.write(dstRecord);
  }

  private Record createRecord(Long dstParentId, FlattenedSchema dstDataset) {
    Record record = new Record(dstDataset.getDataset().getDescriptor().getSchema());
    if (dstDataset.getParentIdField() != null) {
      Preconditions.checkNotNull(dstParentId);
      record.put(dstDataset.getParentIdField().name(), dstParentId);
    }
    Field idField = record.getSchema().getField(dstDataset.getIdFieldName());
    if (idField != null) record.put(idField.name(), dstDataset.nextId());
    return record;
  }

  private void writeRecordFields(GenericRecord srcRecord, Record dstRecord,
                                 FlattenedSchema dstDataset, String fieldNamePrefix) {
    for (Field field : srcRecord.getSchema().getFields()) {
      Object value;
      if (SchemaUtil.recordHasField(srcRecord, field.name())) {
        value = srcRecord.get(field.name());
      } else {
        Preconditions.checkNotNull(field.defaultVal());
        value = GenericData.get().getDefaultValue(field);
      }
      writeValue(value, field.schema(), field.name(), dstRecord, dstDataset,
          fieldNamePrefix);
    }
  }

  private void writeValue(Object srcValue, Schema srcSchema, String srcFieldName,
                          Record dstRecord, FlattenedSchema dstDataset, String fieldNamePrefix) {
    String dstFieldName = fieldNamePrefix + (srcFieldName == null ?
        dstDataset.getCollectionValueFieldName() : srcFieldName);
    if (!SchemaUtil.schemaHasNesting(srcSchema)) {
      dstRecord.put(dstFieldName, srcValue);
      return;
    }

    if (SchemaUtil.isNullable(srcSchema)) {
      dstRecord.put(dstDataset.getIsNullFieldName(dstFieldName), (srcValue == null));
      if (srcValue == null) return;
      if (srcSchema.getType() == Type.UNION) {
        srcSchema = srcSchema.getTypes().get(
            GenericData.get().resolveUnion(srcSchema, srcValue));
      }
    }

    if (!SchemaUtil.requiresChildDataset(srcSchema)) {
      writeRecordFields((GenericRecord)srcValue, dstRecord, dstDataset,
          fieldNamePrefix
              + (srcFieldName == null ?
                  dstDataset.getCollectionValueFieldName() : srcFieldName)
              + dstDataset.getNameSeparator());
      return;
    }

    Long dstParentId = (Long)dstRecord.get(dstDataset.getIdFieldName());
    Preconditions.checkNotNull(dstParentId);
    FlattenedSchema childDataset = (srcFieldName == null) ?
        dstDataset.getChildOfCollection() : dstDataset.getChildOfRecord(srcFieldName);
    if (srcSchema.getType() == Type.ARRAY) {
      writeArray((List) srcValue, srcSchema.getElementType(), dstParentId, childDataset);
    } else {
      Preconditions.checkState(srcSchema.getType() == Type.MAP);
      writeMap((Map) srcValue, srcSchema.getValueType(), dstParentId, childDataset);
    }
  }

  private void writeArray(List srcValues, Schema srcSchema, Long dstParentId,
      FlattenedSchema dstDataset) {
    for (ListIterator it = srcValues.listIterator(); it.hasNext(); ) {
      Object value = it.next();
      Record record = createRecord(dstParentId, dstDataset);
      record.put(dstDataset.getArrayIdxFieldName(), (long)it.previousIndex());
      writeValue(value, srcSchema, null, record, dstDataset, "");
      dstDataset.write(record);
    }
  }

  @SuppressWarnings("unchecked")
  private void writeMap(Map srcValues, Schema srcSchema, Long dstParentId,
      FlattenedSchema dstDataset) {
    for (Entry<String, Object> entry : (Set<Entry<String, Object>>)srcValues.entrySet()) {
      Record record = createRecord(dstParentId, dstDataset);
      record.put(dstDataset.getMapKeyFieldName(), entry.getKey());
      writeValue(entry.getValue(), srcSchema, null, record, dstDataset, "");
      dstDataset.write(record);
    }
  }
}
