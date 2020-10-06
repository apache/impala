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
import com.google.common.collect.Maps;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;

import java.util.Map;

// This class contains information about how unnested datasets are related.
public class FlattenedSchema {

  // If a dataset has nesting, an id field will be created so that records in the child
  // dataset can reference the parent.
  private long referenceId;

  // If this dataset has a parent, the parentIdField_ indicates which field in this
  // dataset is the foreign key to the parent dataset. If this dataset does not have a
  // parent, this should be null??
  private Field parentIdField_;

  // The name of the data set, mainly used to find a child dataset.
  private String name_;
  private Map<String, FlattenedSchema> childrenByName_ = Maps.newHashMap();

  // The actual dataset object.
  private Dataset<GenericRecord> dataset_;
  private DatasetWriter<GenericRecord> datasetWriter_;

  private final String idFieldName_ = "id";

  public FlattenedSchema(String name) {
    name_ = name;
    referenceId = 0;
  }

  public FlattenedSchema(String name, FlattenedSchema parent) {
    this(name);
    parent.childrenByName_.put(name, this);
  }

  // Opens this dataset and all children for writing.
  public void open() {
    if (datasetWriter_ != null) return;
    datasetWriter_ = dataset_.newWriter();
    for (FlattenedSchema child : childrenByName_.values()) {
      child.open();
    }
  }

  // Write a record to this dataset.
  public void write(GenericRecord record) {
    Preconditions.checkNotNull(datasetWriter_, "open() must be called before writing");
    datasetWriter_.write(record);
  }

  // Close this dataset and all children.
  public void close() {
    if (datasetWriter_ == null) return;
    datasetWriter_.close();
    for (FlattenedSchema child : childrenByName_.values()) {
      child.close();
    }
    datasetWriter_ = null;
  }

  // Generates a new id for a new record in this dataset.
  public Long nextId() { return ++referenceId; }

  // Get the name to use when creating an id field.
  public String getIdFieldName() { return idFieldName_; }

  // Get the name of the field used to store the values of an array or map.
  public String getCollectionValueFieldName() { return "value"; }

  // Get the name of the field used to store the index of an array value..
  public String getArrayIdxFieldName() { return "idx"; }

  // Get the name of the field used to store the key of a map entry.
  public String getMapKeyFieldName() { return "key"; }

  // Get the name of a child dataset if this dataset corresponds to an array or map.
  public String getChildOfCollectionName() {
    return name_ + getNameSeparator() + "_values";
  }

  public String getIsNullFieldName(String fieldName) { return fieldName + "_is_null"; }

  // Get the separator when concatenating field or dataset names.
  public static String getNameSeparator() { return "_"; }

  // Get the child of this dataset if this dataset corresponds to an array or map.
  public FlattenedSchema getChildOfCollection() {
    FlattenedSchema child = childrenByName_.get(getChildOfCollectionName());
    Preconditions.checkNotNull(child);
    return child;
  }

  // Get the name of a child dataset if this dataset corresponds to a record.
  public String getChildOfRecordName(String parentFieldName) {
    return name_ + getNameSeparator() + parentFieldName;
  }

  // Get the child of this dataset if this dataset corresponds to a record.
  public FlattenedSchema getChildOfRecord(String parentFieldName) {
    FlattenedSchema child = childrenByName_.get(getChildOfRecordName(parentFieldName));
    Preconditions.checkNotNull(child);
    return child;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(dataset_.getDescriptor().getSchema().toString(true));
    for (FlattenedSchema child : childrenByName_.values()) {
      builder.append("\n\nChild: ")
          .append(child.name_)
          .append("\n")
          .append(child.toString());

    }
    return builder.toString();
  }

  public String getName() { return name_; }

  public Field getParentIdField() { return parentIdField_; }

  public void setParentIdField(Field parentIdField) {
    parentIdField_ = parentIdField;
  }

  public Dataset getDataset() { return dataset_; }

  public void setDataset(Dataset<GenericRecord> dataset) { dataset_ = dataset; }
}
