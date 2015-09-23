package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TStructField;
import com.cloudera.impala.thrift.TTypeNode;
import com.cloudera.impala.thrift.TTypeNodeType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Describes a STRUCT type. STRUCT types have a list of named struct fields.
 */
public class StructType extends Type {
  private final HashMap<String, StructField> fieldMap_ = Maps.newHashMap();
  private final ArrayList<StructField> fields_;

  public StructType(ArrayList<StructField> fields) {
    Preconditions.checkNotNull(fields);
    fields_ = fields;
    for (int i = 0; i < fields_.size(); ++i) {
      fields_.get(i).setPosition(i);
      fieldMap_.put(fields_.get(i).getName().toLowerCase(), fields_.get(i));
    }
  }

  public StructType() {
    fields_ = Lists.newArrayList();
  }

  @Override
  public String toSql(int depth) {
    if (depth >= MAX_NESTING_DEPTH) return "STRUCT<...>";
    ArrayList<String> fieldsSql = Lists.newArrayList();
    for (StructField f: fields_) fieldsSql.add(f.toSql(depth + 1));
    return String.format("STRUCT<%s>", Joiner.on(",").join(fieldsSql));
  }

  @Override
  protected String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    ArrayList<String> fieldsSql = Lists.newArrayList();
    for (StructField f: fields_) fieldsSql.add(f.prettyPrint(lpad + 2));
    return String.format("%sSTRUCT<\n%s\n%s>",
        leftPadding, Joiner.on(",\n").join(fieldsSql), leftPadding);
  }

  public void addField(StructField field) {
    field.setPosition(fields_.size());
    fields_.add(field);
    fieldMap_.put(field.getName().toLowerCase(), field);
  }

  public ArrayList<StructField> getFields() { return fields_; }

  public StructField getField(String fieldName) {
    return fieldMap_.get(fieldName.toLowerCase());
  }

  public void clearFields() {
    fields_.clear();
    fieldMap_.clear();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StructType)) return false;
    StructType otherStructType = (StructType) other;
    return otherStructType.getFields().equals(fields_);
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(fields_);
    Preconditions.checkNotNull(!fields_.isEmpty());
    node.setType(TTypeNodeType.STRUCT);
    node.setStruct_fields(new ArrayList<TStructField>());
    for (StructField field: fields_) {
      field.toThrift(container, node);
    }
  }
}
