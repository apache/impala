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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * This class standardizes the query string building process. Each inner class is
 * responsible for a specific query type, while the outer class can be used to
 * instantiate the inner classes. The methods of the inner classes are supposed to be
 * chainable.
 */
public class QueryStringBuilder {

  public static String createTmpTableName(String dbName, String tableName) {
    return dbName + "." + tableName + "_tmp_" + UUID.randomUUID().toString()
        .substring(0, 8);
  }

  private static String appendProps(StringBuilder builder, Map<String, String> props_) {
    builder.append(" TBLPROPERTIES (");
    for (Entry<String, String> prop : props_.entrySet()) {
      builder.append("'").append(prop.getKey()).append("'='").append(prop.getValue())
          .append("',");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(")");
    return builder.toString();
  }

  public static class Create {
    private String tableName_;
    private boolean external_;
    private boolean like_;
    private String likeFileFormat_;
    private String likeLocation_;
    private String storedAsFileFormat_;
    private String tableLocation_;
    private final Map<String, String> props_= Maps.newHashMap();

    public Create() {}

    public static Create builder() {
      return new Create();
    }

    public Create table(String tableName, boolean external) {
      tableName_ = tableName;
      external_ = external;
      return this;
    }

    public Create like(String fileFormat, String location) {
      like_ = true;
      likeFileFormat_ = fileFormat.toUpperCase();
      likeLocation_ = location;
      return this;
    }

    public Create storedAs(String fileFormat) {
      storedAsFileFormat_ = fileFormat.toUpperCase();
      return this;
    }

    public Create tableLocation(String location) {
      tableLocation_ = location;
      return this;
    }

    public Create property(String k, String v) {
      props_.put(k, v);
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      if (!external_) {
        builder.append("CREATE TABLE ");
      } else {
        builder.append("CREATE EXTERNAL TABLE ");
      }
      builder.append(tableName_ + " ");
      if (like_) {
        builder.append("LIKE " + likeFileFormat_ + " '" + likeLocation_ + "' ");
      }
      builder.append("STORED AS " + storedAsFileFormat_ + " ");
      builder.append("LOCATION '" + tableLocation_ + "'");
      if (props_.isEmpty()) {
        return builder.toString();
      }
      return appendProps(builder, props_);
    }
  }

  public static class Insert {
    private String tableName_;
    private boolean overwrite_;
    private Select select_;

    public static Insert builder() {
      return new Insert();
    }

    public Insert table(String tableName) {
      tableName_ = tableName + " ";
      return this;
    }

    public Insert overwrite(boolean overwrite) {
      overwrite_ = overwrite;
      return this;
    }

    public Insert select(Select select) {
      select_ = select;
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      if (overwrite_) {
        builder.append("INSERT OVERWRITE ");
      } else {
        builder.append("INSERT INTO ");
      }
      builder.append(tableName_);
      builder.append(select_.build());
      return builder.toString();
    }
  }

  public static class Select {
    private String selectList_;
    private String tableName_;

    public static Select builder() {
      return new Select();
    }

    public Select selectList(String selectList) {
      selectList_ = selectList;
      return this;
    }

    public Select from(String tableName) {
      tableName_ = tableName;
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      builder.append("SELECT " + selectList_ + " ");
      builder.append("FROM " + tableName_ + " ");
      return builder.toString();
    }
  }

  public static class Drop {
    private String tableName_;

    public static Drop builder() {
      return new Drop();
    }

    public Drop table(String tableName) {
      tableName_ = tableName;
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      builder.append("DROP TABLE ");
      builder.append(tableName_);
      return builder.toString();
    }
  }

  public static class SetTblProps {
    private String tableName_;
    private final Map<String, String> props_ = Maps.newHashMap();;

    public static SetTblProps builder() {
      return new SetTblProps();
    }

    public SetTblProps table(String tableName) {
      tableName_ = tableName;
      return this;
    }

    public SetTblProps property(String k, String v) {
      props_.put(k, v);
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      builder.append("ALTER TABLE ");
      builder.append(tableName_);
      builder.append(" SET");
      Preconditions.checkState(props_.size() >= 1);
      return appendProps(builder, props_);
    }
  }

  public static class Rename {

    private String sourceTableName_;
    private String targetTableName_;

    public static Rename builder() {
      return new Rename();
    }

    public Rename source(String tableName) {
      sourceTableName_ = tableName;
      return this;
    }

    public Rename target(String tableName) {
      targetTableName_ = tableName;
      return this;
    }

    public String build() {
      StringBuilder builder = new StringBuilder();
      builder.append("ALTER TABLE ");
      builder.append(sourceTableName_);
      builder.append(" RENAME TO ");
      builder.append(targetTableName_);
      return builder.toString();
    }
  }

  public static class Refresh {
    private String tableName_;

    public static Refresh builder() {
      return new Refresh();
    }

    public Refresh table(String tableName) {
      tableName_ = tableName;
      return this;
    }

    public String build() {
      return "REFRESH " + tableName_;
    }
  }

  public static class Invalidate {
    private String tableName_;

    public static Invalidate builder() { return new Invalidate(); }

    public Invalidate table(String tableName) {
      tableName_ = tableName;
      return this;
    }

    public String build() {
      return "INVALIDATE METADATA " + tableName_;
    }
  }
}
