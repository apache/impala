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

/**
 * This class standardizes the query string building process. At this point only used for
 * child query creation for Iceberg LOAD DATA INPATH queries. Each inner class is
 * responsible for a specific query type, while the outer class can be used to instantiate
 * the inner classes. The methods of the inner classes are supposed to be chainable.
 */
public class QueryStringBuilder {

  public static class Create {
    private String tableName_;
    private Boolean external_;
    private Boolean like_;
    private String likeFileFormat_;
    private String likeLocation_;
    private String storedAsFileFormat_;
    private String tableLocation_;

    public Create() {}

    public Create table(String tableName, Boolean external) {
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
      return builder.toString();
    }
  }

  public static class Insert {
    private String tableName_;
    private Boolean overwrite_;
    private Select select_;

    public Insert() {}

    public Insert table(String tableName) {
      tableName_ = tableName + " ";
      return this;
    }

    public Insert overwrite(Boolean overwrite) {
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

    public Select() {}

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

    public Drop() {}

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
}
