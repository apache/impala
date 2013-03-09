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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TCreateDbParams;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.metastore.api.Database;

/**
 * Represents a CREATE DATABASE statement
 */
public class CreateDbStmt extends ParseNodeBase {
  private final String dbName;
  private final String location;
  private final String comment;
  private final boolean ifNotExists;

  /**
   * Creates a database with the given name.
   */
  public CreateDbStmt(String dbName) {
    this(dbName, null, null, false);
  }

  /**
   * Creates a database with the given name, comment, and HDFS table storage location.
   * New tables created in the database inherit the location property for their default
   * storage location. Create database will throw an error if the database already exists
   * unless the ifNotExists is true.
   */
  public CreateDbStmt(String dbName, String comment, String location,
      boolean ifNotExists) {
    this.dbName = dbName;
    this.comment = comment;
    this.location = location;
    this.ifNotExists = ifNotExists;
  }

  public String getComment() {
    return comment;
  }

  public String getDb() {
    return dbName;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public String getLocation() {
    return location;
  }

  public String debugString() {
    return toSql();
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder("CREATE DATABASE");
    if (ifNotExists) {
      sb.append(" IF NOT EXISTS");
    }
    sb.append(dbName);

    if (comment != null) {
      sb.append(" COMMENT '" + comment + "'");
    }

    if (location != null) {
      sb.append(" LOCATION '" + location + "'");
    }
    return sb.toString();
  }

  public TCreateDbParams toThrift() {
    TCreateDbParams params = new TCreateDbParams();
    params.setDb(getDb());
    params.setComment(getComment());
    params.setLocation(getLocation());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Note: It is possible that a database with the same name was created external to
    // this Impala instance. If that happens, the caller will not get an
    // AnalysisException when creating the database, they will get a Hive
    // AlreadyExistsException.
    if (analyzer.getCatalog().getDb(getDb()) != null && !ifNotExists) {
      throw new AnalysisException("Database already exists: " + getDb());
    }
  }
}
