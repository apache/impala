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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.util.CatalogBlacklistUtils;

/**
 * Represents a CREATE DATABASE statement
 */
public class CreateDbStmt extends StatementBase {
  private final String dbName_;
  private final HdfsUri location_;
  private final HdfsUri managedLocation_;
  private final String comment_;
  private final boolean ifNotExists_;
  // Database owner. Set during analysis.
  private String owner_;

  // Server name needed for privileges. Set during analysis.
  private String serverName_;

  /**
   * Creates a database with the given name.
   */
  public CreateDbStmt(String dbName) {
    this(dbName, null, null, null, false);
  }

  /**
   * Creates a database with the given name, comment, and HDFS table storage location.
   * New tables created in the database inherit the location property for their default
   * storage location. Create database will throw an error if the database already exists
   * unless the ifNotExists is true.
   */
  public CreateDbStmt(String dbName, String comment, HdfsUri location,
       HdfsUri managedlocation, boolean ifNotExists) {
    this.dbName_ = dbName;
    this.comment_ = comment;
    this.location_ = location;
    this.managedLocation_ = managedlocation;
    this.ifNotExists_ = ifNotExists;
  }

  public String getComment() { return comment_; }
  public String getDb() { return dbName_; }
  public boolean getIfNotExists() { return ifNotExists_; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("CREATE DATABASE ");
    if (ifNotExists_) sb.append("IF NOT EXISTS");
    sb.append(dbName_);
    if (comment_ != null) sb.append(" COMMENT '" + comment_ + "'");
    if (location_ != null) sb.append(" LOCATION '" + location_ + "'");
    if (managedLocation_ != null) {
      sb.append(" MANAGEDLOCATION '" + managedLocation_ + "'");
    }
    return sb.toString();
  }

  public TCreateDbParams toThrift() {
    TCreateDbParams params = new TCreateDbParams();
    params.setDb(getDb());
    params.setComment(getComment());
    params.setLocation(location_ == null ? null : location_.toString());
    params.setManaged_location(
        managedLocation_ == null ? null : managedLocation_.toString());
    params.setIf_not_exists(getIfNotExists());
    params.setOwner(getOwner());
    params.setServer_name(serverName_);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Check whether the db name meets the Metastore's requirements.
    if (!MetastoreShim.validateName(dbName_)) {
      throw new AnalysisException("Invalid database name: " + dbName_);
    }
    CatalogBlacklistUtils.verifyDbName(dbName_);

    // Note: It is possible that a database with the same name was created external to
    // this Impala instance. If that happens, the caller will not get an
    // AnalysisException when creating the database, they will get a Hive
    // AlreadyExistsException once the request has been sent to the metastore.
    FeDb db = analyzer.getDb(getDb(), Privilege.CREATE, false);
    if (db != null && !ifNotExists_) {
      throw new AnalysisException(Analyzer.DB_ALREADY_EXISTS_ERROR_MSG + getDb());
    }

    if (location_ != null) {
      location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }
    if (managedLocation_ != null) {
      managedLocation_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }
    owner_ = analyzer.getUserShortName();
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();
  }

  /**
   * Can only be called after analysis, returns the owner of this database (the user from
   * the current session).
   */
  public String getOwner() { return Preconditions.checkNotNull(owner_); }
}
