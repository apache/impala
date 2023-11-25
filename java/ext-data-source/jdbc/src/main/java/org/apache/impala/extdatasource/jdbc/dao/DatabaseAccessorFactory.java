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

package org.apache.impala.extdatasource.jdbc.dao;


import org.apache.hadoop.conf.Configuration;
import org.apache.impala.extdatasource.jdbc.conf.DatabaseType;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;

/**
 * Factory for creating the correct DatabaseAccessor class for the job
 */
public class DatabaseAccessorFactory {

  private DatabaseAccessorFactory() {
  }

  public static DatabaseAccessor getAccessor(DatabaseType dbType) {
    DatabaseAccessor accessor;
    switch (dbType) {
      case MYSQL:
        accessor = new MySqlDatabaseAccessor();
        break;

      case JETHRO_DATA:
        accessor = new JethroDatabaseAccessor();
        break;

      case POSTGRES:
        accessor = new PostgresDatabaseAccessor();
        break;

      case ORACLE:
        accessor = new OracleDatabaseAccessor();
        break;

      case MSSQL:
        accessor = new MsSqlDatabaseAccessor();
        break;

      case DB2:
        accessor = new DB2DatabaseAccessor();
        break;

      case IMPALA:
        accessor = new ImpalaDatabaseAccessor();
        break;

      default:
        accessor = new GenericJdbcDatabaseAccessor();
        break;
    }

    return accessor;
  }


  public static DatabaseAccessor getAccessor(Configuration conf) {
    DatabaseType dbType = DatabaseType.valueOf(
        conf.get(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()).toUpperCase());
    return getAccessor(dbType);
  }

}
