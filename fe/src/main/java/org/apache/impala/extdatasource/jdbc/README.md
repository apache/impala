This JDBC External Data Source library is implemented with "External Data Source" mechanism
to query the JDBC table from Impala server.

Following two source files consists Impala specific logic:
    JdbcDataSource.java
    util/QueryConditionUtil.java

Other source files, which add supports to access external database tables through JDBC
drivers, are replicated from Hive JDBC Storage Handler with some modifications:
(https://github.com/apache/hive/tree/master/jdbc-handler/src/main/java/org/apache/hive/storage/jdbc)
    conf/DatabaseType.java
        remove dbType for HIVE, DERBY, and METASTORE.
    conf/JdbcStorageConfig.java
        don't use org.apache.hadoop.hive.conf.Constants
    conf/JdbcStorageConfigManager.java
        add functions: convertMapToConfiguration(), getQueryToExecute(),
            getOrigQueryToExecute()
        remove functions: copyConfigurationToJob(), countNonNull(),
            getPasswordFromProperties(), copySecretsToJob(),
            convertPropertiesToConfiguration(), resolveMetadata(), getMetastoreDatabaseType(),
            getMetastoreConnectionURL(), getMetastoreDriver(), getMetastoreJdbcUser(),
            getMetastoreJdbcPasswd().
        modify functions: checkRequiredPropertiesAreDefined()
    dao/DB2DatabaseAccessor.java
        remove function constructQuery()
    dao/DatabaseAccessor.java
        remoce following APIs:
            getColumnNames(), getColumnTypes(), getRecordWriter(), getBounds(),
            needColumnQuote().
        remove following parameters for API getRecordIterator():
           'partitionColumn', 'lowerBound', and 'upperBound'.
    dao/DatabaseAccessorFactory.java
        remove dbType for HIVE, DERBY, and METASTORE.
    dao/GenericJdbcDatabaseAccessor.java
        add member variable: dataSourceCache
        remove member variable typeInfoTranslator
        remove functions: getColumnMetadata(), getColumnMetadata(), getColumnNames()
            getColumnTypes(), getColNamesFromRS(),  getColTypesFromRS(),
            getMetaDataQuery(), getRecordWriter(), constructQuery(),
            addBoundaryToQuery(), removeDbcpPrefix(), getFromProperties(), getBounds(),
            quote(), needColumnQuote(), getQualifiedTableName(), selectAllFromTable(),
            finalize().
        remove following parameters for API getRecordIterator():
           'partitionColumn', 'lowerBound', and 'upperBound'.
        Modify functions: close().
    dao/JdbcRecordIterator.java
        remove member variable accessor, remove parameter 'accessor' from ctor,
        remove functions: remove()
        modify functions: next(), close()
    dao/JethroDatabaseAccessor.java
        remove getMetaDataQuery()
    dao/MsSqlDatabaseAccessor.java
    dao/MySqlDatabaseAccessor.java
        remove function needColumnQuote()
    dao/OracleDatabaseAccessor.java
        remove function constructQuery()
    dao/PostgresDatabaseAccessor.java
    exception/JdbcDatabaseAccessException.java
        renamed from exception/HiveJdbcDatabaseAccessException.java

