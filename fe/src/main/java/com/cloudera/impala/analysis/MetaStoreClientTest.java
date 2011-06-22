// Copyright (c) 2011 Cloudera, Inc. All rights reserved. 

package com.cloudera.impala.analysis;

import java.io.*;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

public class MetaStoreClientTest {
  public static void main(String[] args) throws Exception {
    HiveConf hiveConf = new HiveConf(MetaStoreClientTest.class);
    hiveConf.set("javax.jdo.option.ConnectionURL",
        "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true");
    hiveConf.set("javax.jdo.option.ConnectionDriverName",
        "com.mysql.jdbc.Driver");
    hiveConf.set("javax.jdo.option.ConnectionUserName", "marcel");
    hiveConf.set("javax.jdo.option.ConnectionPassword", "marcel");
    hiveConf.set("hive.metastore.local", "true");
    hiveConf.set("hive.metastore.warehouse.dir", "/home/marcel/hive/warehouse");
    hiveConf.set("hive.metastore.rawstore.impl", "org.apache.hadoop.hive.metastore.ObjectStore");
    hiveConf.set("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.jdo.JDOPersistenceManagerFactory");

    hiveConf.set("datanucleus.autoStartMechanismMode", "checked");
    hiveConf.set("datanucleus.identifierFactory", "datanucleus");
    hiveConf.set("datanucleus.transactionIsolation", "read-committed");
    hiveConf.set("datanucleus.validateTables", "false");
    hiveConf.set("javax.jdo.option.DetachAllOnCommit", "true");
    hiveConf.set("javax.jdo.option.NonTransactionalRead", "true");
    hiveConf.set("datanucleus.validateConstraints", "false");
    hiveConf.set("datanucleus.validateColumns", "false");
    hiveConf.set("datanucleus.cache.level2", "false");
    hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
    hiveConf.set("datanucleus.cache.level2.type", "SOFT");
    hiveConf.set("datanucleus.autoCreateSchema", "true");
    hiveConf.set("datanucleus.storeManagerType", "rdbms");
    hiveConf.set("datanucleus.connectionPoolingType", "DBCP");

    HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
    List<String> dbs = client.getDatabases("*");
    for (String db: dbs) {
      System.err.println("db=" + db);
      List<String> tbls = client.getTables(db, "*");
      for (String tbl: tbls) {
        System.err.println("tbl=" + tbl);
        List<FieldSchema> fieldSchemas = client.getFields(db, tbl);
        for (FieldSchema s: fieldSchemas) {
          System.err.println(s.toString());
        }
        List<Index> indices = client.listIndexes(db, tbl, (short)16);
        for (Index idx: indices) {
          System.err.println(idx.toString());
        }
        List<Partition> partitions = client.listPartitions(db, tbl, (short)64);
        for (Partition p: partitions) {
          System.err.println(p.toString());
        }
        StorageDescriptor sd = client.getTable(db, tbl).getSd();
        System.err.println(sd.toString());
      }
    }
  }
}
