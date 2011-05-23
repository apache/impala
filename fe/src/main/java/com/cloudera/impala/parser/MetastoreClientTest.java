package com.cloudera.impala.parser;

import java.io.*;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

public class MetastoreClientTest {
  public static void main(String[] args) throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set("javax.jdo.option.ConnectionURL",
        "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true");
    hiveConf.set("javax.jdo.option.ConnectionDriverName",
        "com.mysql.jdbc.Driver");
    hiveConf.set("javax.jdo.option.ConnectionUserName", "marcel");
    hiveConf.set("javax.jdo.option.ConnectionPassword", "marcel");
    hiveConf.set("hive.metastore.local", "true");
    hiveConf.set("hive.metastore.warehouse.dir", "/home/marcel/hive/warehouse");
    hiveConf.set("hive.metastore.rawstore.impl", "org.apache.hadoop.hive.metastore.ObjectStore");

    HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
    List<String> dbs = client.getDatabases("*");
    for (String db: dbs) {
        System.err.println("db=" + db);
        List<String> tbls = client.getTables(db, "*");
        for (String tbl: tbls) {
            System.err.println("tbl=" + tbl);
        }
    }
  }
}
