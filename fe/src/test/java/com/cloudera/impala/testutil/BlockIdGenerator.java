// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.testutil;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileDescriptor;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;

/**
 * Utility to generate an output file with all the block ids for each table
 * currently in the metastore.  Having the block ids allows us to map hdfs
 * files to filesystem files. This is mostly a hack since hdfs does not
 * willingly expose this information.
 */
public class BlockIdGenerator {

  @SuppressWarnings("deprecation")
  public static void main(String[] args)
      throws Exception {

    if (args.length != 1) {
      throw new Exception("Invalid args: BlockIdGenerator <output_file>");
    }

    HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    File output = new File(args[0]);
    FileWriter writer = null;

    try {
      writer = new FileWriter(output);

      // Load all tables in the catalog
      Catalog catalog = CatalogServiceTestCatalog.create();
      for (Db database: catalog.getDbs(null)) {
        for (String tableName: database.getAllTableNames()) {
          Table table = database.getTable(tableName);
          // Only do this for hdfs tables
          if (table == null || !(table instanceof HdfsTable)) {
            continue;
          }
          HdfsTable hdfsTable = (HdfsTable)table;

          // Write the output as <tablename>: <blockid1> <blockid2> <etc>
          writer.write(tableName + ":");
          for (HdfsPartition partition: hdfsTable.getPartitions()) {
            // Ignore the default partition.
            if (partition.getId() ==
                    ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
              continue;
            }
            List<FileDescriptor> fileDescriptors = partition.getFileDescriptors();
            for (FileDescriptor fd : fileDescriptors) {
              Path p = new Path(partition.getLocation(), fd.getFileName());

              // Use a deprecated API to get block ids
              DistributedFileSystem dfs =
                  (DistributedFileSystem)p.getFileSystem(hdfsConfig);
              LocatedBlocks locations = dfs.getClient().getNamenode().getBlockLocations(
                  p.toUri().getPath(), 0, fd.getFileLength());

              for (LocatedBlock lb : locations.getLocatedBlocks()) {
                long id = lb.getBlock().getBlockId();
                writer.write(" " + id);
              }
            }
          }
          writer.write("\n");
        }
      }
    } finally {
      if (writer != null) writer.close();
    }
  }
}
