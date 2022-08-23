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

package org.apache.impala.testutil;

import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.util.PatternMatcher;

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
      try (Catalog catalog = CatalogServiceTestCatalog.create()) {
        for (FeDb database : catalog.getDbs(PatternMatcher.MATCHER_MATCH_ALL)) {
          for (String tableName : database.getAllTableNames()) {
            FeTable table = database.getTable(tableName);
            // Only do this for hdfs tables
            if (table == null || !(table instanceof HdfsTable)) {
              continue;
            }
            HdfsTable hdfsTable = (HdfsTable) table;

            // Write the output as <tablename>: <blockid1> <blockid2> <etc>
            writer.write(tableName + ":");
            Collection<? extends FeFsPartition> parts =
                FeCatalogUtils.loadAllPartitions(hdfsTable);
            for (FeFsPartition partition : parts) {
              List<FileDescriptor> fileDescriptors = partition.getFileDescriptors();
              for (FileDescriptor fd : fileDescriptors) {
                Path p = new Path(fd.getAbsolutePath(partition.getLocation()));

                // Use a deprecated API to get block ids
                DistributedFileSystem dfs =
                    (DistributedFileSystem) p.getFileSystem(hdfsConfig);
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
      }
    } finally {
      if (writer != null) writer.close();
    }
  }
}
