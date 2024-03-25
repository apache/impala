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

package org.apache.impala.util;

import com.google.common.base.Preconditions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.thrift.TIcebergOptimizationMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IcebergFileFilterTest {

  private final static List<Integer> fileSizes =
      Arrays.asList(2, 10, 11, 100, 101, 200, 222, 250);

  private static PartitionSpec partitionSpec = buildPartitionSpec();

  private static PartitionSpec buildPartitionSpec() {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.StringType.get()),
            required(3, "data", Types.IntegerType.get()));
    return PartitionSpec.builderFor(schema).identity("a").build();
  }

  private static DataFile buildDataFile(long fileSizeInBytes, String path,
      String filename) {
    DataFile df = DataFiles.builder(partitionSpec)
        .withPath(path + "/" + filename + ".parquet")
        .withFileSizeInBytes(fileSizeInBytes)
        .withPartitionPath(path)
        .withRecordCount(1)
        .build();
    return df;
  }

  private void checkFiltering(GroupedContentFiles contentFiles, int fileSizeThreshold,
      TIcebergOptimizationMode expectedMode, Set<String> expectedPaths) {
    IcebergOptimizeFileFilter.FileFilteringResult result =
        IcebergOptimizeFileFilter.filterFilesBySize(
            new IcebergOptimizeFileFilter.FilterArgs(contentFiles, fileSizeThreshold));
    assertEquals(result.getSelectedFilesWithoutDeletes().size(),
        expectedPaths != null ? expectedPaths.size() : 0);
    assertEquals(result.getOptimizationMode(), expectedMode);
    if (expectedMode == TIcebergOptimizationMode.PARTIAL) {
      Preconditions.checkState(expectedPaths != null);
      for (DataFile df : result.getSelectedFilesWithoutDeletes()) {
        assertTrue(expectedPaths.contains(df.path()));
      }
    } else {
      Preconditions.checkState(expectedPaths == null);
      assertTrue(result.getSelectedFilesWithoutDeletes().isEmpty());
    }
  }

  @Test
  public void testUnpartitioned() {
    GroupedContentFiles contentFiles = new GroupedContentFiles();

    for (long fileSize : fileSizes) {
      DataFile df = buildDataFile(fileSize, "a=1", "size_" + fileSize);
      contentFiles.dataFilesWithoutDeletes.add(df);
    }

    checkFiltering(contentFiles, 0, TIcebergOptimizationMode.NOOP, null);
    checkFiltering(contentFiles, 2, TIcebergOptimizationMode.NOOP, null);

    Set<String> filePaths = new HashSet<>();
    Collections.addAll(filePaths,
        "a=1/size_2.parquet", "a=1/size_10.parquet", "a=1/size_11.parquet");
    checkFiltering(contentFiles, 100, TIcebergOptimizationMode.PARTIAL, filePaths);
    checkFiltering(contentFiles, 500, TIcebergOptimizationMode.REWRITE_ALL, null);
  }

  @Test
  public void testPartitioned() {
    GroupedContentFiles contentFiles = new GroupedContentFiles();

    for (int i = 0; i < fileSizes.size(); i++) {
      int size = fileSizes.get(i);
      DataFile df = buildDataFile(size, "a=" + i % 3, "size_" + size);
      contentFiles.dataFilesWithoutDeletes.add(df);
    }
    // Add data files that are alone in their partitions.
    contentFiles.dataFilesWithoutDeletes.add(buildDataFile(100, "a=3", "size_100"));
    contentFiles.dataFilesWithoutDeletes.add(buildDataFile(120, "a=4", "size_120"));

    /*
    Naming of the data files: size_[file_size](.parquet).
    The content of the table per partition so far:
    a=0: size_2, size_100, size_222
    a=1: size_10, size_101, size_250
    a=2: size_11, size_200
    a=3: size_100
    a=4: size_120
    */

    // Only a=0/size_2 meets the filtering criteria, but it is the only selected file from
    // the partition, so it will not be rewritten.
    checkFiltering(contentFiles, 5, TIcebergOptimizationMode.NOOP, null);

    // Add data files with deletes to check if they are considered in the 1 file per
    // partition rule.
    contentFiles.dataFilesWithDeletes.add(buildDataFile(10, "a=1", "d10"));
    contentFiles.dataFilesWithDeletes.add(buildDataFile(100, "a=4", "d100"));

    // No data files without deletes were selected, but there are data files with deletes,
    // so this is not a NOOP. Delete files should be merged with corresponding data files.
    Set<String> filePaths = new HashSet<>();
    checkFiltering(contentFiles, 0, TIcebergOptimizationMode.PARTIAL, filePaths);

    // Data files without deletes selected: a=0/size_2, a=1/size_10, a=2/size_11.
    // There is another data file with deletes in a=1 (d10), so they will be merged, but
    // the other 2 files will not be rewritten since they are the only data files in their
    // partition.
    filePaths.add("a=1/size_10.parquet");
    checkFiltering(contentFiles, 12, TIcebergOptimizationMode.PARTIAL, filePaths);

    // a=2/size_11 and a=3/size_100 are the only files in their partitions, so they will
    // not be selected.
    Collections.addAll(filePaths, "a=0/size_2.parquet", "a=0/size_100.parquet",
        "a=1/size_101.parquet", "a=4/size_120.parquet");
    checkFiltering(contentFiles, 200, TIcebergOptimizationMode.PARTIAL, filePaths);

    // a=3/size_100 is the only file in the a=3 partition, so it will not be selected.
    Collections.addAll(filePaths, "a=0/size_222.parquet", "a=1/size_250.parquet",
        "a=2/size_11.parquet", "a=2/size_200.parquet");
    checkFiltering(contentFiles, 500, TIcebergOptimizationMode.PARTIAL, filePaths);
  }
}