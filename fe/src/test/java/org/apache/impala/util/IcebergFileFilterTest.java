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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

  private void checkNoOp(GroupedContentFiles contentFiles, int fileSizeThreshold) {
    IcebergOptimizeFileFilter.FileFilteringResult result =
        IcebergOptimizeFileFilter.filterFilesBySize(
            new IcebergOptimizeFileFilter.FilterArgs(contentFiles, fileSizeThreshold));
    assertTrue(result.isNoOp());
    assertTrue(result.getSelectedFilesWithoutDeletes().isEmpty());
  }

  private void checkFiltering(GroupedContentFiles contentFiles, int fileSizeThreshold,
      boolean expectedAllFilesSelected, Set<String> expectedPaths) {
    IcebergOptimizeFileFilter.FileFilteringResult result =
        IcebergOptimizeFileFilter.filterFilesBySize(
            new IcebergOptimizeFileFilter.FilterArgs(contentFiles, fileSizeThreshold));
    assertFalse(result.isNoOp());
    assertEquals(expectedAllFilesSelected, result.allFilesSelected());
    Set<String> selectedPaths = new HashSet<>();
    for (DataFile df : result.getSelectedFilesWithoutDeletes()) {
      selectedPaths.add(df.location());
    }
    assertEquals(expectedPaths, selectedPaths);
  }

  // The paths of the data files that the single-partition tests below build, i.e. the
  // full selection when the size threshold narrows nothing.
  private static Set<String> allPathsInPartitionA1() {
    Set<String> paths = new HashSet<>();
    for (int fileSize : fileSizes) paths.add("a=1/size_" + fileSize + ".parquet");
    return paths;
  }

  @Test
  public void testUnpartitioned() {
    GroupedContentFiles contentFiles = new GroupedContentFiles();

    for (long fileSize : fileSizes) {
      DataFile df = buildDataFile(fileSize, "a=1", "size_" + fileSize);
      contentFiles.dataFilesWithoutDeletes.add(df);
    }

    checkNoOp(contentFiles, 0);
    checkNoOp(contentFiles, 2);

    Set<String> filePaths = new HashSet<>();
    Collections.addAll(filePaths,
        "a=1/size_2.parquet", "a=1/size_10.parquet", "a=1/size_11.parquet");
    checkFiltering(contentFiles, 100, false, filePaths);
    // A threshold above every file size selects all of them.
    checkFiltering(contentFiles, 500, true, allPathsInPartitionA1());
  }

  /**
   * Without a FILE_SIZE_THRESHOLD_MB clause the filter selects every data file without
   * deletes. It has to work on its own copy and not on the caller's list, which the
   * caller still needs in order to build the scoped file store and collect the paths of
   * the replaced files.
   */
  @Test
  public void testNoThreshold() {
    GroupedContentFiles contentFiles = new GroupedContentFiles();
    for (long fileSize : fileSizes) {
      contentFiles.dataFilesWithoutDeletes.add(
          buildDataFile(fileSize, "a=1", "size_" + fileSize));
    }

    checkFiltering(contentFiles, -1, true, allPathsInPartitionA1());
    assertEquals(fileSizes.size(), contentFiles.dataFilesWithoutDeletes.size());

    // A data file with deletes does not change the verdict: every data file without
    // deletes is still selected.
    contentFiles.dataFilesWithDeletes.add(buildDataFile(10, "a=1", "d10"));
    checkFiltering(contentFiles, -1, true, allPathsInPartitionA1());
    assertEquals(fileSizes.size(), contentFiles.dataFilesWithoutDeletes.size());
    assertEquals(1, contentFiles.dataFilesWithDeletes.size());

    // An empty table is a no-op even without a threshold.
    checkNoOp(new GroupedContentFiles(), -1);
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
    checkNoOp(contentFiles, 5);

    // Add data files with deletes to check if they are considered in the 1 file per
    // partition rule.
    contentFiles.dataFilesWithDeletes.add(buildDataFile(10, "a=1", "d10"));
    contentFiles.dataFilesWithDeletes.add(buildDataFile(100, "a=4", "d100"));

    // No data files without deletes were selected, but there are data files with deletes,
    // so this is not a NOOP. Delete files should be merged with corresponding data files.
    Set<String> filePaths = new HashSet<>();
    checkFiltering(contentFiles, 0, false, filePaths);

    // Data files without deletes selected: a=0/size_2, a=1/size_10, a=2/size_11.
    // There is another data file with deletes in a=1 (d10), so they will be merged, but
    // the other 2 files will not be rewritten since they are the only data files in their
    // partition.
    filePaths.add("a=1/size_10.parquet");
    checkFiltering(contentFiles, 12, false, filePaths);

    // a=2/size_11 and a=3/size_100 are the only files in their partitions, so they will
    // not be selected.
    Collections.addAll(filePaths, "a=0/size_2.parquet", "a=0/size_100.parquet",
        "a=1/size_101.parquet", "a=4/size_120.parquet");
    checkFiltering(contentFiles, 200, false, filePaths);

    // a=3/size_100 is the only file in the a=3 partition, so it will not be selected.
    Collections.addAll(filePaths, "a=0/size_222.parquet", "a=1/size_250.parquet",
        "a=2/size_11.parquet", "a=2/size_200.parquet");
    checkFiltering(contentFiles, 500, false, filePaths);
  }
}