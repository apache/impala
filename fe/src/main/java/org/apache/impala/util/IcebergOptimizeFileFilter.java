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
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TIcebergOptimizationMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides file filtering for Iceberg files, based on file size. Used by
 * OptimizeStmt to filter small data files to compact.
 * It also determines the mode of optimization: full table compaction, partial compaction
 * or none (no-op).
 */
public class IcebergOptimizeFileFilter {

  public static class FileFilteringResult {
    // Defines if the operation is a partial or full table compaction or no-op.
    private final TIcebergOptimizationMode optimizationMode_;
    // Contains the selected data files without deletes to compact.
    private final List<DataFile> selectedFilesWithoutDeletes_;

    public FileFilteringResult(TIcebergOptimizationMode mode,
        List<DataFile> selectedFiles) {
      optimizationMode_ = mode;
      selectedFilesWithoutDeletes_ = selectedFiles;
    }

    public TIcebergOptimizationMode getOptimizationMode() {
      return optimizationMode_;
    }

    public List<DataFile> getSelectedFilesWithoutDeletes() {
      return selectedFilesWithoutDeletes_;
    }
  }


  public static class FilterArgs {
    // File size threshold in bytes. -1 indicates full table compaction with no filtering.
    private final long fileSizeThreshold_;
    private final GroupedContentFiles filesToFilter_;

    public FilterArgs(GroupedContentFiles filesToFilter, long fileSizeThreshold) {
      filesToFilter_ = filesToFilter;
      fileSizeThreshold_ = fileSizeThreshold;
    }
  }

  /**
   * Filters Iceberg data files without deletes based on file size. Files smaller than the
   * given threshold will be selected for rewriting. All delete files and data files with
   * deletes should be compacted regardless of the filter criteria.
   * If there is only 1 file per partition that qualifies for the selection criteria,
   * it should be excluded from the result, since rewriting it would be redundant.
   * @param args contains the file size threshold and the files to filter
   * @return the mode of the optimization and the selected data files without deletes in
   * case of PARTIAL mode
   */
  public static FileFilteringResult filterFilesBySize(FilterArgs args) {
    // Stores the selected data files without deletes.
    List<DataFile> selectedFiles = new ArrayList<>();

    // Select data files without deletes only if  the file size threshold is positive.
    // If the file size threshold is 0, no data files without deletes will be selected,
    // only the delete files will be merged. It would be unnecessary to iterate through
    // the file descriptors.
    if (args.fileSizeThreshold_ > 0) {
      // Stores the number of selected data files without deletes plus all data files with
      // deletes per partition and the list of file paths to the selected data files
      // without deletes. The key is the partition hash.
      Map<Integer, Pair<Integer, List<DataFile>>> filesPerPartition = new HashMap<>();
      // Add files from contentFiles.dataFilesWithoutDeletes filtered by file_size.
      // Only data files without deletes have to be filtered, all other files will be
      // rewritten.
      // Group file descriptors per partition; if there is only 1 file per partition,
      // do not rewrite it.
      for (DataFile dataFile : args.filesToFilter_.dataFilesWithoutDeletes) {
        if (dataFile.fileSizeInBytes() < args.fileSizeThreshold_) {
          int hashValue = dataFile.partition().hashCode();
          Pair<Integer, List<DataFile>> partition = filesPerPartition.computeIfAbsent(
              hashValue, k -> new Pair<>(0, new ArrayList<>()));
          partition.first += 1;
          partition.second.add(dataFile);
        }
      }
      // Also count data files with deletes when counting the files per partition. Since
      // all of them will be rewritten, we do not add the path, just the number.
      for (DataFile dataFile : args.filesToFilter_.dataFilesWithDeletes) {
        int hashValue = dataFile.partition().hashCode();
        if (filesPerPartition.get(hashValue) != null) {
          filesPerPartition.get(hashValue).first += 1;
        }
      }
      // If there are multiple data files in the partition, add all data files without
      // deletes to the selected files.
      for (Pair<Integer, List<DataFile>> partition : filesPerPartition.values()) {
        if (partition.first > 1) {
          selectedFiles.addAll(partition.second);
        }
      }
    } else if (args.fileSizeThreshold_ < 0) {
      // Select all files if FILE_SIZE_THRESHOLD_MB was not specified. We must still
      // calculate the optimization mode since the operation could be 'REWRITE_ALL'
      // or 'NOOP'.
      Preconditions.checkState(args.fileSizeThreshold_ == -1);
      selectedFiles = args.filesToFilter_.dataFilesWithoutDeletes;
    }
    TIcebergOptimizationMode mode =
        calculateOptimizationMode(args.filesToFilter_, selectedFiles);
    if (mode != TIcebergOptimizationMode.PARTIAL) {
      selectedFiles.clear();
    }
    return new FileFilteringResult(mode, selectedFiles);
  }

  private static TIcebergOptimizationMode calculateOptimizationMode(
      GroupedContentFiles filesToFilter, List<DataFile> selectedFiles) {
    // Check if no files are selected for optimization, and set the operation no-op.
    if (selectedFiles.isEmpty() && filesToFilter.dataFilesWithDeletes.isEmpty()) {
      Preconditions.checkState(filesToFilter.positionDeleteFiles.isEmpty() &&
          filesToFilter.equalityDeleteFiles.isEmpty());
      return TIcebergOptimizationMode.NOOP;
    } else {
      if (selectedFiles.size() == filesToFilter.dataFilesWithoutDeletes.size()) {
        return TIcebergOptimizationMode.REWRITE_ALL;
      } else {
        return TIcebergOptimizationMode.PARTIAL;
      }
    }
  }
}
