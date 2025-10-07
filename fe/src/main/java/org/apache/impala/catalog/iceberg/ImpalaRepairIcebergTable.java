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

package org.apache.impala.catalog.iceberg;

import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Transaction;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;


public class ImpalaRepairIcebergTable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRepairIcebergTable.class);
  private final FeIcebergTable table_;
  public ImpalaRepairIcebergTable(FeIcebergTable table) {
    table_ = table;
  }

  public int execute(Transaction iceTxn) throws CatalogException {
    Collection<String> missingFiles;
    IcebergContentFileStore fileStore = table_.getContentFileStore();
    if (fileStore.hasMissingFile()) {
      missingFiles = fileStore.getMissingFiles();
    } else {
      missingFiles = getMissingFiles(fileStore);
    }
    // Delete all files from the missing files collection
    int numRemovedReferences = missingFiles.size();
    if (numRemovedReferences > 0) {
      DeleteFiles deleteFiles = iceTxn.newDelete();
      for (String path : missingFiles) {
        deleteFiles.deleteFile(path);
      }
      deleteFiles.commit();
      LOG.info("Removed {} files during table repair: {}",
          numRemovedReferences, getFilesLog(missingFiles));
    } else {
      LOG.info("No files were removed during table repair.");
    }
    return numRemovedReferences;
  }

  private List<String> getMissingFiles(IcebergContentFileStore fileStore)
      throws CatalogException {
    // Check all data files in parallel and create a list of dangling references.
    List<String> missingFiles;
    ForkJoinPool forkJoinPool = null;
    try {
      forkJoinPool = new ForkJoinPool(BackendConfig.INSTANCE.icebergCatalogNumThreads());
      missingFiles = forkJoinPool.submit(() ->
          StreamSupport.stream(
              fileStore.getAllDataFiles().spliterator(), /*parallel=*/true)
            .map(fileDesc -> fileDesc.getAbsolutePath(table_.getLocation()))
            .filter(path -> !table_.getIcebergApiTable().io().newInputFile(path).exists())
            .collect(Collectors.toList())
      ).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e.getMessage(), e);
    } finally {
      if (forkJoinPool != null) {
        forkJoinPool.shutdown();
      }
    }
    return missingFiles;
  }

  private static String getFilesLog(Collection<String> missingFiles) {
    int numPathsToLog = 3;
    if (LOG.isTraceEnabled()) {
      numPathsToLog = 1000;
    }
    int numRemoved = missingFiles.size();
    String fileNames = Joiner.on(", ").join(
        Iterables.limit(missingFiles, numPathsToLog));
    if (numRemoved > numPathsToLog) {
      int remaining = numRemoved - numPathsToLog;
      fileNames += String.format(", and %d more.", remaining);
    }
    return fileNames;
  }
}
