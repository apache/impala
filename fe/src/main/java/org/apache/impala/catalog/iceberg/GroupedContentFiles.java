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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Struct-like object to group different Iceberg content files:
 * - data files without deleted rows
 * - data files with deleted rows
 * - delete files (position and equality)
 */
public class GroupedContentFiles {
  public List<DataFile> dataFilesWithoutDeletes = new ArrayList<>();
  public List<DataFile> dataFilesWithDeletes = new ArrayList<>();
  public Set<DeleteFile> positionDeleteFiles = new HashSet<>();
  public Set<DeleteFile> equalityDeleteFiles = new HashSet<>();

  public GroupedContentFiles() { }

  public GroupedContentFiles(CloseableIterable<FileScanTask> fileScanTasks) {
    for (FileScanTask scanTask : fileScanTasks) {
      if (scanTask.deletes().isEmpty()) {
        dataFilesWithoutDeletes.add(scanTask.file());
      } else {
        dataFilesWithDeletes.add(scanTask.file());
        for (DeleteFile delFile : scanTask.deletes()) {
          if (delFile.content() == FileContent.POSITION_DELETES) {
            positionDeleteFiles.add(delFile);
          } else {
            Preconditions.checkState(delFile.content() == FileContent.EQUALITY_DELETES);
            equalityDeleteFiles.add(delFile);
          }
        }
      }
    }
  }

  public Iterable<ContentFile<?>> getAllContentFiles() {
    return Iterables.concat(dataFilesWithoutDeletes, dataFilesWithDeletes,
        positionDeleteFiles, equalityDeleteFiles);
  }

  public int size() {
    return dataFilesWithDeletes.size() + dataFilesWithoutDeletes.size() +
        positionDeleteFiles.size() + equalityDeleteFiles.size();
  }

  public boolean isEmpty() {
    return Iterables.isEmpty(getAllContentFiles());
  }
}
