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

package org.apache.impala.catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.util.Pair;

import org.apache.impala.common.FileSystemUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PuffinStatsLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PuffinStatsLoader.class);

  private Table iceApiTable_;
  private String tblName_;
  private Map<Integer, PuffinStatsRecord> result_ = new HashMap<>();

  public static class PuffinStatsRecord {
    public final StatisticsFile file;
    public final long ndv;

    public PuffinStatsRecord(StatisticsFile file, long ndv) {
      this.file = file;
      this.ndv = ndv;
    }
  }

  private PuffinStatsLoader(Table iceApiTable, String tblName) {
    iceApiTable_ = iceApiTable;
    tblName_ = tblName;
  }

  public static Map<Integer, PuffinStatsRecord> loadPuffinStats(Table iceApiTable,
      String tblName) {
    PuffinStatsLoader loader = new PuffinStatsLoader(iceApiTable, tblName);

    final List<StatisticsFile> statsFiles = iceApiTable.statisticsFiles();
    for (StatisticsFile statsFile : statsFiles) {
      loader.loadStatsFromFile(statsFile);
    }
    return loader.result_;
  }

  private void loadStatsFromFile(StatisticsFile statsFile) {
    final long currentSnapshotId = iceApiTable_.currentSnapshot().snapshotId();
    if (statsFile.snapshotId() != currentSnapshotId) return;

    // Keep track of the Iceberg column field ids for which we read statistics from this
    // Puffin file. If we run into an error reading the contents of the file, the file may
    // be corrupt so we want to remove values already read from it from the overall
    // result.
    List<Integer> fieldIdsFromFile = new ArrayList<>();
    try {
      PuffinReader puffinReader = createPuffinReader(statsFile);
      List<BlobMetadata> blobs = getBlobs(puffinReader, currentSnapshotId);

      // The 'UncheckedIOException' can be thrown from the 'next()' method of the
      // iterator. Statistics that are loaded successfully before an exception is thrown
      // are discarded because the file is probably corrupt.
      for (Pair<BlobMetadata, ByteBuffer> puffinData: puffinReader.readAll(blobs)) {
        BlobMetadata blobMetadata = puffinData.first();
        ByteBuffer blobData = puffinData.second();

        loadStatsFromBlob(blobMetadata, blobData, statsFile, fieldIdsFromFile);
      }
    } catch (NotFoundException e) {
      // 'result_' has not been touched yet.
      logWarning(tblName_, statsFile.path(), true, e);
    } catch (Exception e) {
      // We restore 'result_' to the previous state because the Puffin file may be
      // corrupt.
      logWarning(tblName_, statsFile.path(), false, e);
      result_.keySet().removeAll(fieldIdsFromFile);
    }
  }

  private static void logWarning(String tableName, String statsFilePath,
      boolean fileMissing, Exception e) {
    String missingStr = fileMissing ? "missing " : "";
    LOG.warn(String.format("Could not load Iceberg Puffin column statistics "
        + "for table '%s' from %sPuffin file '%s'. Exception: %s",
        tableName, missingStr, statsFilePath, e));
  }

  private static PuffinReader createPuffinReader(StatisticsFile statsFile) {
    org.apache.iceberg.io.InputFile puffinFile = HadoopInputFile.fromLocation(
        statsFile.path(), FileSystemUtil.getConfiguration());

    return Puffin.read(puffinFile)
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build();
  }

  private static List<BlobMetadata> getBlobs(PuffinReader puffinReader,
      long currentSnapshotId) throws java.io.IOException {
    FileMetadata fileMetadata = puffinReader.fileMetadata();
    return fileMetadata.blobs().stream()
      .filter(blob ->
          blob.snapshotId() == currentSnapshotId &&
          blob.type().equals("apache-datasketches-theta-v1") &&
          blob.inputFields().size() == 1)
      .collect(Collectors.toList());
  }

  private void loadStatsFromBlob(BlobMetadata blobMetadata, ByteBuffer blobData,
      StatisticsFile statsFile, List<Integer> fieldIdsFromFile) {
    Preconditions.checkState(blobMetadata.inputFields().size() == 1);
    int fieldId = blobMetadata.inputFields().get(0);
    if (iceApiTable_.schema().findField(fieldId) == null) {
      LOG.warn(String.format("Invalid field id %s for table '%s' found "
            + "in Puffin stats file '%s'. Ignoring blob.",
            fieldId, tblName_, statsFile.path()));
      return;
    }

    double ndv = -1;
    try {
      // Memory.wrap(ByteBuffer) would result in an incorrect deserialisation.
      ndv = Sketches.getEstimate(Memory.wrap(getBytes(blobData)));
    } catch (SketchesArgumentException e) {
      String colName = iceApiTable_.schema().idToName().get(fieldId);
      LOG.warn(String.format("Error reading datasketch for column '%s' of table '%s' "
          + "from Puffin stats file %s: %s", colName, tblName_, statsFile.path(), e));
      return;
    }
    Preconditions.checkState(ndv != -1);

    long ndvRounded = Math.round(ndv);
    PuffinStatsRecord record = new PuffinStatsRecord(statsFile, ndvRounded);

    PuffinStatsRecord prevRecord = result_.putIfAbsent(fieldId, record);

    if (prevRecord == null) {
      fieldIdsFromFile.add(fieldId);
    } else {
      String colName = iceApiTable_.schema().idToName().get(fieldId);
      LOG.warn(String.format("Multiple NDV values from Puffin statistics for column '%s' "
          + "of table '%s'. Old value (from file %s): %s; new value (from file %s): %s. "
          + "Using the old value.", colName, tblName_, prevRecord.file.path(),
          prevRecord.ndv, record.file.path(), record.ndv));
    }
  }

  // Gets the bytes from the provided 'ByteBuffer' without advancing buffer position. The
  // returned byte array may be shared with the buffer.
  private static byte[] getBytes(ByteBuffer byteBuffer) {
    if (byteBuffer.hasArray() && byteBuffer.arrayOffset() == 0 &&
        byteBuffer.position() == 0) {
      byte[] array = byteBuffer.array();
      if (byteBuffer.remaining() == array.length) {
        return array;
      }
    }

    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }
}
