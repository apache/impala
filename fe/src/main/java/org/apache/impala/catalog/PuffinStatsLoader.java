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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.StatisticsFile;

import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for loading NDV statistics from Puffin files that belong to an Iceberg table.
 *
 * When stats are available for multiple snapshots, the most recent one is chosen for each
 * column. Note that this also means that the values for the columns may come from
 * different snapshots.
 *
 * Note:
 * In Iceberg, 'org.apache.iceberg.BlobMetadata' is an interface for metadata blobs.
 * 'StatisticsFile' objects obtained from the "metadata.json" file contain implementors of
 * this interface. 'org.apache.iceberg.puffin.BlobMetadata' is a class that represents
 * blob metadata present in Puffin files. This latter class unfortunately does not
 * implement the 'org.apache.iceberg.BlobMetadata' interface but can be converted to
 * org.apache.iceberg.GenericBlobMetadata, which does.
 */
public class PuffinStatsLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PuffinStatsLoader.class);

  private final Table iceApiTable_;
  private final String tblName_;

  // The timestamp of the HMS stats and columns that have HMS stats. Puffin NDVs will only
  // be loaded if they are more recent than existing HMS stats.
  private final long hmsStatsTimestampMs_;
  private final Set<Integer> fieldIdsWithHmsStats_;

  // The blobs to read from Puffin files. Initialised in 'initBlobsToRead()'. The keys of
  // the inner map are fieldIds and its values are snapshotIds - together they identify
  // the blobs we'd like to read. When a blob is processed and the NDV it contains is
  // added to 'result_', it is removed from this map so even if there are multiple blobs
  // for the same fieldId and snapshotId in the same file, we'll only read one.
  private Map<StatisticsFile, Map<Integer, Long>> blobsToRead_;
  private Map<Integer, PuffinStatsRecord> result_ = new HashMap<>();

  public static class PuffinStatsRecord {
    public final StatisticsFile file;
    public final long snapshotId;
    public final boolean isFromMetadataJson;
    public final long ndv;

    public PuffinStatsRecord(StatisticsFile file, long snapshotId,
        boolean isFromMetadataJson, long ndv) {
      this.file = file;
      this.snapshotId = snapshotId;
      this.isFromMetadataJson = isFromMetadataJson;
      this.ndv = ndv;
    }
  }

  private PuffinStatsLoader(Table iceApiTable, String tblName, long hmsStatsTimestampMs,
      Set<Integer> fieldIdsWithHmsStats) {
    iceApiTable_ = iceApiTable;
    tblName_ = tblName;
    hmsStatsTimestampMs_ = hmsStatsTimestampMs;
    fieldIdsWithHmsStats_ = fieldIdsWithHmsStats;
  }

  /**
   * Loads NDV stats from Iceberg Puffin statistics belonging to the table. For each
   * column, the most recent available NDV value is chosen.
   *
   * Stats for columns in 'fieldIdsWithHmsStats' are only loaded if they belong to a
   * snapshot that is more recent than 'hmsStatsTimestampMs'.
   *
   * If it is detected that there are multiple blobs for a given fieldId-snapshotId pair,
   * a warning log is issued, but no attempt is made to detect all such cases.
   */
  public static Map<Integer, PuffinStatsRecord> loadPuffinStats(Table iceApiTable,
      String tblName, long hmsStatsTimestampMs, Set<Integer> fieldIdsWithHmsStats) {
    PuffinStatsLoader loader = new PuffinStatsLoader(iceApiTable, tblName,
        hmsStatsTimestampMs, fieldIdsWithHmsStats);
    return loader.loadPuffinStatsImpl();
  }

  private Map<Integer, PuffinStatsRecord> loadPuffinStatsImpl() {
    // There is no snapshot in the table yet.
    if (iceApiTable_.currentSnapshot() == null) return new HashMap<>();

    initBlobsToRead();

    for (StatisticsFile statsFile : blobsToRead_.keySet()) {
      loadStatsFromMetadata(statsFile);
    }

    for (StatisticsFile statsFile : blobsToRead_.keySet()) {
      loadStatsFromFile(statsFile);
    }

    return result_;
  }

  private void initBlobsToRead() {
    Map<Integer, Pair<Long, StatisticsFile>> blobsToRead = calculateBlobsToRead();
    blobsToRead_ = groupBlobsToReadByFile(blobsToRead);
  }

  private Map<Integer, Pair<Long, StatisticsFile>> calculateBlobsToRead() {
    Map<Integer, Pair<Long, StatisticsFile>> fieldIdToSnapshotId = new HashMap<>();

    final List<StatisticsFile> statsFiles = iceApiTable_.statisticsFiles();

    for (StatisticsFile statsFile : statsFiles) {
      List<org.apache.iceberg.BlobMetadata> metadataBlobs =
          getBlobsFromMetadataJsonSection(statsFile);

      for (org.apache.iceberg.BlobMetadata mBlob : metadataBlobs) {
        Preconditions.checkState(mBlob.fields().size() == 1);
        int fieldId = mBlob.fields().get(0);
        long snapshotId = mBlob.sourceSnapshotId();

        if (hmsHasMoreRecentStats(fieldId, snapshotId)) continue;

        Pair<Long, StatisticsFile> oldValue = fieldIdToSnapshotId.get(fieldId);
        if (oldValue == null || isMoreRecentSnapshot(snapshotId, oldValue.first)) {
          fieldIdToSnapshotId.put(fieldId, new Pair<>(snapshotId, statsFile));
        } else if (oldValue.first == snapshotId) {
          logDuplicateStat(fieldId, snapshotId, oldValue.second.path(),
              statsFile.path(), null);
        }
      }
    }

    return fieldIdToSnapshotId;
  }

  private Map<StatisticsFile, Map<Integer, Long>> groupBlobsToReadByFile(
      Map<Integer, Pair<Long, StatisticsFile>> blobsToRead) {
    Map<StatisticsFile, Map<Integer, Long>> res = new HashMap<>();

    for (Map.Entry<Integer, Pair<Long, StatisticsFile>> entry : blobsToRead.entrySet()) {
      int fieldId = entry.getKey();
      long snapshotId = entry.getValue().first;
      StatisticsFile file = entry.getValue().second;

      res.computeIfAbsent(file, k -> new HashMap<>()).put(fieldId, snapshotId);
    }

    return res;
  }

  private boolean isMoreRecentSnapshot(long snapshotIdToCheck, long baseSnapshotId) {
    long baseTs = iceApiTable_.snapshot(baseSnapshotId).timestampMillis();
    long toCheckTs = iceApiTable_.snapshot(snapshotIdToCheck).timestampMillis();
    return toCheckTs > baseTs;
  }

  // Returns true if there are HMS stats for the column referenced by 'fieldId' that are
  // at least as recent as the snapshot referenced by 'snapshotId'
  private boolean hmsHasMoreRecentStats(int fieldId, long snapshotId) {
    long snapshotTs = iceApiTable_.snapshot(snapshotId).timestampMillis();
    return hmsStatsTimestampMs_ >= snapshotTs && fieldIdsWithHmsStats_.contains(fieldId);
  }

  // Checks the metadata of 'statsFile' and loads NDV values where available.
  private void loadStatsFromMetadata(StatisticsFile statsFile) {
    List<org.apache.iceberg.BlobMetadata> metadataBlobs =
        getBlobsFromMetadataJsonSection(statsFile);
    for (org.apache.iceberg.BlobMetadata mBlob : metadataBlobs) {
      Preconditions.checkState(mBlob.fields().size() == 1);
      int fieldId = mBlob.fields().get(0);
      long snapshotId = mBlob.sourceSnapshotId();

      if (!shouldUseBlob(statsFile, fieldId, snapshotId)) continue;

      PuffinStatsRecord existingRecord = result_.get(fieldId);
      Preconditions.checkState(existingRecord == null);

      long ndv = getNdvFromMetadata(mBlob);
      if (ndv != -1) {
        PuffinStatsRecord record = new PuffinStatsRecord(
            statsFile, mBlob.sourceSnapshotId(), true, ndv);
        addStatsRecordToResult(fieldId, record, null);
        blobsToRead_.get(statsFile).remove(fieldId);
      }
    }
  }

  // Loads NDV values from the Puffin file referenced by 'statsFile' for field ids for
  // which an NDV value has not already been loaded.
  private void loadStatsFromFile(StatisticsFile statsFile) {
    // Keep track of the Iceberg column field ids for which we read statistics from this
    // Puffin file. If we run into an error reading the contents of the file, the file may
    // be corrupt so we want to remove values already read from it from the overall
    // result.
    List<Integer> fieldIdsFromFile = new ArrayList<>();

    try {
      PuffinReader puffinReader = createPuffinReader(statsFile);
      List<org.apache.iceberg.puffin.BlobMetadata> blobs = getBlobsFromPuffinFile(
          puffinReader, statsFile);

      // The 'UncheckedIOException' can be thrown from the 'next()' method of the
      // iterator. Statistics that are loaded successfully before an exception is thrown
      // are discarded because the file is probably corrupt.
      for (org.apache.iceberg.util.Pair<
              org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> puffinData
          : puffinReader.readAll(blobs)) {
        org.apache.iceberg.puffin.BlobMetadata blobMetadata = puffinData.first();
        ByteBuffer blobData = puffinData.second();

        loadStatsFromBlob(blobMetadata, blobData, statsFile, fieldIdsFromFile);
      }
    } catch (NotFoundException e) {
      // 'result_' has not been touched yet.
      logWarningWithFile(tblName_, statsFile.path(), true, e);
    } catch (Exception e) {
      // We restore 'result_' to the previous state because the Puffin file may be
      // corrupt.
      logWarningWithFile(tblName_, statsFile.path(), false, e);
      result_.keySet().removeAll(fieldIdsFromFile);
    }
  }

  private long getNdvFromMetadata(org.apache.iceberg.BlobMetadata blob) {
    String ndvProperty = blob.properties().get("ndv");
    if (ndvProperty == null) return -1;

    try {
      return Long.parseLong(ndvProperty);
    } catch (NumberFormatException e) {
      int fieldId = blob.fields().get(0);
      Preconditions.checkNotNull(iceApiTable_.schema().findField(fieldId));

      String colName = fieldIdToColName(fieldId);
      LOG.warn(String.format(
            "Invalid NDV property in the statistics metadata for column %s: '%s'"),
            colName, ndvProperty);
      return -1;
    }
  }

  private static void logWarningWithFile(String tableName, String statsFilePath,
      boolean fileMissing, Exception e) {
    String missingStr = fileMissing ? "missing " : "";
    LOG.warn(String.format("Could not load Iceberg Puffin column statistics "
        + "for table '%s' from %sPuffin file '%s'. Exception: %s",
        tableName, missingStr, statsFilePath, e));
  }

  private void logDuplicateStat(int fieldId, long snapshotId,
      String existingRecordFilePath, String newRecordFilePath, Long existingRecordNdv) {
    String colName = fieldIdToColName(fieldId);

    String existingNdvStr = existingRecordNdv == null ?
        "" : String.format(" (%s)", existingRecordNdv);

    if (existingRecordFilePath.equals(newRecordFilePath)) {
      LOG.warn(String.format("Multiple NDV values from Puffin statistics file %s for "
          + "column '%s' of table '%s' for snapshot %s. Only using the first encountered "
          + "one%s, ignoring the rest.",
          existingRecordFilePath, colName, tblName_, snapshotId, existingNdvStr));
    } else {
      LOG.warn(String.format("Multiple NDV values from Puffin statistics for column '%s' "
          + "of table '%s' for snapshot %s. Ignoring new value from file %s, using old "
          + "value%s from file %s.", colName, tblName_, snapshotId, newRecordFilePath,
          existingNdvStr, existingRecordFilePath));
    }
  }

  private static PuffinReader createPuffinReader(StatisticsFile statsFile) {
    org.apache.iceberg.io.InputFile puffinFile = HadoopInputFile.fromLocation(
        statsFile.path(), FileSystemUtil.getConfiguration());

    return Puffin.read(puffinFile)
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build();
  }

  private List<org.apache.iceberg.BlobMetadata> getBlobsFromMetadataJsonSection(
      StatisticsFile statsFile) {
    return statsFile.blobMetadata().stream()
        .filter(blob -> blobFilterPredicate(blob, statsFile.path()))
        .collect(Collectors.toList());
  }

  private List<org.apache.iceberg.puffin.BlobMetadata> getBlobsFromPuffinFile(
      PuffinReader puffinReader, StatisticsFile statsFile) throws java.io.IOException {
    FileMetadata fileMetadata = puffinReader.fileMetadata();

    List<org.apache.iceberg.puffin.BlobMetadata> res = new ArrayList<>();
    Set<Integer> fieldIdsAdded = new HashSet<>();
    for (org.apache.iceberg.puffin.BlobMetadata blob : fileMetadata.blobs()) {
      if (!blobFilterPredicate(org.apache.iceberg.GenericBlobMetadata.from(blob), null)) {
        continue;
      }

      int fieldId = blob.inputFields().get(0);
      long snapshotId = blob.snapshotId();

      if (!shouldUseBlob(statsFile, fieldId, snapshotId)) continue;

      Preconditions.checkState(!result_.containsKey(fieldId));

      // It's possible that 'fieldIdsAdded' already contains 'fieldId' if there are
      // multiple blobs for a certain fieldId-snapshotId pair in the Puffin file, but then
      // we have already logged this when determining which blobs we should read.
      if (!fieldIdsAdded.contains(fieldId)) {
        res.add(blob);
        fieldIdsAdded.add(fieldId);
      }
    }
    return res;
  }

  private boolean shouldUseBlob(StatisticsFile statsFile, int fieldId, long snapshotId) {
    Map<Integer, Long> fieldIdsAndSnapshots = blobsToRead_.get(statsFile);
    if (fieldIdsAndSnapshots == null) return false;

    Long snapshotToRead = fieldIdsAndSnapshots.get(fieldId);
    return snapshotToRead != null && snapshotId == snapshotToRead;
  }

  // Filters out blobs that are invalid or that cannot be read by Impala.
  // If 'fileName' is not null, logs a warning if the field id contained in the blob is
  // invalid.
  private boolean blobFilterPredicate(org.apache.iceberg.BlobMetadata blobMetadata,
      String fileName) {
    if (blobMetadata.fields().size() != 1) return false;
    // Invalid snapshot id for the table.
    if (iceApiTable_.snapshot(blobMetadata.sourceSnapshotId()) == null) return false;

    int fieldId = blobMetadata.fields().get(0);
    if (iceApiTable_.schema().findField(fieldId) == null) {
      if (fileName != null) {
        LOG.warn(String.format("Invalid field id %s for table '%s' found "
            + "in Puffin stats file '%s'. Ignoring blob.",
            fieldId, tblName_, fileName));
      }
      return false;
    }

    return blobMetadata.type().equals("apache-datasketches-theta-v1");
  }

  private void loadStatsFromBlob(org.apache.iceberg.puffin.BlobMetadata blobMetadata,
      ByteBuffer blobData, StatisticsFile statsFile, List<Integer> fieldIdsFromFile) {
    Preconditions.checkState(blobMetadata.inputFields().size() == 1);
    int fieldId = blobMetadata.inputFields().get(0);
    Preconditions.checkNotNull(iceApiTable_.schema().findField(fieldId));

    double ndv = -1;
    try {
      // Memory.wrap(ByteBuffer) would result in an incorrect deserialisation.
      ndv = Sketches.getEstimate(Memory.wrap(getBytes(blobData)));
    } catch (SketchesArgumentException e) {
      String colName = fieldIdToColName(fieldId);
      LOG.warn(String.format("Error reading datasketch for column '%s' of table '%s' "
          + "from Puffin stats file %s: %s", colName, tblName_, statsFile.path(), e));
      return;
    }
    Preconditions.checkState(ndv != -1);

    long ndvRounded = Math.round(ndv);
    PuffinStatsRecord record = new PuffinStatsRecord(
        statsFile, blobMetadata.snapshotId(), false, ndvRounded);

    addStatsRecordToResult(fieldId, record, fieldIdsFromFile);
    blobsToRead_.get(statsFile).remove(fieldId);
  }

  private void addStatsRecordToResult(int fieldId, PuffinStatsRecord record,
      List<Integer> fieldIdsFromFile) {
    PuffinStatsRecord prevRecord = result_.putIfAbsent(fieldId, record);
    // Duplicate stats are detected earlier.
    Preconditions.checkState(prevRecord == null);
    if (!record.isFromMetadataJson) fieldIdsFromFile.add(fieldId);
  }

  private String fieldIdToColName(int fieldId) {
    String colName = iceApiTable_.schema().idToName().get(fieldId);
    Preconditions.checkNotNull(colName);
    return colName;
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
