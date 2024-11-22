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

package org.apache.impala.puffindatagenerator;

import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Scanner;

import org.apache.datasketches.theta.UpdateSketch;

import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * This class is used to generate data for testing the reading of Puffin files.
 *
 * We create data for different scenarios, e.g. all statistics are in the same file,
 * statistics are in different files, some Puffin files are corrupt etc. For each scenario
 * we generate (one or more) Puffin files and a metadata.json file. All of these can be
 * copied into the directory of the table. To activate a scenario, set its metadata.json
 * file as the current metadata file of the table.
 * Note that the UUID and the table location in the metadata.json file must match those of
 * the table for which the metadata.json file is used. To facilitate this, the generated
 * metadata.json files contain placeholders for these values: UUID_PLACEHOLDER and
 * TABLE_LOCATION_PLACEHOLDER. These placeholders in the files can be easily replaced with
 * the actual values obtained from the table, e.g. using the 'sed' tool.
 *
 * The metadata.json files are generated based on an existing metadata.json file (a
 * template). This should match the schema of the table for which we are generating
 * metadata. The statistics in the generated Puffin files do not normally match the actual
 * data in the table. The template metadata.json file can be taken from a newly created
 * and truncated table (truncation is needed so that a snapshot exists). The table is
 * truncated multiple times so that there are multiple snapshots.
 */
public class PuffinDataGenerator {
  // The table for which we generate data can be created this way:
  // CREATE TABLE ice_puffin(
  //   int_col1 INT,
  //   int_col2 INT,
  //   bigint_col BIGINT,
  //   float_col FLOAT,
  //   double_col DOUBLE,
  //   decimal_col DECIMAL,
  //   date_col DATE,
  //   string_col STRING,
  //   timestamp_col TIMESTAMP,
  //   bool_col BOOLEAN)
  // STORED BY ICEBERG

  public static String SKETCH_TYPE = "apache-datasketches-theta-v1";
  public static final String TABLE_LOCATION_PLACEHOLDER = "TABLE_LOCATION_PLACEHOLDER";
  public static final String UUID_PLACEHOLDER = "UUID_PLACEHOLDER";
  public static final long SEQUENCE_NUMBER = 0;

  public static final List<ByteBuffer> sketches = createSketches();

  private final String localOutputDir_;
  private final long snapshotId_;
  private final List<Long> oldSnapshotIds_;
  private final ObjectMapper mapper_;
  private final JsonNode metadataJsonTemplate_;

  private static class FileData {
    public final String filename;
    public final long snapshotId;
    public final List<Blob> blobs;
    public final boolean compressBlobs;
    // Footer compression is not supported yet by Iceberg. The spec only allows the footer
    // to be compressed with LZ4 but the Iceberg library can't handle LZ4 yet.
    public final boolean compressFooter;
    // If true, do not write file, only the 'statistics' section in metadata.json.
    public final boolean missingFile;

    public FileData(String filename, long snapshotId, List<Blob> blobs,
        boolean compressBlobs, boolean missingFile) {
      this.filename = filename;
      this.snapshotId = snapshotId;
      this.blobs = blobs;
      this.compressBlobs = compressBlobs;
      this.compressFooter = false;
      this.missingFile = missingFile;
    }

    public FileData(String filename, long snapshotId, List<Blob> blobs,
        boolean compressBlobs) {
      this(filename, snapshotId, blobs, compressBlobs, false);
    }
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {
    final String metadataJsonTemplatePath =
        "./testdata/ice_puffin/00003-442f9acd-964c-43d7-92b8-e0737a39719a.metadata.json";
    final String localOutputDir = "./puffin_files/";
    PuffinDataGenerator generator = new PuffinDataGenerator(
        metadataJsonTemplatePath, localOutputDir);

    generator.writeFileWithAllStats();
    generator.writeAllStatsTwoFiles();
    generator.writeDuplicateStatsInFile();
    generator.writeDuplicateStatsInTwoFiles();
    generator.writeOneFileCurrentOneNot();
    generator.writeNotAllBlobsCurrent();
    generator.writeMissingFile();
    generator.writeOneFileCorruptOneNot();
    generator.writeAllFilesCorrupt();
    generator.writeFileContainsInvalidFieldId();
    generator.writeStatForUnsupportedType();
    generator.writeFileWithInvalidAndCorruptSketches();
    generator.writeFileMetadataNdvOkFileCorrupt();
    generator.writeFileMultipleFieldIds();
    generator.writeSomeBlobsCurrentSomeNotInTwoFiles();
  }

  public PuffinDataGenerator(String metadataJsonTemplatePath, String localOutputDir)
      throws java.io.FileNotFoundException, JsonProcessingException {
    localOutputDir_ = localOutputDir;

    String metadataJsonStr;
    try (Scanner scanner = new Scanner(new File(metadataJsonTemplatePath))) {
      metadataJsonStr = scanner.useDelimiter("\\Z").next();
    }

    snapshotId_ = getSnapshotIdFromMetadataJson(metadataJsonStr);

    String tableLocation = getTableLocationFromMetadataJson(metadataJsonStr);
    metadataJsonStr = metadataJsonStr.replace(tableLocation, TABLE_LOCATION_PLACEHOLDER);

    mapper_ = new ObjectMapper();
    metadataJsonTemplate_ = mapper_.readTree(metadataJsonStr);

    List<Long> snapshotIds = getSnapshotIds();
    snapshotIds.remove(snapshotId_);
    oldSnapshotIds_ = snapshotIds;
  }

  private static long getSnapshotIdFromMetadataJson(String metadataJsonStr) {
    Pattern regex = Pattern.compile("\"current-snapshot-id\" ?: ?([0-9]+)");
    Matcher matcher = regex.matcher(metadataJsonStr);
    boolean match = matcher.find();
    Preconditions.checkState(match);
    String snapshotIdStr = matcher.group(1);
    return Long.parseLong(snapshotIdStr);
  }

  private static String getTableLocationFromMetadataJson(String metadataJsonStr) {
    Pattern regex = Pattern.compile("\"location\" ?: ?\"(.*)\"");
    Matcher matcher = regex.matcher(metadataJsonStr);
    boolean match = matcher.find();
    Preconditions.checkState(match);
    return matcher.group(1);
  }

  private List<Long> getSnapshotIds() {
    JsonNode snapshots = metadataJsonTemplate_.findValue("snapshots");
    List<JsonNode> snapshotIds = snapshots.findValues("snapshot-id");
    List<Long> res = new ArrayList<>();
    for (JsonNode node : snapshotIds) {
      res.add(node.asLong());
    }
    return res;
  }

  private String getPuffinFilePrefix() {
    return TABLE_LOCATION_PLACEHOLDER + "/metadata/";
  }

  // All stats are in the same Puffin file.
  private void writeFileWithAllStats()
      throws IOException {
    List<Blob> blobs = new ArrayList<>();

    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 5, 5));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 6, 6));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 7, 7));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 8, 8));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 9, 9));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData("all_stats.stats", snapshotId_, blobs, false));
    writeFilesForScenario(puffinFiles, "all_stats_in_1_file.metadata.json");
  }

  // The stats are in two separate Puffin files.
  private void writeAllStatsTwoFiles()
      throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 5, 5));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 6, 6));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 7, 7));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 8, 8));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 9, 9));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(
        new FileData("stats_divided1.stats", snapshotId_, blobs1, false));
    puffinFiles.add(
        new FileData("stats_divided2.stats", snapshotId_, blobs2, true));
    writeFilesForScenario(puffinFiles, "stats_divided.metadata.json");
  }

  // There are duplicate stats for some column(s) in the same Puffin file. The first value
  // should be used.
  private void writeDuplicateStatsInFile()
      throws IOException {
    List<Blob> blobs = new ArrayList<>();

    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 3));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "duplicate_stats_in_1_file.stats", snapshotId_, blobs, true));
    writeFilesForScenario(puffinFiles, "duplicate_stats_in_1_file.metadata.json");
  }

  // There are duplicate stats for some column(s) in separate Puffin files. The first
  // value should be used.
  private void writeDuplicateStatsInTwoFiles()
      throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 5));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "duplicate_stats_in_2_files1.stats", snapshotId_, blobs1, true));
    puffinFiles.add(new FileData(
        "duplicate_stats_in_2_files2.stats", snapshotId_, blobs2, false));
    writeFilesForScenario(puffinFiles, "duplicate_stats_in_2_files.metadata.json");
  }

  // One Puffin file is for the current snapshot while another is not.
  private void writeOneFileCurrentOneNot() throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));

    List<Blob> blobs2 = new ArrayList<>();
    long notCurrentSnapshotId = oldSnapshotIds_.get(0);
    blobs2.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 3, 3));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "current_snapshot_id.stats", snapshotId_, blobs1, true));
    puffinFiles.add(new FileData(
        "not_current_snapshot_id.stats", notCurrentSnapshotId, blobs2, true));
    writeFilesForScenario(puffinFiles, "one_file_current_one_not.metadata.json");
  }

  // Some blobs are for the current snapshot while some are not.
  private void writeNotAllBlobsCurrent() throws IOException {
    long notCurrentSnapshotId = oldSnapshotIds_.get(0);
    List<Blob> blobs = new ArrayList<>();
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    blobs.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 3, 3));
    blobs.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 4, 4));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "not_all_blobs_current.stats", snapshotId_, blobs, true));
    writeFilesForScenario(puffinFiles, "not_all_blobs_current.metadata.json");
  }

  // Two Puffin files both contain ndv values for the current and an old snapshot for the
  // same column. The reader should take the snapshot into account, otherwise it will
  // produce an invalid result whichever file is read first, i.e. there is no "lucky"
  // ordering.
  private void writeSomeBlobsCurrentSomeNotInTwoFiles() throws IOException {
    long notCurrentSnapshotId = oldSnapshotIds_.get(0);

    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 2, 4));

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 1, 5));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));

    // Even within a file, the reader has to take the snapshot into account.
    blobs2.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 3, 8));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs2.add(createBlob(notCurrentSnapshotId, SEQUENCE_NUMBER, 3, 6));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "some_blobs_current_some_not_in_2_files1.stats", snapshotId_, blobs1, true));
    puffinFiles.add(new FileData("some_blobs_current_some_not_in_2_files2.stats",
        notCurrentSnapshotId, blobs2, true));
    writeFilesForScenario(puffinFiles,
        "some_blobs_current_some_not_in_2_files.metadata.json");
  }

  // One of the Puffin files is missing. The other file(s) should be taken into account.
  private void writeMissingFile() throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(new FileData(
        "missing_file.stats", snapshotId_, blobs1, false, true));
    puffinFiles.add(new FileData("existing_file.stats", snapshotId_, blobs2, true));
    writeFilesForScenario(puffinFiles, "missing_file.metadata.json");
  }

  // One of the Puffin files is corrupt, the other is not. The other file should be taken
  // into account.
  private void writeOneFileCorruptOneNot() throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    FileData corruptFile = new FileData(
        "corrupt_file.stats", snapshotId_, blobs1, false);

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));
    FileData nonCorruptFile = new FileData(
        "non_corrupt_file.stats", snapshotId_, blobs2, false);

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(corruptFile);
    puffinFiles.add(nonCorruptFile);
    writeFilesForScenario(puffinFiles, "one_file_corrupt_one_not.metadata.json");

    this.corruptFile(corruptFile.filename);
  }

  private void writeAllFilesCorrupt() throws IOException {
    List<Blob> blobs1 = new ArrayList<>();
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    blobs1.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 2, 2));
    FileData corruptFile1 = new FileData(
        "corrupt_file1.stats", snapshotId_, blobs1, true);

    List<Blob> blobs2 = new ArrayList<>();
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs2.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 4, 4));
    FileData corruptFile2 = new FileData(
        "corrupt_file2.stats", snapshotId_, blobs2, true);

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(corruptFile1);
    puffinFiles.add(corruptFile2);
    writeFilesForScenario(puffinFiles, "all_files_corrupt.metadata.json");

    this.corruptFile(corruptFile1.filename);
    this.corruptFile(corruptFile2.filename);
  }

  private void writeFileContainsInvalidFieldId() throws IOException {
    List<Blob> blobs = new ArrayList<>();
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));
    int invalid_field_id = 200;
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, invalid_field_id, 2));
    FileData corruptFile1 = new FileData(
        "file_contains_invalid_field_id.stats", snapshotId_, blobs, true);


    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(corruptFile1);
    writeFilesForScenario(puffinFiles, "file_contains_invalid_field_id.metadata.json");
  }

  private void writeStatForUnsupportedType() throws IOException {
    List<Blob> blobs = new ArrayList<>();
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 2));
    int unsupported_field_id = 10;
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, unsupported_field_id, 2));
    FileData corruptFile1 = new FileData(
        "stats_for_unsupported_type.stats", snapshotId_, blobs, true);

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(corruptFile1);
    writeFilesForScenario(puffinFiles, "stats_for_unsupported_type.metadata.json");
  }

  private void writeFileWithInvalidAndCorruptSketches() throws IOException {
    List<Blob> blobs = new ArrayList<>();
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 1, 1));

    // Sketch with invalid type
    final String invalidSketchType = "invalidSketchType";
    blobs.add(new Blob(invalidSketchType, Arrays.asList(2), snapshotId_,
        SEQUENCE_NUMBER, sketches.get(1)));

    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 3, 3));
    blobs.add(createBlobCorruptSketch(snapshotId_, SEQUENCE_NUMBER, 4, 4, false));
    blobs.add(createBlob(snapshotId_, SEQUENCE_NUMBER, 5, 5));

    FileData fileData = new FileData(
        "invalidAndCorruptSketches.stats", snapshotId_, blobs, true);
    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(fileData);
    writeFilesForScenario(puffinFiles, "invalidAndCorruptSketches.metadata.json");
  }

  private void writeFileMetadataNdvOkFileCorrupt() throws IOException {
    // The sketches in the Puffin file are corrupt but it shouldn't cause an error since
    // we don't actually read it because we read the NDV value from the metadata.json
    // file.
    List<Blob> blobs = new ArrayList<>();
    blobs.add(createBlobCorruptSketch(snapshotId_, SEQUENCE_NUMBER, 1, 1, true));
    blobs.add(createBlobCorruptSketch(snapshotId_, SEQUENCE_NUMBER, 2, 2, true));

    FileData corruptFile = new FileData(
        "metadata_ndv_ok_sketches_corrupt.stats", snapshotId_, blobs, true);

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(corruptFile);
    writeFilesForScenario(puffinFiles,
        "metadata_ndv_ok_stats_file_corrupt.metadata.json");
  }

  private void writeFileMultipleFieldIds() throws IOException {
    List<Blob> blobs = new ArrayList<>();
    List<Integer> fieldIds = Arrays.asList(1, 2);
    blobs.add(createBlobMultipleFieldIds(snapshotId_, SEQUENCE_NUMBER, fieldIds, 1,
        true));

    FileData file = new FileData(
        "multiple_field_ids.stats", snapshotId_, blobs, true);

    List<FileData> puffinFiles = new ArrayList<>();
    puffinFiles.add(file);
    writeFilesForScenario(puffinFiles, "multiple_field_ids.metadata.json");
  }

  private static ByteBuffer createSketchWithNdv(int ndv) {
    UpdateSketch sketch = UpdateSketch.builder().build();
    for (int i = 0; i < ndv; i++) sketch.update(i);
    return ByteBuffer.wrap(sketch.compact().toByteArray());
  }

  private static List<ByteBuffer> createSketches() {
    ImmutableList.Builder<ByteBuffer> builder = new ImmutableList.Builder<>();
    for (int i = 1; i <= 9; i++) {
      builder.add(createSketchWithNdv(i));
    }
    return builder.build();
  }

  private static Blob createBlob(long snapshotId, long sequenceNumber,
      int fieldId, int ndv) {
    return createBlob(snapshotId, sequenceNumber, fieldId, ndv, false);
  }

  private static Blob createBlob(long snapshotId, long sequenceNumber,
      int fieldId, int ndv, boolean addNdvProperty) {
    return createBlobMultipleFieldIds(snapshotId, sequenceNumber, Arrays.asList(fieldId),
        ndv, addNdvProperty);
  }

  private static Blob createBlobCorruptSketch(long snapshotId, long sequenceNumber,
      int fieldId, int ndv, boolean addNdvProperty) {
    // Corrupt sketch.
    byte[] bytes = {0, 0};
    ByteBuffer corruptSketch = ByteBuffer.wrap(bytes);

    return createBlobWithProperties(snapshotId, sequenceNumber, Arrays.asList(fieldId),
        ndv, corruptSketch, addNdvProperty);
  }

  private static Blob createBlobMultipleFieldIds(long snapshotId, long sequenceNumber,
      List<Integer> fieldIds, int ndv, boolean addNdvProperty) {
    return createBlobWithProperties(snapshotId, sequenceNumber, fieldIds, ndv,
        sketches.get(ndv-1), addNdvProperty);
  }

  private static Blob createBlobWithProperties(long snapshotId, long sequenceNumber,
      List<Integer> fieldIds, int ndv, ByteBuffer datasketch, boolean addNdvProperty) {
    Map<String, String> properties = new HashMap<>();
    if (addNdvProperty) {
      properties.put("ndv", Integer.toString(ndv));
    }
    return new Blob(SKETCH_TYPE, fieldIds, snapshotId, sequenceNumber,
        datasketch, null, properties);
  }

  private void writeFilesForScenario(List<FileData> puffinFiles, String statsJsonFile)
      throws IOException {
    ArrayNode jsonStatsList = mapper_.createArrayNode();
    for (FileData fileData : puffinFiles) {
      jsonStatsList.add(writeBlobsToFile(fileData));
    }
    writeMetadataJsonWithStatsToFile(statsJsonFile, jsonStatsList);
  }

  private ObjectNode writeBlobsToFile(FileData fileData) throws IOException {
    String localOutfile = localOutputDir_ + fileData.filename;

    // These values are used if we don't actually write a file ('fileData.missingFile' is
    // true). These are the values with a file with two blobs.
    long fileSize = 340;
    long footerSize = 288;

    if (!fileData.missingFile) {
      Puffin.WriteBuilder writeBuilder = Puffin.write(
          HadoopOutputFile.fromLocation(localOutfile,
          new org.apache.hadoop.conf.Configuration()));
      writeBuilder.createdBy("Impala Puffin Data Generator");
      if (fileData.compressBlobs) {
        writeBuilder.compressBlobs(PuffinCompressionCodec.ZSTD);
      }
      if (fileData.compressFooter) writeBuilder.compressFooter();

      PuffinWriter writer = writeBuilder.build();
      for (Blob blob : fileData.blobs) writer.add(blob);
      writer.finish();
      writer.close();

      fileSize = writer.fileSize();
      footerSize = writer.footerSize();
    }


    ObjectNode statsNode = mapper_.createObjectNode();
    statsNode.put("snapshot-id", fileData.snapshotId);
    statsNode.put("statistics-path", getPuffinFilePrefix() + fileData.filename);
    statsNode.put("file-size-in-bytes", fileSize);
    statsNode.put("file-footer-size-in-bytes", footerSize);

    statsNode.put("blob-metadata", blobsToJson(fileData.blobs));
    return statsNode;
  }

  private ArrayNode blobsToJson(List<Blob> blobs) throws JsonProcessingException {
    ArrayNode list = mapper_.createArrayNode();
    for (Blob blob : blobs) list.add(blobMetadataToJson(blob));
    return list;
  }

  private ObjectNode blobMetadataToJson(Blob blob) throws JsonProcessingException {
    ObjectNode blobNode = mapper_.createObjectNode();

    blobNode.put("type", blob.type());
    blobNode.put("snapshot-id", blob.snapshotId());
    blobNode.put("sequence-number", blob.sequenceNumber());

    ArrayNode fieldsList = mapper_.createArrayNode();
    for (int fieldId : blob.inputFields()) fieldsList.add(fieldId);
    blobNode.set("fields", fieldsList);

    // Put properties
    if (!blob.properties().isEmpty()) {
      ObjectNode properties = mapper_.createObjectNode();
      for (Map.Entry<String, String> entry : blob.properties().entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
      blobNode.set("properties", properties);
    }

    return blobNode;
  }

  private void writeMetadataJsonWithStatsToFile(String outfile, ArrayNode stats)
      throws IOException {
    JsonNode metadataJson = metadataJsonTemplate_.deepCopy();

    // Replace UUID with a placeholder.
    String uuidKey = "table-uuid";
    ObjectNode uuidParent = (ObjectNode) metadataJson.findParent(uuidKey);
    uuidParent.put(uuidKey, UUID_PLACEHOLDER);

    ObjectNode statsParent = (ObjectNode) metadataJson.findParent("statistics");
    statsParent.put("statistics", stats);

    String outfilePath = localOutputDir_ + outfile;
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(outfilePath), "utf-8"))) {
      String jsonString = mapper_.writerWithDefaultPrettyPrinter()
          .writeValueAsString(metadataJson);
      writer.write(jsonString);
    }
  }

  // Re-write the file without the magic (first 4 bytes) and some additional bytes.
  private void corruptFile(String filename) throws FileNotFoundException, IOException {
    String filePath = localOutputDir_ + filename;

    int fileSize = (int) new File(filePath).length();
    byte[] bytes = new byte[fileSize];

    try (InputStream inputStream = new FileInputStream(filePath)) {
      int bytesRead = inputStream.read(bytes);
    }

    try (OutputStream outputStream = new FileOutputStream(filePath)) {
      final int magicLength = 4;
      final int bytesToOmit = magicLength + 4;
      outputStream.write(bytes, bytesToOmit, bytes.length - bytesToOmit);
    }
  }
}
