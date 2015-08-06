// Copyright (c) 2015 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.datagenerator;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.kitesdk.data.spi.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class JsonToParquetConverter {

  public static void main(String[] args) throws IOException {
    if (!(args.length == 3 || args.length == 4)) {
      System.err .println(
          "Usage: [--legacy_collection_format] <schema path> <json path> <output path>");
      System.exit(1);
    }

    // "Parse" args
    int i = 0;
    boolean legacyCollectionFormat = false;
    if (args.length == 4) {
      legacyCollectionFormat = true;
      ++i;
    }
    File schemaPath = new File(args[i++]);
    File jsonPath = new File(args[i++]);
    Path outputPath = new Path("file://" + args[i++]);

    // Parse Avro schema
    Schema schema = new Schema.Parser().parse(schemaPath);

    // Parse JSON file
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readValue(jsonPath, JsonNode.class);
    Preconditions.checkArgument(root.isArray(),
        "Input JSON should be an array of records");

    // Set configuration to use legacy two-level collection format, or modern
    // three-level collection format
    Configuration conf = new Configuration();
    if (legacyCollectionFormat) {
      conf.set("parquet.avro.write-old-list-structure", "true");
    } else {
      conf.set("parquet.avro.write-old-list-structure", "false");
    }

    // Write each JSON record to the parquet file

    // TODO: this ctor is deprecated, figure out how to create AvroWriteSupport
    // object instead of using 'schema' directly
    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        outputPath, schema, AvroParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
        AvroParquetWriter.DEFAULT_BLOCK_SIZE,
        AvroParquetWriter.DEFAULT_PAGE_SIZE, true, conf);
    try {
      for (JsonNode jsonRecord : root) {
        System.out.println("record: " + jsonRecord);
        GenericRecord record = (GenericRecord) JsonUtil.convertToAvro(
            GenericData.get(), jsonRecord, schema);
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
}
