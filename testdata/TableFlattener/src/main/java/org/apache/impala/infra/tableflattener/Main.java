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

package org.apache.impala.infra.tableflattener;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

  Options cliOptions_;
  DatasetDescriptor datasetDescr_;

  // The dir to write the flat datasets to. The dir should either not exist or be
  // empty. The URI can either point to a local dir or an HDFS dir.
  URI outputDir_;
  CommandLine commandLine_;

  @SuppressWarnings("static-access")
  void parseArgs(String[] args) throws ParseException, IOException {
    cliOptions_ = new Options();
    cliOptions_.addOption(OptionBuilder.withLongOpt("help").create("h"));
    cliOptions_.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("input-data-format")
        .withDescription("The format of the input file. Ex, avro")
        .create("f"));
    cliOptions_.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("input-data-compression")
        .withDescription("The compression type of the input file. Ex, snappy")
        .create("c"));
    cliOptions_.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("input-schema-uri")
        .withDescription("The URI of the input file's schema. Ex, file://foo.avsc")
        .create("s"));
    CommandLineParser parser = new PosixParser();
    commandLine_ = parser.parse(cliOptions_, args);

    if (commandLine_.hasOption("h")) printHelp();

    DatasetDescriptor.Builder datasetDescrBuilder = new DatasetDescriptor.Builder();

    String[] dataArgs = commandLine_.getArgs();
    if (dataArgs.length != 2) {
      printHelp("Exactly two arguments are required");
    }

    URI dataFile = URI.create(dataArgs[0]);
    outputDir_ = URI.create(dataArgs[1]);
    datasetDescrBuilder.location(dataFile);

    Format inputFormat;
    if (commandLine_.hasOption("f")) {
      inputFormat = Formats.fromString(commandLine_.getOptionValue("f"));
    } else {
      String dataFilePath = dataFile.getPath();
      if (dataFilePath == null || dataFilePath.isEmpty()) {
        printHelp("Data file URI is missing a path component: " + dataFile.toString());
      }
      String ext = FilenameUtils.getExtension(dataFilePath);
      if (ext.isEmpty()) {
        printHelp("The file format (-f) must be specified");
      }
      inputFormat = Formats.fromString(ext);
    }
    datasetDescrBuilder.format(inputFormat);

    if (commandLine_.hasOption("c")) {
      datasetDescrBuilder.compressionType(
          CompressionType.forName(commandLine_.getOptionValue("c")));
    }

    if (commandLine_.hasOption("s")) {
      datasetDescrBuilder.schemaUri(commandLine_.getOptionValue("s"));
    } else if (inputFormat == Formats.AVRO) {
      datasetDescrBuilder.schemaFromAvroDataFile(dataFile);
    } else if (inputFormat == Formats.PARQUET) {
      ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(
          new Configuration(), new org.apache.hadoop.fs.Path(dataFile));
      datasetDescrBuilder.schema(new AvroSchemaConverter().convert(
          parquetMetadata.getFileMetaData().getSchema()));
    } else {
      printHelp("A schema (-s) is required for data format " + inputFormat.getName());
    }

    datasetDescr_ = datasetDescrBuilder.build();
  }

  void printHelp() { printHelp(""); }

  void printHelp(String errorMessage) {
    PrintWriter printer = new PrintWriter(
        errorMessage.isEmpty() ? System.out : System.err);
    if (!errorMessage.isEmpty()) printer.println("Error: " + errorMessage + "\n");
    printer.println("Usage: [options] <input uri> <output uri>\n\n" +
        "input uri    The URI to the input file.\n" +
        "               Ex, file:///foo.avro or hdfs://localhost:20500/foo.avro\n" +
        "output uri   The URI to the output directory. The dir must either not\n" +
        "               exist or it must be empty.\n" +
        "               Ex, file:///bar or hdfs://localhost:20500/bar\n\n" +
        "Options:");
    new HelpFormatter().printOptions(printer, 80, cliOptions_, 1 , 3);
    printer.close();
    System.exit(errorMessage.isEmpty() ? 0 : 1);
  }

  void exec(String[] args) throws ParseException, IOException {
    Logger.getRootLogger().setLevel(Level.OFF);
    parseArgs(args);

    SchemaFlattener schemaFlattener = new SchemaFlattener(outputDir_);
    FlattenedSchema rootDataset =
        schemaFlattener.flatten(datasetDescr_.getSchema());

    Path tempDatasetPath = Files.createTempDirectory(null);
    try {
      Dataset<GenericRecord> srcDataset = Datasets.create(
          "dataset:file:" + tempDatasetPath.toString(), datasetDescr_);
      FileMigrator migrator = new FileMigrator();
      migrator.migrate(srcDataset, rootDataset);
    } finally {
      FileUtils.deleteDirectory(tempDatasetPath.toFile());
    }
  }

  public static void main(String[] args) throws Exception {
    new Main().exec(args);
  }
}
