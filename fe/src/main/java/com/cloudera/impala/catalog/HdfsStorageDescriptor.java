// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.hive.serde.ParquetHiveSerDe;

import com.cloudera.impala.thrift.CatalogObjectsConstants;
import com.cloudera.impala.thrift.THdfsCompression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Represents the file format metadata for files stored in a table or partition.
 */
public class HdfsStorageDescriptor {
  private static final char DEFAULT_LINE_DELIM = '\n';
  // hive by default uses ctrl-a as field delim
  private static final char DEFAULT_FIELD_DELIM = '\u0001';
  // hive by default has no escape char
  public static final char DEFAULT_ESCAPE_CHAR = '\u0000';

  private final HdfsFileFormat fileFormat;

  private final char lineDelim;
  private final char fieldDelim;
  private final char collectionDelim;
  private final char mapKeyDelim;
  private final char escapeChar;
  private final char quoteChar;
  private final int blockSize;
  private final THdfsCompression compression;

  // Serde parameters that are recognized by table writers.
  private static final String BLOCK_SIZE = "blocksize";
  private static final String COMPRESSION = "compression";

  // Important: don't change the ordering of these keys - if e.g. FIELD_DELIM is not
  // found, the value of LINE_DELIM is used, so LINE_DELIM must be found first.
  final static List<String> DELIMITER_KEYS =
      ImmutableList.of(serdeConstants.LINE_DELIM, serdeConstants.FIELD_DELIM,
        serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
        serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR);

  // The Parquet serde shows up multiple times as the location of the implementation
  // has changed between Impala versions.
  final static List<String> COMPATIBLE_SERDES = ImmutableList.of(
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", // (seq / text / parquet)
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe", // (avro)
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", // (rc)
      ParquetHiveSerDe.class.getName()); // (parquet)

  private final static Logger LOG = LoggerFactory.getLogger(HdfsStorageDescriptor.class);

  /**
   * Returns a map from delimiter key to a single delimiter character,
   * filling in defaults if explicit values are not found in the supplied
   * serde descriptor.
   *
   * @throws InvalidStorageDescriptorException - if an invalid delimiter is found
   */
  private static Map<String, Character> extractDelimiters(SerDeInfo serdeInfo)
      throws InvalidStorageDescriptorException {
    // The metastore may return null for delimiter parameters,
    // which means we need to use a default instead.
    // We tried long and hard to find default values for delimiters in Hive,
    // but could not find them.
    Map<String, Character> delimMap = Maps.newHashMap();

    for (String delimKey: DELIMITER_KEYS) {
      String delimValue = serdeInfo.getParameters().get(delimKey);
      if (delimValue == null) {
        if (delimKey.equals(serdeConstants.FIELD_DELIM)) {
          delimMap.put(delimKey, DEFAULT_FIELD_DELIM);
        } else if (delimKey.equals(serdeConstants.ESCAPE_CHAR)) {
          delimMap.put(delimKey, DEFAULT_ESCAPE_CHAR);
        } else if (delimKey.equals(serdeConstants.LINE_DELIM)) {
          delimMap.put(delimKey, DEFAULT_LINE_DELIM);
        } else {
          delimMap.put(delimKey, delimMap.get(serdeConstants.FIELD_DELIM));
        }
      } else {
        if (delimValue.length() != 1) {
          throw new InvalidStorageDescriptorException("Invalid delimiter: " + delimKey +
              " has invalid length (" + delimValue + ")"
              + ". Already found: " + delimMap);
        }

        delimMap.put(delimKey, delimValue.charAt(0));
      }
    }

    return delimMap;
  }

  public char getLineDelim() {
    return lineDelim;
  }

  public char getFieldDelim() {
    return fieldDelim;
  }

  public char getCollectionDelim() {
    return collectionDelim;
  }

  public char getMapKeyDelim() {
    return mapKeyDelim;
  }

  public char getEscapeChar() {
    return escapeChar;
  }

  public char getQuoteChar() {
    return quoteChar;
  }

  public HdfsFileFormat getFileFormat() {
    return fileFormat;
  }

  public int getBlockSize(){
    return blockSize;
  }

  public THdfsCompression getCompression() {
    return compression;
  }

  public HdfsStorageDescriptor(String tblName, HdfsFileFormat fileFormat, char lineDelim,
      char fieldDelim, char collectionDelim, char mapKeyDelim, char escapeChar,
      char quoteChar, int blockSize, THdfsCompression compression) {
    this.fileFormat = fileFormat;
    this.lineDelim = lineDelim;
    this.fieldDelim = fieldDelim;
    this.collectionDelim = collectionDelim;
    this.mapKeyDelim = mapKeyDelim;
    this.quoteChar = quoteChar;
    this.blockSize = blockSize;
    this.compression = compression;

    // You can set the escape character as a tuple or row delim.  Empirically,
    // this is ignored by hive.
    if (escapeChar == fieldDelim ||
        escapeChar == lineDelim ||
        escapeChar == collectionDelim) {
      // TODO: we should output the table name here but it's hard to get to now.
      this.escapeChar = DEFAULT_ESCAPE_CHAR;
      LOG.warn("Escape character for table, " + tblName + " is set to "
          + "the same character as one of the delimiters.  Ignoring escape character.");
    } else {
      this.escapeChar = escapeChar;
    }
  }

  /**
   * Thrown when constructing an HdfsStorageDescriptor from an invalid
   * metatore storage descriptor.
   */
  public static class InvalidStorageDescriptorException extends CatalogException {
    // Mandatory since Exception implements Serialisable
    private static final long serialVersionUID = -555234913768134760L;
    public InvalidStorageDescriptorException(String s) { super(s); }
    public InvalidStorageDescriptorException(Exception ex) {
      super(ex.getMessage(), ex);
    }
  }

  /**
   * Constructs a new HdfsStorageDescriptor from a StorageDescriptor retrieved from the
   * metastore.
   *
   * @throws InvalidStorageDescriptorException - if the storage descriptor has invalid
   * delimiters, an unsupported SerDe, or an unknown file format.
   */
  public static HdfsStorageDescriptor fromStorageDescriptor(String tblName,
      StorageDescriptor sd)
      throws InvalidStorageDescriptorException {
    Map<String, Character> delimMap = extractDelimiters(sd.getSerdeInfo());
    if (!COMPATIBLE_SERDES.contains(sd.getSerdeInfo().getSerializationLib())) {
      throw new InvalidStorageDescriptorException(
          "Unsupported SerDe: " + sd.getSerdeInfo().getSerializationLib());
    }
    // Extract the blocksize and compression specification from the SerDe parameters,
    // if present.
    Map<String, String> parameters = sd.getSerdeInfo().getParameters();
    int blockSize = 0;
    String blockValue = parameters.get(BLOCK_SIZE);
    if (blockValue != null) {
      blockSize = Integer.parseInt(blockValue);
    }
    THdfsCompression compression = THdfsCompression.NONE;
    String compressionValue = parameters.get(COMPRESSION);
    if (compressionValue != null) {
      if (CatalogObjectsConstants.COMPRESSION_MAP.containsKey(compressionValue)) {
        compression = CatalogObjectsConstants.COMPRESSION_MAP.get(compressionValue);
      } else {
        LOG.warn("Unknown compression type: " + compressionValue);
      }
    }

    try {
      return new HdfsStorageDescriptor(tblName,
          HdfsFileFormat.fromJavaClassName(sd.getInputFormat()),
          delimMap.get(serdeConstants.LINE_DELIM),
          delimMap.get(serdeConstants.FIELD_DELIM),
          delimMap.get(serdeConstants.COLLECTION_DELIM),
          delimMap.get(serdeConstants.MAPKEY_DELIM),
          delimMap.get(serdeConstants.ESCAPE_CHAR),
          delimMap.get(serdeConstants.QUOTE_CHAR),
          blockSize, compression);
    } catch (IllegalArgumentException ex) {
      // Thrown by fromJavaClassName
      throw new InvalidStorageDescriptorException(ex);
    }
  }
}
