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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsStorageDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.errorprone.annotations.Immutable;

/**
 * Represents the file format metadata for files stored in a table or partition.
 *
 * This class is immutable, and instances are stored in a global interner, since
 * the number of distinct combinations of file formats and other parameters
 * is typically quite low relative to the total number of partitions (e.g. almost
 * all partitions will use the default quoting).
 */
@Immutable
public class HdfsStorageDescriptor {
  public static final char DEFAULT_LINE_DELIM = '\n';
  // hive by default uses ctrl-a as field delim
  public static final char DEFAULT_FIELD_DELIM = '\u0001';
  // hive by default has no escape char
  public static final char DEFAULT_ESCAPE_CHAR = '\u0000';

  // Serde parameters that are recognized by table writers.
  private static final String BLOCK_SIZE = "blocksize";

  // Important: don't change the ordering of these keys - if e.g. FIELD_DELIM is not
  // found, the value of LINE_DELIM is used, so LINE_DELIM must be found first.
  // Package visible for testing.
  final static ImmutableList<String> DELIMITER_KEYS = ImmutableList.of(
      serdeConstants.LINE_DELIM, serdeConstants.FIELD_DELIM,
      serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
      serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR);

  // The Parquet serde shows up multiple times as the location of the implementation
  // has changed between Impala versions.
  final static ImmutableList<String> COMPATIBLE_SERDES = ImmutableList.of(
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", // (seq / text / parquet)
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe", // (avro)
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", // (rc)
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde", // (orc)
      "parquet.hive.serde.ParquetHiveSerDe", // (parquet - legacy)
      // TODO: Verify the following Parquet SerDe works with Impala and add
      // support for the new input/output format classes. See IMPALA-4214.
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", // (parquet)
      "org.apache.iceberg.mr.hive.HiveIcebergSerDe", // (iceberg)
      "org.apache.hadoop.hive.serde2.JsonSerDe");// (json)

  private final static Logger LOG = LoggerFactory.getLogger(HdfsStorageDescriptor.class);

  private final static Interner<HdfsStorageDescriptor> INTERNER =
      Interners.newWeakInterner();

  private final HdfsFileFormat fileFormat_;
  private final byte lineDelim_;
  private final byte fieldDelim_;
  private final byte collectionDelim_;
  private final byte mapKeyDelim_;
  private final byte escapeChar_;
  private final byte quoteChar_;
  private final int blockSize_;

  /**
   * Returns a map from delimiter key to a single delimiter character,
   * filling in defaults if explicit values are not found in the supplied
   * serde descriptor.
   *
   * @throws InvalidStorageDescriptorException - if an invalid delimiter is found
   */
  private static Map<String, Byte> extractDelimiters(SerDeInfo serdeInfo)
      throws InvalidStorageDescriptorException {
    // The metastore may return null for delimiter parameters,
    // which means we need to use a default instead.
    // We tried long and hard to find default values for delimiters in Hive,
    // but could not find them.
    Map<String, Byte> delimMap = new HashMap<>();

    for (String delimKey: DELIMITER_KEYS) {
      String delimValue = serdeInfo.getParameters().get(delimKey);
      if (delimValue == null) {
        if (delimKey.equals(serdeConstants.FIELD_DELIM)) {
          delimMap.put(delimKey, (byte) DEFAULT_FIELD_DELIM);
        } else if (delimKey.equals(serdeConstants.ESCAPE_CHAR)) {
          delimMap.put(delimKey, (byte) DEFAULT_ESCAPE_CHAR);
        } else if (delimKey.equals(serdeConstants.LINE_DELIM)) {
          delimMap.put(delimKey, (byte) DEFAULT_LINE_DELIM);
        } else {
          delimMap.put(delimKey, delimMap.get(serdeConstants.FIELD_DELIM));
        }
      } else {
        Byte delimByteValue = parseDelim(delimValue);
        if (delimByteValue == null) {
          throw new InvalidStorageDescriptorException("Invalid delimiter: '" +
              delimValue + "'. Delimiter must be specified as a single character or " +
              "as a decimal value in the range [-128:127]");
        }
        delimMap.put(delimKey, parseDelim(delimValue));
      }
    }
    return delimMap;
  }

  /**
   * Parses a delimiter in a similar way as Hive, with some additional error checking.
   * A delimiter must fit in a single byte and can be specified in the following
   * formats, as far as I can tell (there isn't documentation):
   * - A single ASCII or unicode character (ex. '|')
   * - An escape character in octal format (ex. \001. Stored in the metastore as a
   *   unicode character: \u0001).
   * - A signed decimal integer in the range [-128:127]. Used to support delimiters
   *   for ASCII character values between 128-255 (-2 maps to ASCII 254).
   *
   * The delimiter is first parsed as a decimal number. If the parsing succeeds AND
   * the resulting value fits in a signed byte, the byte value of the parsed int is
   * returned. Otherwise, if the string has a single char, the byte value of this
   * char is returned.
   * If the delimiter is invalid, null will be returned.
   */
  public static Byte parseDelim(String delimVal) {
    Preconditions.checkNotNull(delimVal);
    try {
      // In the future we could support delimiters specified in hex format, but we would
      // need support from the Hive side.
      return Byte.parseByte(delimVal);
    } catch (NumberFormatException e) {
      if (delimVal.length() == 1) {
        // Adding additional check as Java chars are two bytes.
        // e.g. \u1111 as delimVal will return a valid byte '11'
        int cp = Character.codePointAt(delimVal, 0);
        if (cp >= 0 && cp <= 255) return (byte) cp;
      }
    }
    return null;
  }

  private HdfsStorageDescriptor(String tblName, HdfsFileFormat fileFormat, byte lineDelim,
      byte fieldDelim, byte collectionDelim, byte mapKeyDelim, byte escapeChar,
      byte quoteChar, int blockSize) {
    this.fileFormat_ = fileFormat;
    this.lineDelim_ = lineDelim;
    this.fieldDelim_ = fieldDelim;
    this.collectionDelim_ = collectionDelim;
    this.mapKeyDelim_ = mapKeyDelim;
    this.quoteChar_ = quoteChar;
    this.blockSize_ = blockSize;

    // You can set the escape character as a tuple or row delim.  Empirically,
    // this is ignored by hive.
    if (escapeChar == fieldDelim ||
        escapeChar == lineDelim ||
        escapeChar == collectionDelim) {
      this.escapeChar_ = DEFAULT_ESCAPE_CHAR;
      LOG.warn("Escape character for table, " + tblName + " is set to "
          + "the same character as one of the delimiters.  Ignoring escape character.");
    } else {
      this.escapeChar_ = escapeChar;
    }
  }

  /**
   * Thrown when constructing an HdfsStorageDescriptor from an invalid/unsupported
   * metastore storage descriptor.
   * TODO: Get rid of this class.
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
    Map<String, Byte> delimMap = extractDelimiters(sd.getSerdeInfo());
    if (!COMPATIBLE_SERDES.contains(sd.getSerdeInfo().getSerializationLib())) {
      throw new InvalidStorageDescriptorException(String.format("Impala does not " +
          "support tables of this type. REASON: SerDe library '%s' is not " +
          "supported.", sd.getSerdeInfo().getSerializationLib()));
    }
    // Extract the blocksize and compression specification from the SerDe parameters,
    // if present.
    Map<String, String> parameters = sd.getSerdeInfo().getParameters();
    int blockSize = 0;
    String blockValue = parameters.get(BLOCK_SIZE);
    if (blockValue != null) {
      blockSize = Integer.parseInt(blockValue);
    }

    try {
      return INTERNER.intern(new HdfsStorageDescriptor(tblName,
          HdfsFileFormat.fromJavaClassName(
              sd.getInputFormat(), sd.getSerdeInfo().getSerializationLib()),
          delimMap.get(serdeConstants.LINE_DELIM),
          delimMap.get(serdeConstants.FIELD_DELIM),
          delimMap.get(serdeConstants.COLLECTION_DELIM),
          delimMap.get(serdeConstants.MAPKEY_DELIM),
          delimMap.get(serdeConstants.ESCAPE_CHAR),
          delimMap.get(serdeConstants.QUOTE_CHAR),
          blockSize));
    } catch (IllegalArgumentException ex) {
      // Thrown by fromJavaClassName
      throw new InvalidStorageDescriptorException(ex);
    }
  }

  public static HdfsStorageDescriptor fromThrift(THdfsStorageDescriptor tDesc,
      String tableName) {
    return INTERNER.intern(new HdfsStorageDescriptor(tableName,
        HdfsFileFormat.fromThrift(tDesc.getFileFormat()), tDesc.lineDelim,
        tDesc.fieldDelim, tDesc.collectionDelim, tDesc.mapKeyDelim, tDesc.escapeChar,
        tDesc.quoteChar, tDesc.blockSize));
  }

  public THdfsStorageDescriptor toThrift() {
    return new THdfsStorageDescriptor(lineDelim_, fieldDelim_, collectionDelim_,
        mapKeyDelim_, escapeChar_, quoteChar_, fileFormat_.toThrift(), blockSize_);
  }

  public HdfsStorageDescriptor cloneWithChangedFileFormat(HdfsFileFormat newFormat) {
    return INTERNER.intern(new HdfsStorageDescriptor(
        "<unknown>", newFormat, lineDelim_, fieldDelim_, collectionDelim_, mapKeyDelim_,
        escapeChar_, quoteChar_, blockSize_));
  }

  public byte getLineDelim() { return lineDelim_; }
  public byte getFieldDelim() { return fieldDelim_; }
  public byte getCollectionDelim() { return collectionDelim_; }
  public byte getMapKeyDelim() { return mapKeyDelim_; }
  public byte getEscapeChar() { return escapeChar_; }
  public HdfsFileFormat getFileFormat() { return fileFormat_; }
  public int getBlockSize() { return blockSize_; }

  @Override
  public int hashCode() {
    return Objects.hash(blockSize_, collectionDelim_, escapeChar_, fieldDelim_,
        fileFormat_, lineDelim_, mapKeyDelim_, quoteChar_);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    HdfsStorageDescriptor other = (HdfsStorageDescriptor) obj;
    if (blockSize_ != other.blockSize_) return false;
    if (collectionDelim_ != other.collectionDelim_) return false;
    if (escapeChar_ != other.escapeChar_) return false;
    if (fieldDelim_ != other.fieldDelim_) return false;
    if (fileFormat_ != other.fileFormat_) return false;
    if (lineDelim_ != other.lineDelim_) return false;
    if (mapKeyDelim_ != other.mapKeyDelim_) return false;
    if (quoteChar_ != other.quoteChar_) return false;
    return true;
  }
}
