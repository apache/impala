// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.Constants;

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
  private static final char DEFAULT_ESCAPE_CHAR = '\u0000';

  private final HdfsFileFormat fileFormat;

  private final char lineDelim;
  private final char fieldDelim;
  private final char collectionDelim;
  private final char mapKeyDelim;
  private final char escapeChar;
  private final char quoteChar;

  // Important: don't change the ordering of these keys - if e.g. FIELD_DELIM is not
  // found, the value of LINE_DELIM is used, so LINE_DELIM must be found first.
  final static List<String> DELIMITER_KEYS =
      ImmutableList.of(Constants.LINE_DELIM, Constants.FIELD_DELIM,
        Constants.COLLECTION_DELIM, Constants.MAPKEY_DELIM, Constants.ESCAPE_CHAR,
        Constants.QUOTE_CHAR);

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
        if (delimKey.equals(Constants.FIELD_DELIM)) {
          delimMap.put(delimKey, DEFAULT_FIELD_DELIM);
        } else if (delimKey.equals(Constants.ESCAPE_CHAR)) {
          delimMap.put(delimKey, DEFAULT_ESCAPE_CHAR);
        } else if (delimKey.equals(Constants.LINE_DELIM)) {
          delimMap.put(delimKey, DEFAULT_LINE_DELIM);
        } else {
          delimMap.put(delimKey, delimMap.get(Constants.FIELD_DELIM));
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

  public HdfsStorageDescriptor(HdfsFileFormat fileFormat, char lineDelim,
      char fieldDelim, char collectionDelim, char mapKeyDelim, char escapeChar,
      char quoteChar) {
    this.fileFormat = fileFormat;
    this.lineDelim = lineDelim;
    this.fieldDelim = fieldDelim;
    this.collectionDelim = collectionDelim;
    this.mapKeyDelim = mapKeyDelim;
    this.escapeChar = escapeChar;
    this.quoteChar = quoteChar;
  }

  /**
   * Thrown when constructing an HdfsStorageDescriptor from an invalid
   * metatore storage descriptor.
   */
  public static class InvalidStorageDescriptorException extends Exception {
    // Mandatory since Exception implements Serialisable
    private static final long serialVersionUID = -555234913768134760L;
    public InvalidStorageDescriptorException(String s) { super(s); }
    public InvalidStorageDescriptorException(Exception ex) { super(ex); }
  }

  /**
   * Constructs a new HdfsStorageDescriptor from a StorageDescriptor retrieved from the
   * metastore.
   *
   * @throws InvalidStorageDescriptorException - if the storage descriptor has invalid
   * delimiters, or an unknown file format.
   */
  public static HdfsStorageDescriptor fromStorageDescriptor(StorageDescriptor sd)
      throws InvalidStorageDescriptorException {
    Map<String, Character> delimMap = extractDelimiters(sd.getSerdeInfo());
    try {
      return new HdfsStorageDescriptor(
          HdfsFileFormat.fromJavaClassName(sd.getInputFormat()),
          delimMap.get(Constants.LINE_DELIM), delimMap.get(Constants.FIELD_DELIM),
          delimMap.get(Constants.COLLECTION_DELIM), delimMap.get(Constants.MAPKEY_DELIM),
          delimMap.get(Constants.ESCAPE_CHAR), delimMap.get(Constants.QUOTE_CHAR));
    } catch (IllegalArgumentException ex) {
      // Thrown by fromJavaClassName
      throw new InvalidStorageDescriptorException(ex);
    }
  }
}
