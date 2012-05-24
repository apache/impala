// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_TREVNI_DEF_H
#define IMPALA_EXEC_TREVNI_DEF_H

#include <boost/algorithm/string.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/unordered_map.hpp>

// This file contains common elements between the Trenvi Writer and Scanner.
// Trevni defines the following data types used in the file format:
//   Null, requires zero Bytes. Sometimes used in array columns.
//   Long 64-bit signed values, represented in zig-zag format.
//   Int, like Long, but restricted to 32-bit signed values
//   Fixed32 32-bit values stored as four Bytes, little-endian.
//   Fixed64 64-bit values stored as eight Bytes, little-endian.
//   Float 32-bit IEEE floating point value, little-endian
//   Double 64-bit IEEE floating point value, little-endian
//   String encoded as Long followed by that many Bytes
//   Bytes as above, may be used to encapsulate more complex objects
//
//   The following type is used internally by the implementation:
//   IntegerArray column data is augmented with arrays of variable sized
//   integers to represent // repeated, nested and optional columns as in Dremel
//   (http://sergey.melnix.com/pub/melnik_VLDB10.pdf).
//
// The following is a pseudo-BNF grammar for Trevni file formats. Comments are prefixed
// with dashes:
//
// trevni ::=
//   <file-header>
//   <column>+
//
// file-header ::=
//   <file-version-header>
//   <row-count>
//   <column-count>
//   <file-metadata>
//   <column-metadata>+
//   <column-start>+
//
// file-version-header ::= Byte[4] {'T', 'r', 'v', 1}
// row-count ::= Fixed64
// column-count ::= Fixed32
// column-start ::= Fixed64
//
// file-metadata ::= <metadata>
// column-metadata ::= <metadata>
//
// column ::=
//   <block-count>
//   <block-descriptor>+
//   <block>+
//
// block-count ::= Fixed32
//
// block-descriptor ::=
//   <block-row-count>
//   <block-uncompressed-size>
//   <block-compressed-size>
//   [ <first-value> ]
//
// block-row-count ::= Fixed32
// block-uncompressed-size ::= Fixed32
// block-compressed-size ::= Fixed32
//
// first-value := -- First value in column in type of column
//
// block ::=
//   <column-values>*
//   [<definition-array>
//   [<repetition-array> ]]
//   [ <column-checksum> ]
//
// definition-array ::= IntegerArray
// repetition-array ::= IntegerArray
// column-values ::= -- Serialized column values
//
// -- A collection of key-value pairs defining metadata values for the file.
// -- Text key and value pairs.
// metadata ::=
//   <meta-count>
//   <metadata-pair>*
//
// meta-count ::= Long
//
// metadata-pair ::=
//   String
//   Bytes
//

namespace impala {
// Trevni version string.
static const uint8_t TREVNI_VERSION_HEADER[4] = {'T', 'r', 'v', 1};

// Column and file metadata key names
static const std::string TREVNI_CODEC = "trevni.codec";
static const std::string TREVNI_CHECKSUM = "trevni.checksum";
static const std::string TREVNI_NAME = "trevni.name";
static const std::string TREVNI_TYPE = "trevni.type";
static const std::string TREVNI_VALUES = "trevni.values";
static const std::string TREVNI_ARRAY = "trevni.array";
static const std::string TREVNI_PARENT = "trevni.parent";
static const std::string TREVNI_REPETITION = "trevni.repetition";
static const std::string TREVNI_DEFINITION = "trevni.definition";

// Types defined by Trevni.
enum TrevniType {
  TREVNI_UNDEFINED = 0,
  TREVNI_INT,
  TREVNI_LONG,
  TREVNI_BOOL,
  TREVNI_FIXED8,
  TREVNI_FIXED16,
  TREVNI_FIXED32,
  TREVNI_FIXED64,
  TREVNI_FLOAT,
  TREVNI_DOUBLE,
  TREVNI_TIMESTAMP,
  TREVNI_STRING,
  TREVNI_BYTES
};

// Return the on disk size of a Trevni data type. 0 implies variable length.
inline int GetTrevniTypeLength(TrevniType type) {
  switch (type) {
    default:
    case TREVNI_UNDEFINED:
      DCHECK(false);
      return -1;
    case TREVNI_STRING:
    case TREVNI_BYTES:
    case TREVNI_INT:
    case TREVNI_LONG:
    // Bool is special cased to be stored as a bit array.
    case TREVNI_BOOL:
      return 0;
    case TREVNI_FIXED8:
      return 1;
    case TREVNI_FIXED16:
      return 2;
    case TREVNI_FIXED32:
    case TREVNI_FLOAT:
      return 4;
    case TREVNI_FIXED64:
    case TREVNI_DOUBLE:
      return 8;
    case TREVNI_TIMESTAMP:
      return 12;
  }
  return (0);
}

// Map of recognized file meta data
enum FileMeta {
  CODEC,
  CHECKSUM
};

static const std::map<const std::string, FileMeta> file_meta_map = boost::assign::map_list_of
  (TREVNI_CODEC, CODEC)
  (TREVNI_CHECKSUM, CHECKSUM);

// Map of recognized column meta data
enum ColumnMeta {
  COL_CODEC,        // Column specific compression codec.
  COL_NAME,         // Name of column
  COL_TYPE,         // Type of column
  COL_VALUES,       // Indicates first value of block is stored.
  COL_ARRAY,        // Trevni: is array
  COL_PARENT,       // Trevni: name of parent column, if any
  COL_REPETITION,     // Impala: Max repetition level, 0 if absent
  COL_DEFINITION    // Impala: Max definition level, 0 if absent. > 0 implies nullable.
};

static const std::map<const std::string, ColumnMeta> column_meta_map = boost::assign::map_list_of
  (TREVNI_CODEC, COL_CODEC)
  (TREVNI_NAME, COL_NAME)
  (TREVNI_TYPE, COL_TYPE)
  (TREVNI_VALUES, COL_VALUES)
  (TREVNI_ARRAY, COL_ARRAY)
  (TREVNI_PARENT, COL_PARENT)
  (TREVNI_REPETITION, COL_REPETITION)
  (TREVNI_DEFINITION, COL_DEFINITION);

// Map of recognized checksum names
enum Checksum {
  NONE,
  CRC32
};

static const std::map<const std::string, Checksum> checksum_map = boost::assign::map_list_of
  ("null", NONE)
  ("crc-32", CRC32);

// Maps of types defined by Trevni.
static const std::map<const std::string, TrevniType> type_map = boost::assign::map_list_of
  ("int", TREVNI_INT)
  ("long", TREVNI_LONG)
  ("boolean", TREVNI_BOOL)
  ("fixed8", TREVNI_FIXED8)
  ("fixed16", TREVNI_FIXED16)
  ("fixed32", TREVNI_FIXED32)
  ("fixed64", TREVNI_FIXED64)
  ("float", TREVNI_FLOAT)
  ("double", TREVNI_DOUBLE)
  ("timestamp", TREVNI_TIMESTAMP)
  ("string", TREVNI_STRING)
  ("bytes", TREVNI_BYTES);

// Map Impala types to Trevni types.
static const std::map<PrimitiveType, TrevniType> type_map_trevni = boost::assign::map_list_of
  (TYPE_BOOLEAN, TREVNI_BOOL)
  (TYPE_TINYINT, TREVNI_FIXED8)
  (TYPE_SMALLINT, TREVNI_FIXED16)
  (TYPE_INT, TREVNI_INT)
  (TYPE_BIGINT, TREVNI_LONG)
  (TYPE_FLOAT, TREVNI_FLOAT)
  (TYPE_DOUBLE, TREVNI_DOUBLE)
  (TYPE_TIMESTAMP, TREVNI_TIMESTAMP)
  (TYPE_STRING, TREVNI_STRING);

// Map of types to string names defined by Trevni.
static const std::map<TrevniType, const std::string> type_map_string = boost::assign::map_list_of
  (TREVNI_INT, "int")
  (TREVNI_LONG, "long")
  (TREVNI_BOOL, "boolean")
  (TREVNI_FIXED8, "fixed8")
  (TREVNI_FIXED16, "fixed16")
  (TREVNI_FIXED32, "fixed32")
  (TREVNI_FIXED64, "fixed64")
  (TREVNI_FLOAT, "float")
  (TREVNI_DOUBLE, "double")
  (TREVNI_TIMESTAMP, "timestamp")
  (TREVNI_STRING, "string")
  (TREVNI_BYTES, "bytes");

}
#endif
