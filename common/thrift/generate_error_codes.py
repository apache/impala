#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# For readability purposes we define the error codes and messages at the top of the
# file. New codes and messages must be added here. Old error messages MUST NEVER BE
# DELETED, but can be renamed. The tuple layout for a new entry is: error code enum name,
# numeric error code, format string of the message.
#
# TODO Add support for SQL Error Codes
#      https://msdn.microsoft.com/en-us/library/ms714687%28v=vs.85%29.aspx
error_codes = (
  ("OK", 0, ""),

  ("UNUSED", 1, "<UNUSED>"),

  ("GENERAL", 2, "$0"),

  ("CANCELLED", 3, "$0"),

  ("ANALYSIS_ERROR", 4, "$0"),

  ("NOT_IMPLEMENTED_ERROR", 5, "$0"),

  ("RUNTIME_ERROR", 6, "$0"),

  ("MEM_LIMIT_EXCEEDED", 7, "$0"),

  ("INTERNAL_ERROR", 8, "$0"),

  ("RECOVERABLE_ERROR", 9, "$0"),

  ("PARQUET_MULTIPLE_BLOCKS", 10,
   "Parquet files should not be split into multiple hdfs-blocks. file=$0"),

  ("PARQUET_COLUMN_METADATA_INVALID", 11,
   "Column metadata states there are $0 values, but read $1 values from column $2. "
   "file=$3"),

  ("PARQUET_HEADER_PAGE_SIZE_EXCEEDED", 12, "(unused)"),

  ("PARQUET_HEADER_EOF", 13,
    "ParquetScanner: reached EOF while deserializing data page header. file=$0"),

  ("PARQUET_GROUP_ROW_COUNT_ERROR", 14,
    "Metadata states that in group $0($1) there are $2 rows, but $3 rows were read."),

  ("PARQUET_GROUP_ROW_COUNT_OVERFLOW", 15, "(unused)"),

  ("PARQUET_MISSING_PRECISION", 16,
   "File '$0' column '$1' does not have the decimal precision set."),

  ("PARQUET_WRONG_PRECISION", 17,
    "File '$0' column '$1' has a precision that does not match the table metadata "
    " precision. File metadata precision: $2, table metadata precision: $3."),

  ("PARQUET_BAD_CONVERTED_TYPE", 18,
   "File '$0' column '$1' does not have converted type set to DECIMAL"),

  ("PARQUET_INCOMPATIBLE_DECIMAL", 19,
   "File '$0' column '$1' contains decimal data but the table metadata has type $2"),

  ("SEQUENCE_SCANNER_PARSE_ERROR", 20,
   "Problem parsing file $0 at $1$2"),

  ("SNAPPY_DECOMPRESS_INVALID_BLOCK_SIZE", 21,
   "Decompressor: block size is too big.  Data is likely corrupt. Size: $0"),

  ("SNAPPY_DECOMPRESS_INVALID_COMPRESSED_LENGTH", 22,
   "Decompressor: invalid compressed length.  Data is likely corrupt."),

  ("SNAPPY_DECOMPRESS_UNCOMPRESSED_LENGTH_FAILED", 23,
   "Snappy: GetUncompressedLength failed"),

  ("SNAPPY_DECOMPRESS_RAW_UNCOMPRESS_FAILED", 24,
   "SnappyBlock: RawUncompress failed"),

  ("SNAPPY_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT", 25,
   "Snappy: Decompressed size is not correct."),

  ("FRAGMENT_EXECUTOR", 26, "Reserved resource size ($0) is larger than "
    "query mem limit ($1), and will be restricted to $1. Configure the reservation "
    "size by setting RM_INITIAL_MEM."),

  ("PARTITIONED_HASH_JOIN_MAX_PARTITION_DEPTH", 27,
   "Cannot perform join at hash join node with id $0."
   " The input data was partitioned the maximum number of $1 times."
   " This could mean there is significant skew in the data or the memory limit is"
   " set too low."),

  ("PARTITIONED_AGG_MAX_PARTITION_DEPTH", 28,
   "Cannot perform aggregation at hash aggregation node with id $0."
   " The input data was partitioned the maximum number of $1 times."
   " This could mean there is significant skew in the data or the memory limit is"
   " set too low."),

  ("MISSING_BUILTIN", 29, "Builtin '$0' with symbol '$1' does not exist. "
   "Verify that all your impalads are the same version."),

  ("RPC_GENERAL_ERROR", 30, "RPC Error: $0"),
  ("RPC_RECV_TIMEOUT", 31, "RPC recv timed out: dest address: $0, rpc: $1"),

  ("UDF_VERIFY_FAILED", 32,
   "Failed to verify function $0 from LLVM module $1, see log for more details."),

  ("PARQUET_CORRUPT_RLE_BYTES", 33, "File $0 corrupt. RLE level data bytes = $1"),

  ("AVRO_DECIMAL_RESOLUTION_ERROR", 34, "Column '$0' has conflicting Avro decimal types. "
   "Table schema $1: $2, file schema $1: $3"),

  ("AVRO_DECIMAL_METADATA_MISMATCH", 35, "Column '$0' has conflicting Avro decimal types. "
   "Declared $1: $2, $1 in table's Avro schema: $3"),

  ("AVRO_SCHEMA_RESOLUTION_ERROR", 36, "Unresolvable types for column '$0': "
   "table type: $1, file type: $2"),

  ("AVRO_SCHEMA_METADATA_MISMATCH", 37, "Unresolvable types for column '$0': "
   "declared column type: $1, table's Avro schema type: $2"),

  ("AVRO_UNSUPPORTED_DEFAULT_VALUE", 38, "Field $0 is missing from file and default "
   "values of type $1 are not yet supported."),

  ("AVRO_MISSING_FIELD", 39, "Inconsistent table metadata. Mismatch between column "
   "definition and Avro schema: cannot read field $0 because there are only $1 fields."),

  ("AVRO_MISSING_DEFAULT", 40,
   "Field $0 is missing from file and does not have a default value."),

  ("AVRO_NULLABILITY_MISMATCH", 41,
   "Field $0 is nullable in the file schema but not the table schema."),

  ("AVRO_NOT_A_RECORD", 42,
   "Inconsistent table metadata. Field $0 is not a record in the Avro schema."),

  ("PARQUET_DEF_LEVEL_ERROR", 43, "Could not read definition level, even though metadata"
   " states there are $0 values remaining in data page. file=$1"),

  ("PARQUET_NUM_COL_VALS_ERROR", 44, "Mismatched number of values in column index $0 "
   "($1 vs. $2). file=$3"),

  ("PARQUET_DICT_DECODE_FAILURE", 45, "File '$0' is corrupt: error decoding "
   "dictionary-encoded value of type $1 at offset $2"),

  ("SSL_PASSWORD_CMD_FAILED", 46,
   "SSL private-key password command ('$0') failed with error: $1"),

  ("SSL_CERTIFICATE_PATH_BLANK", 47, "The SSL certificate path is blank"),
  ("SSL_PRIVATE_KEY_PATH_BLANK", 48, "The SSL private key path is blank"),

  ("SSL_CERTIFICATE_NOT_FOUND", 49, "The SSL certificate file does not exist at path $0"),
  ("SSL_PRIVATE_KEY_NOT_FOUND", 50, "The SSL private key file does not exist at path $0"),

  ("SSL_SOCKET_CREATION_FAILED", 51, "SSL socket creation failed: $0"),

  ("MEM_ALLOC_FAILED", 52, "Memory allocation of $0 bytes failed"),

  ("PARQUET_REP_LEVEL_ERROR", 53, "Could not read repetition level, even though metadata"
   " states there are $0 values remaining in data page. file=$1"),

  ("PARQUET_UNRECOGNIZED_SCHEMA", 54, "File '$0' has an incompatible Parquet schema for "
   "column '$1'. Column type: $2, Parquet schema:\\n$3"),

  ("COLLECTION_ALLOC_FAILED", 55, "Failed to allocate $0 bytes for collection '$1'.\\n"
   "Current buffer size: $2 num tuples: $3."),

  ("TMP_DEVICE_BLACKLISTED", 56,
    "Temporary device for directory $0 is blacklisted from a previous error and cannot "
    "be used."),

  ("TMP_FILE_BLACKLISTED", 57,
    "Temporary file $0 is blacklisted from a previous error and cannot be expanded."),

  ("RPC_CLIENT_CONNECT_FAILURE", 58,
    "RPC client failed to connect: $0"),

  ("STALE_METADATA_FILE_TOO_SHORT", 59, "Metadata for file '$0' appears stale. "
   "Try running \\\"refresh $1\\\" to reload the file metadata."),

  ("PARQUET_BAD_VERSION_NUMBER", 60, "File '$0' has an invalid version number: $1\\n"
   "This could be due to stale metadata. Try running \\\"refresh $2\\\"."),

  ("SCANNER_INCOMPLETE_READ", 61, "Tried to read $0 bytes but could only read $1 bytes. "
   "This may indicate data file corruption. (file $2, byte offset: $3)"),

  ("SCANNER_INVALID_READ", 62, "Invalid read of $0 bytes. This may indicate data file "
   "corruption. (file $1, byte offset: $2)"),

  ("AVRO_BAD_VERSION_HEADER", 63, "File '$0' has an invalid version header: $1\\n"
   "Make sure the file is an Avro data file."),

  ("UDF_MEM_LIMIT_EXCEEDED", 64, "$0's allocations exceeded memory limits."),

  ("UNUSED_65", 65, "No longer in use."),

  ("COMPRESSED_FILE_MULTIPLE_BLOCKS", 66,
   "For better performance, snappy-, gzip-, and bzip-compressed files "
   "should not be split into multiple HDFS blocks. file=$0 offset $1"),

  ("COMPRESSED_FILE_BLOCK_CORRUPTED", 67,
   "$0 Data error, likely data corrupted in this block."),

  ("COMPRESSED_FILE_DECOMPRESSOR_ERROR", 68, "$0 Decompressor error at $1, code=$2"),

  ("COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS", 69,
   "Decompression failed to make progress, but end of input is not reached. "
   "File appears corrupted. file=$0"),

  ("COMPRESSED_FILE_TRUNCATED", 70,
   "Unexpected end of compressed file. File may be truncated. file=$0"),

  ("DATASTREAM_SENDER_TIMEOUT", 71, "Sender$0 timed out waiting for receiver fragment "
   "instance: $1, dest node: $2"),

  ("KUDU_IMPALA_TYPE_MISSING", 72, "Kudu type $0 is not available in Impala."),

  ("IMPALA_KUDU_TYPE_MISSING", 73, "Impala type $0 is not available in Kudu."),

  ("KUDU_NOT_SUPPORTED_ON_OS", 74, "Kudu is not supported on this operating system."),

  ("KUDU_NOT_ENABLED", 75, "Kudu features are disabled by the startup flag "
   "--disable_kudu."),

  ("PARTITIONED_HASH_JOIN_REPARTITION_FAILS", 76, "Cannot perform hash join at node with "
   "id $0. Repartitioning did not reduce the size of a spilled partition. Repartitioning "
   "level $1. Number of rows $2:\\n$3\\n$4"),

  ("UNUSED_77", 77,  "Not in use."),

  ("AVRO_TRUNCATED_BLOCK", 78, "File '$0' is corrupt: truncated data block at offset $1"),

  ("AVRO_INVALID_UNION", 79, "File '$0' is corrupt: invalid union value $1 at offset $2"),

  ("AVRO_INVALID_BOOLEAN", 80, "File '$0' is corrupt: invalid boolean value $1 at offset "
   "$2"),

  ("AVRO_INVALID_LENGTH", 81, "File '$0' is corrupt: invalid length $1 at offset $2"),

  ("SCANNER_INVALID_INT", 82, "File '$0' is corrupt: invalid encoded integer at offset $1"),

  ("AVRO_INVALID_RECORD_COUNT", 83, "File '$0' is corrupt: invalid record count $1 at "
   "offset $2"),

  ("AVRO_INVALID_COMPRESSED_SIZE", 84, "File '$0' is corrupt: invalid compressed block "
   "size $1 at offset $2"),

  ("AVRO_INVALID_METADATA_COUNT", 85, "File '$0' is corrupt: invalid metadata count $1 "
   "at offset $2"),

  ("SCANNER_STRING_LENGTH_OVERFLOW", 86, "File '$0' could not be read: string $1 was "
    "longer than supported limit of $2 bytes at offset $3"),

  ("PARQUET_CORRUPT_PLAIN_VALUE", 87, "File '$0' is corrupt: error decoding value of type "
   "$1 at offset $2"),

  ("PARQUET_CORRUPT_DICTIONARY", 88, "File '$0' is corrupt: error reading dictionary for "
   "data of type $1: $2"),

  ("TEXT_PARSER_TRUNCATED_COLUMN", 89, "Length of column is $0 which exceeds maximum "
   "supported length of 2147483647 bytes."),

  ("SCRATCH_LIMIT_EXCEEDED", 90, "Scratch space limit of $0 bytes exceeded for query "
   "while spilling data to disk on backend $1."),

  ("BUFFER_ALLOCATION_FAILED", 91, "Unexpected error allocating $0 byte buffer: $1"),

  ("PARQUET_ZERO_ROWS_IN_NON_EMPTY_FILE", 92, "File '$0' is corrupt: metadata indicates "
   "a zero row count but there is at least one non-empty row group."),

  ("NO_REGISTERED_BACKENDS", 93, "Cannot schedule query: no registered backends "
   "available."),

  ("KUDU_KEY_ALREADY_PRESENT", 94, "Key already present in Kudu table '$0'."),

  ("KUDU_NOT_FOUND", 95, "Not found in Kudu table '$0': $1"),

  ("KUDU_SESSION_ERROR", 96, "Error in Kudu table '$0': $1"),

  ("AVRO_UNSUPPORTED_TYPE", 97, "Column '$0': unsupported Avro type '$1'"),

  ("AVRO_INVALID_DECIMAL", 98,
      "Column '$0': invalid Avro decimal type with precision = '$1' scale = '$2'"),

  ("KUDU_NULL_CONSTRAINT_VIOLATION", 99,
      "Row with null value violates nullability constraint on table '$0'."),

  ("PARQUET_TIMESTAMP_OUT_OF_RANGE", 100,
   "Parquet file '$0' column '$1' contains an out of range timestamp. "
   "The valid date range is 1400-01-01..9999-12-31."),

  # TODO: IMPALA-4697: the merged errors do not show up in the query error log,
  # so we must point users to the impalad error log.
  ("SCRATCH_ALLOCATION_FAILED", 101, "Could not create files in any configured scratch "
   "directories (--scratch_dirs=$0) on backend '$1'. See logs for previous errors that may "
   "have prevented creating or writing scratch files."),

  ("SCRATCH_READ_TRUNCATED", 102, "Error reading $0 bytes from scratch file '$1' "
   "on backend $2 at offset $3: could only read $4 bytes"),

  ("KUDU_TIMESTAMP_OUT_OF_RANGE", 103,
   "Kudu table '$0' column '$1' contains an out of range timestamp. "
   "The valid date range is 1400-01-01..9999-12-31."),

  ("MAX_ROW_SIZE", 104, "Row of size $0 could not be materialized in plan node with "
    "id $1. Increase the max_row_size query option (currently $2) to process larger rows."),

  ("IR_VERIFY_FAILED", 105,
   "Failed to verify generated IR function $0, see log for more details."),

  ("MINIMUM_RESERVATION_UNAVAILABLE", 106, "Failed to get minimum memory reservation of "
     "$0 on daemon $1:$2 for query $3 due to following error: $4Memory is likely "
     "oversubscribed. Reducing query concurrency or configuring admission control may "
     "help avoid this error."),

  ("ADMISSION_REJECTED", 107, "Rejected query from pool $0: $1"),

  ("ADMISSION_TIMED_OUT", 108, "Admission for query exceeded timeout $0ms in pool $1. "
     "Queued reason: $2"),

  ("THREAD_CREATION_FAILED", 109, "Failed to create thread $0 in category $1: $2"),

  ("DISK_IO_ERROR", 110, "Disk I/O error: $0"),

  ("DATASTREAM_RECVR_CLOSED", 111,
   "DataStreamRecvr for fragment=$0, node=$1 is closed already"),

  ("BAD_PRINCIPAL_FORMAT", 112,
    "Kerberos principal should be of the form: <service>/<hostname>@<realm> - got: $0"),

  ("LZ4_COMPRESSION_INPUT_TOO_LARGE", 113,
   "The input size is too large for LZ4 compression: $0"),

  ("SASL_APP_NAME_MISMATCH", 114,
   "InitAuth() called multiple times with different names. Was called with $0. "
   "Now using $1."),

  ("PARQUET_BIT_PACKED_LEVELS", 115,
      "Can not read Parquet file $0 with deprecated BIT_PACKED encoding for rep or "
      "def levels. Support was removed in Impala 3.0 - see IMPALA-6077."),

  ("ROW_BATCH_TOO_LARGE", 116,
   "Row batch cannot be serialized: size of $0 bytes exceeds supported limit of $1"),
)

import sys
import os

# Verifies the uniqueness of the error constants and numeric error codes.
# Numeric codes must start from 0, be in order and have no gaps
def check_duplicates(codes):
  constants = {}
  next_num_code = 0
  for row in codes:
    if row[0] in constants:
      print("Constant %s already used, please check definition of '%s'!" % \
            (row[0], constants[row[0]]))
      exit(1)
    if row[1] != next_num_code:
      print("Numeric error codes must start from 0, be in order, and not have any gaps: "
            "got %d, expected %d" % (row[1], next_num_code))
      exit(1)
    next_num_code += 1
    constants[row[0]] = row[2]

preamble = """
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
//
//
// THIS FILE IS AUTO GENERATED BY generate_error_codes.py DO NOT MODIFY
// IT BY HAND.
//

namespace cpp impala
namespace java org.apache.impala.thrift

"""
# The script will always generate the file, CMake will take care of running it only if
# necessary.
target_file = "ErrorCodes.thrift"

# Check uniqueness of error constants and numeric codes
check_duplicates(error_codes)

fid = open(target_file, "w+")
try:
  fid.write(preamble)
  fid.write("""\nenum TErrorCode {\n""")
  fid.write(",\n".join(map(lambda x: "  %s = %d" % (x[0], x[1]), error_codes)))
  fid.write("\n}")
  fid.write("\n")
  fid.write("const list<string> TErrorMessage = [\n")
  fid.write(",\n".join(map(lambda x: "  // %s\n  \"%s\"" %(x[0], x[2]), error_codes)))
  fid.write("\n]")
finally:
  fid.close()

print("%s created." % target_file)
