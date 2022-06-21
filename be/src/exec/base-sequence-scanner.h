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


#ifndef IMPALA_EXEC_BASE_SEQUENCE_SCANNER_H
#define IMPALA_EXEC_BASE_SEQUENCE_SCANNER_H

#include <vector>
#include <memory>
#include <stdint.h>

#include "exec/hdfs-scanner.h"

namespace impala {

struct HdfsFileDesc;
class ScannerContext;

/// Superclass for all sequence container based file formats: SequenceFile, RCFile, Avro.
/// Sequence container formats have sync markers periodically in the file. This scanner
/// recovers from corrupt or otherwise non-parsable data blocks by skipping to the next
/// sync marker in the range.
///
/// Handling of sync markers:
/// A scanner is responsible for the data with the first complete sync in its scan range
/// through the first complete sync in the next range. 'eos_' is set to true when this
/// scanner has processed all the bytes it is responsible for, i.e., when it reads a sync
/// occurring completely in the next scan range, as this is the first sync that the next
/// scan range will be able to locate. Note that checking context_->eosr() after reading
/// each sync is insufficient for determining 'eos_'. If a sync marker spans two scan
/// ranges, the first scan range must process the following block since the second scan
/// range cannot find the incomplete sync. context_->eosr() will not alert us to this
/// situation, causing the block to be incorrectly skipped.
class BaseSequenceScanner : public HdfsScanner {
 public:
  /// Issue the initial ranges for all sequence container files. 'files' must not be
  /// empty.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
                                   const std::vector<HdfsFileDesc*>& files)
                                   WARN_UNUSED_RESULT;

  /// Returns true if 'format' uses a scanner derived from BaseSequenceScanner.
  static bool FileFormatIsSequenceBased(THdfsFileFormat::type format);

  virtual Status Open(ScannerContext* context) WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch);

  virtual ~BaseSequenceScanner();

 protected:
  /// Size of the sync hash field.
  const static int SYNC_HASH_SIZE = 16;

  /// Data that is shared between scan ranges of the same file.  The subclass is
  /// responsible for filling in all these fields in ReadFileHeader
  struct FileHeader {
    virtual ~FileHeader() {}

    /// The sync hash for this file.
    uint8_t sync[SYNC_HASH_SIZE];

    /// true if the file is compressed
    bool is_compressed;

    /// Codec name if it is compressed
    std::string codec;

    /// Enum for compression type.
    THdfsCompression::type compression_type;

    /// Byte size of header. This must not include the sync directly preceding the data
    /// (even if the sync is considered part of the header in the file format spec).
    int64_t header_size;
  };

  virtual Status GetNextInternal(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Subclasses must implement these functions.  The order for calls will be
  ///  1. AllocateFileHeader() - called once per file
  ///  2. ReadFileHeader() - called once per file
  ///  3. InitNewRange() - called once per scan range
  ///  4. ProcessRange()* - called until eos, bytes may be skipped between calls to
  ///                       recover from parse errors

  /// Allocate a file header object for this scanner.  If the scanner needs
  /// additional header information, it should subclass FileHeader.
  /// The allocated object will be placed in the scan node's pool.
  virtual FileHeader* AllocateFileHeader() = 0;

  /// Read the file header.  The underlying ScannerContext is at the start of
  /// the file header.  This function must read the file header (which advances
  /// context_ past it) and initialize header_.
  virtual Status ReadFileHeader() WARN_UNUSED_RESULT = 0;

  /// Materializes tuples into 'row_batch' by reading from the underlying ScannerContext.
  /// Assumes that the 'stream_' is positioned in the data portion of the range, i.e.,
  /// not at a sync marker or other metadata of the range. May set 'eos_'.
  virtual Status ProcessRange(RowBatch* row_batch) WARN_UNUSED_RESULT = 0;

  BaseSequenceScanner(HdfsScanNodeBase*, RuntimeState*);

  /// Read sync marker from 'stream_' and validate against 'header_->sync'. Returns
  /// non-ok if the sync marker did not match. Scanners should always use this function
  /// to read sync markers, otherwise eos() might not be updated correctly. If eos()
  /// returns true after calling this function, scanners must not process any more
  /// records.
  Status ReadSync() WARN_UNUSED_RESULT;

  /// Utility function to advance 'stream_' past the next sync marker. If no sync is
  /// found in the scan range, returns OK and sets 'eos_' to true. It is safe to call
  /// this function past eosr.
  /// - sync: sync marker to search for (does not include 0xFFFFFFFF prefix)
  /// - sync_size: number of bytes for sync
  Status SkipToSync(const uint8_t* sync, int sync_size) WARN_UNUSED_RESULT;

  /// Estimate of header size in bytes.  This is initial number of bytes to issue
  /// per file.  If the estimate is too low, more bytes will be read as necessary.
  const static int HEADER_SIZE;

  /// Sync indicator.
  const static int SYNC_MARKER;

  /// File header for this scan range.  This is not owned by the parent scan node.
  FileHeader* header_ = nullptr;

  /// If true, this scanner object is only for processing the header.
  bool only_parsing_header_ = false;

  /// Unit test constructor
  BaseSequenceScanner();

 private:
  /// Byte offset from the start of the file for the current block. Note that block refers
  /// to all the data between two syncs.
  int64_t block_start_ = 0;

  /// The total number of bytes in all blocks this scan range has processed (updated in
  /// SkipToSync(), so only includes blocks that were completely processed).
  int total_block_size_ = 0;

  /// The number of syncs seen by this scanner so far.
  int num_syncs_ = 0;

  /// Callback for stream_ to compute how much to read past the scan range. Returns the
  /// average number of bytes per block minus how far 'file_offset' is into the current
  /// block.
  int ReadPastSize(int64_t file_offset);

  /// Utility function to look for 'sync' in buffer.  Returns the offset into
  /// buffer of the _end_ of sync if it is found, otherwise, returns -1.
  int FindSyncBlock(const uint8_t* buffer, int buffer_len, const uint8_t* sync,
                    int sync_len);

  /// Close skipped ranges for 'file'.  This is only called when processing
  /// the header range and the header had an issue.
  void CloseFileRanges(const char* file);

  /// Number of bytes skipped when advancing to next sync on error.
  RuntimeProfile::Counter* bytes_skipped_counter_ = nullptr;
};

}

#endif
