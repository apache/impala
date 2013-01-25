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


#ifndef IMPALA_EXEC_BASE_SEQUENCE_SCANNER_H
#define IMPALA_EXEC_BASE_SEQUENCE_SCANNER_H

#include <vector>
#include <memory>
#include <stdint.h>

#include "exec/hdfs-scanner.h"

namespace impala {

class Codec;
struct HdfsFileDesc;
class ScanRangeContext;

// Superclass for all sequence container based file formats: 
// e.g. SequenceFile, RCFile, Avro
// Sequence container formats have sync markers periodically in the file.
// This class is will skip to the start of sync markers for errors and
// hdfs splits.
class BaseSequenceScanner : public HdfsScanner {
 public:
  // Issue the initial ranges for all sequence container files.
  static void IssueInitialRanges(HdfsScanNode*, const std::vector<HdfsFileDesc*>&);

  virtual Status Prepare();
  virtual Status Close();
  virtual Status ProcessScanRange(ScanRangeContext* context);

  virtual ~BaseSequenceScanner();

 protected:
  // Size of the sync hash field.
  const static int SYNC_HASH_SIZE = 16;

  // Data that is shared between scan ranges of the same file.  The subclass is 
  // responsible for filling in all these fields in ReadFileHeader
  struct FileHeader {
    // Type of file: e.g. rcfile, seqfile
    THdfsFileFormat::type file_type;
  
    // The sync hash for this file.
    uint8_t sync[SYNC_HASH_SIZE];

    // true if the file is compressed
    bool is_compressed;

    // Codec name if it is compressed
    std::string codec;

    // Enum for compression type.
    THdfsCompression::type compression_type;

    // Byte size of header
    int64_t header_size;
  };
  
  // Subclasses must implement these functions.  The order for calls will be
  //  1. AllocateFileHeader() - called once per file
  //  2. InitNewRange()
  //  3. ReadFileHeader()
  //  4. ProcessRange()
  // In the normal case, 2-4 is called for each range once.  In the case of
  // errors and skipped bytes, 4 is repeatedly called, each time starting
  // right after the sync marker.

  // Allocate a file header object for this scanner.  If the scanner needs 
  // additional header information, it should subclass FileHeader.
  // The allocated object will be placed in the scan node's pool.
  virtual FileHeader* AllocateFileHeader() = 0;

  // Reset internal state for a new scan range.
  virtual Status InitNewRange() = 0;

  // Read the file header.  The underlying ScanRangeContext is at the start of
  // the file header.  This function must read the file header (which advances
  // context_ past it) and initialize header_.
  virtual Status ReadFileHeader() = 0;
  
  // Process the current range until the end or an error occurred.  Note this might
  // be called multiple times if we skip over bad data.
  // This function should read from the underlying ScanRangeContext materializing
  // tuples to the context.  When this function is called, it is guaranteed to be
  // at the start of a data block (i.e. right after the sync marker).
  virtual Status ProcessRange() = 0;
  
  BaseSequenceScanner(HdfsScanNode*, RuntimeState*);
  
  // Read and validate sync marker against header_->sync.  Returns non-ok if the
  // sync marker did not match.
  Status ReadSync();
  
  // Utility function to advance to the next sync marker, reading bytes from context_.
  // - sync: sync marker (does not include 0xFFFFFFFF prefix)
  // - sync_size: number of bytes for sync
  // - sync_found: returns if the sync marker was found before the end of the scan range
  Status SkipToSync(
      const uint8_t* sync, int sync_size, bool* sync_found);

  // Estimate of header size in bytes.  This is initial number of bytes to issue
  // per file.  If the estimate is too low, more bytes will be read as necessary.
  const static int HEADER_SIZE;
  
  // Sync indicator.
  const static int SYNC_MARKER;

  // File header for this scan range.  This is not owned by the parent scan node.
  FileHeader* header_;
  
  // If true, this scanner object is only for processing the header.
  bool only_parsing_header_;
  
  // If we skip ahead on error and read the sync block this is set to true
  // so we do not need to look for it in ProcessRange
  bool have_sync_;
  
  // Byte offset from start of file for current block.  Used for error reporting.
  int block_start_;

  // Decompressor class to use, if any.
  boost::scoped_ptr<Codec> decompressor_;

  // Pool to allocate per data block memory.  This should be used with the 
  // decompressor and any other per data block allocations.
  boost::scoped_ptr<MemPool> data_buffer_pool_;

  // Time spent decompressing bytes
  RuntimeProfile::Counter* decompress_timer_;
};

}

#endif
