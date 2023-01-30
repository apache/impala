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

#include <algorithm>

#include "gutil/strings/substitute.h"
#include "runtime/io/data-cache.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/hdfs-file-reader.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

DEFINE_bool(use_hdfs_pread, false, "Enables using hdfsPread() instead of hdfsRead() "
    "when performing HDFS read operations. This is necessary to use HDFS hedged reads "
    "(assuming the HDFS client is configured to do so). Preads are always enabled for "
    "S3A and ABFS reads.");

DEFINE_int64(fs_slow_read_log_threshold_ms, 10L * 1000L,
    "Log diagnostics about I/Os issued via the HDFS client that take longer than this "
    "threshold.");

DEFINE_bool(fs_trace_remote_reads, false,
    "(Advanced) Log block locations for remote reads.");

#ifndef NDEBUG
DECLARE_int32(stress_disk_read_delay_ms);
#endif

namespace impala {
namespace io {

HdfsFileReader::~HdfsFileReader() {
  DCHECK(exclusive_hdfs_fh_ == nullptr) << "File was not closed.";
  DCHECK(cached_buffer_ == nullptr) << "Cached buffer was not released.";
}

Status HdfsFileReader::Open() {
  unique_lock<SpinLock> hdfs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);

  if (exclusive_hdfs_fh_ != nullptr) return Status::OK();
  return DoOpen();
}

Status HdfsFileReader::DoOpen() {
  auto io_mgr = scan_range_->io_mgr_;
  // Get a new exclusive file handle.
  RETURN_IF_ERROR(io_mgr->GetExclusiveHdfsFileHandle(hdfs_fs_,
      scan_range_->file_string(), scan_range_->mtime(), scan_range_->reader_,
      exclusive_hdfs_fh_));
  if (hdfsSeek(hdfs_fs_, exclusive_hdfs_fh_->file(), scan_range_->offset_) != 0) {
    // Destroy the file handle
    io_mgr->ReleaseExclusiveHdfsFileHandle(std::move(exclusive_hdfs_fh_));
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Error seeking to $0 in file: $1 $2", scan_range_->offset(),
            *scan_range_->file_string(), GetHdfsErrorMsg("")));
  }
  ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(1L);
  return Status::OK();
}

std::string HdfsFileReader::GetHostList(int64_t file_offset,
    int64_t bytes_to_read) const {
  char*** hosts = hdfsGetHosts(hdfs_fs_, scan_range_->file_string()->c_str(),
      file_offset, bytes_to_read);
  if (hosts) {
    std::ostringstream ostr;
    int blockIndex = 0;
    while (hosts[blockIndex]) {
      ostr << " [" << blockIndex << "] { ";
      int hostIndex = 0;
      while (hosts[blockIndex][hostIndex]) {
        if(hostIndex > 0) ostr << ", ";
        ostr << hosts[blockIndex][hostIndex];
        hostIndex++;
      }
      ostr << " }";
      blockIndex++;
    }
    hdfsFreeHosts(hosts);
    return ostr.str();
  }
  return "";
}

Status HdfsFileReader::ReadFromPos(DiskQueue* queue, int64_t file_offset, uint8_t* buffer,
    int64_t bytes_to_read, int64_t* bytes_read, bool* eof) {
  DCHECK(scan_range_->read_in_flight());
  DCHECK_GE(bytes_to_read, 0);
  // Delay before acquiring the lock, to allow triggering IMPALA-6587 race.
#ifndef NDEBUG
  if (FLAGS_stress_disk_read_delay_ms > 0) {
    SleepForMs(FLAGS_stress_disk_read_delay_ms);
  }
#endif
  unique_lock<SpinLock> hdfs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);

  auto io_mgr = scan_range_->io_mgr_;
  auto request_context = scan_range_->reader_;
  *eof = false;
  *bytes_read = 0;

  Status status = Status::OK();
  {
    ScopedTimer<MonotonicStopWatch> req_context_read_timer(
        scan_range_->reader_->read_timer_);
    bool logged_slow_read = false; // True if we already logged about a slow read.

    // Try reading from the remote data cache if it's enabled for the scan range.
    DataCache* remote_data_cache = io_mgr->remote_data_cache();
    bool try_data_cache = scan_range_->UseDataCache() && remote_data_cache != nullptr;
    int64_t cached_read = 0;
    if (try_data_cache) {
      cached_read = ReadDataCache(remote_data_cache, file_offset, buffer, bytes_to_read);
      DCHECK_GE(cached_read, 0);
      *bytes_read = cached_read;
    }

    if (*bytes_read == bytes_to_read) {
      // All bytes successfully read from data cache. We can safely return.
      return status;
    }

    // If we get here, the next bytes are not available in data cache, so we need to get
    // file handle in order to read the rest of data from file.
    // If the reader has an exclusive file handle, use it. Otherwise, borrow
    // a file handle from the cache.
    req_context_read_timer.Stop();

    // RAII accessor, it will release the file handle at destruction
    FileHandleCache::Accessor accessor;

    hdfsFile hdfs_file;
    if (exclusive_hdfs_fh_ != nullptr) {
      hdfs_file = exclusive_hdfs_fh_->file();
    } else if (scan_range_->FileHandleCacheEnabled()) {
      RETURN_IF_ERROR(io_mgr->GetCachedHdfsFileHandle(hdfs_fs_,
          scan_range_->file_string(), scan_range_->mtime(), request_context, &accessor));
      hdfs_file = accessor.Get()->file();
    } else {
      RETURN_IF_ERROR(DoOpen());
      hdfs_file = exclusive_hdfs_fh_->file();
    }
    req_context_read_timer.Start();

    while (*bytes_read < bytes_to_read) {
      int bytes_remaining = bytes_to_read - *bytes_read;
      DCHECK_GT(bytes_remaining, 0);
      // The hdfsRead() length argument is an int.
      DCHECK_LE(bytes_remaining, numeric_limits<int>::max());
      int current_bytes_read = -1;
      // bytes_read_ is only updated after the while loop
      int64_t position_in_file = file_offset + *bytes_read;

      // ReadFromPosInternal() might fail due to a bad file handle.
      // If that was the case, allow for a retry to fix it.
      status = ReadFromPosInternal(hdfs_file, queue, position_in_file,
          buffer + *bytes_read, bytes_remaining, &current_bytes_read);

      // Retry if:
      // - first read was not successful
      // and
      // - used a borrowed file handle
      if (!status.ok() && accessor.Get()) {
        // The error may be due to a bad file handle. Reopen the file handle and retry.
        // Exclude this time from the read timers.
        req_context_read_timer.Stop();
        RETURN_IF_ERROR(
            io_mgr->ReopenCachedHdfsFileHandle(hdfs_fs_, scan_range_->file_string(),
                scan_range_->mtime(), request_context, &accessor));
        hdfs_file = accessor.Get()->file();
        VLOG_FILE << "Reopening file " << scan_range_->file_string() << " with mtime "
                  << scan_range_->mtime() << " offset " << file_offset;
        req_context_read_timer.Start();
        status = ReadFromPosInternal(hdfs_file, queue, position_in_file,
            buffer + *bytes_read, bytes_remaining, &current_bytes_read);
      }
      // Log diagnostics for failed and successful reads.
      int64_t elapsed_time = req_context_read_timer.ElapsedTime();
      bool is_slow_read = elapsed_time
          > FLAGS_fs_slow_read_log_threshold_ms * NANOS_PER_MICRO * MICROS_PER_MILLI;
      bool log_slow_read = is_slow_read && (!logged_slow_read || !status.ok()
                 || *bytes_read + current_bytes_read == bytes_to_read);
      // Log at most two slow read errors in each invocation of this function. Always log
      // the first read where we exceeded the threshold and the last read before exiting
      // this loop.
      if (log_slow_read) {
        LOG(INFO) << "Slow FS I/O operation on " << *scan_range_->file_string() << " for "
                  << "instance " << PrintId(scan_range_->reader_->instance_id())
                  << " of query " << PrintId(scan_range_->reader_->query_id()) << ". "
                  << "Last read returned "
                  << PrettyPrinter::PrintBytes(current_bytes_read) << ". "
                  << "This thread has read "
                  << PrettyPrinter::PrintBytes(*bytes_read + current_bytes_read)
                  << "/" << PrettyPrinter::PrintBytes(bytes_to_read)
                  << " starting at offset " << file_offset << " in this I/O scheduling "
                  << "quantum and taken "
                  << PrettyPrinter::Print(elapsed_time, TUnit::TIME_NS) << " so far. "
                  << "I/O status: " << (status.ok() ? "OK" : status.GetDetail());
        logged_slow_read = true;
      }

      if (!status.ok()) {
        break;
      }
      DCHECK_GT(current_bytes_read, -1);
      if (current_bytes_read == 0) {
        // No more bytes in the file. The scan range went past the end.
        *eof = true;
        break;
      }
      *bytes_read += current_bytes_read;

      bool is_first_read = (num_remote_bytes_ == 0);
      // Collect and accumulate statistics
      GetHdfsStatistics(hdfs_file, log_slow_read);
      if (scan_range_->is_encrypted()) {
        scan_range_->reader_->bytes_read_encrypted_.Add(current_bytes_read);
      }
      if (scan_range_->is_erasure_coded()) {
        scan_range_->reader_->bytes_read_ec_.Add(current_bytes_read);
      }

      if (FLAGS_fs_trace_remote_reads && expected_local_ &&
          num_remote_bytes_ > 0 && is_first_read) {
        // Only log the first unexpected remote read for scan range
        LOG(INFO)
            << "First remote read of scan range on file "
            << *scan_range_->file_string()
            << " " << num_remote_bytes_
            << " bytes. offsets " << file_offset
            << "-" << file_offset+bytes_to_read-1
            << GetHostList(file_offset, bytes_to_read);
      }
    }

    int64_t cached_bytes_missed = *bytes_read - cached_read;
    if (try_data_cache && status.ok() && cached_bytes_missed > 0) {
      DCHECK_LE(*bytes_read, bytes_to_read);
      WriteDataCache(remote_data_cache, file_offset, buffer, *bytes_read,
          cached_bytes_missed);
    }
  }

  return status;
}

Status HdfsFileReader::ReadFromPosInternal(hdfsFile hdfs_file, DiskQueue* queue,
    int64_t position_in_file, uint8_t* buffer, int64_t bytes_to_read, int* bytes_read) {
  ScopedHistogramTimer read_timer(queue->read_latency());
  // For file handles from the cache, any of the below file operations may fail
  // due to a bad file handle.
  if (FLAGS_use_hdfs_pread || IsS3APath(scan_range_->file_string()->c_str())
      || IsABFSPath(scan_range_->file_string()->c_str())) {
    if (hdfsPreadFully(
          hdfs_fs_, hdfs_file, position_in_file, buffer, bytes_to_read) == -1) {
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          GetHdfsErrorMsg("Error reading from HDFS file: ",
              *scan_range_->file_string()));
    }
    *bytes_read = bytes_to_read;
  } else {
    const int64_t cur_offset = hdfsTell(hdfs_fs_, hdfs_file);
    if (cur_offset == -1) {
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute("Error getting current offset of file $0: $1",
              *scan_range_->file_string(), GetHdfsErrorMsg("")));
    }
    // If the file handle is borrowed or if we had a cache hit earlier, it may not be
    // at the appropriate location. Seek to the appropriate location.
    if (cur_offset != position_in_file) {
      if (hdfsSeek(hdfs_fs_, hdfs_file, position_in_file) != 0) {
        return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
            Substitute("Error seeking to $0 in file: $1: $2",
                position_in_file, *scan_range_->file_string(), GetHdfsErrorMsg("")));
      }
    }
    *bytes_read = hdfsRead(hdfs_fs_, hdfs_file, buffer, bytes_to_read);
    if (*bytes_read == -1) {
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          GetHdfsErrorMsg("Error reading from HDFS file: ",
              *scan_range_->file_string()));
    }
  }
  queue->read_size()->Update(*bytes_read);
  return Status::OK();
}

void HdfsFileReader::CachedFile(uint8_t** data, int64_t* length) {
  {
    unique_lock<SpinLock> hdfs_lock(lock_);
    DCHECK(cached_buffer_ == nullptr);
    DCHECK(exclusive_hdfs_fh_ != nullptr);
    cached_buffer_ = hadoopReadZero(exclusive_hdfs_fh_->file(),
        scan_range_->io_mgr_->cached_read_options(), scan_range_->len());
  }
  if (cached_buffer_ == nullptr) {
    *data = nullptr;
    *length = 0;
    return;
  }
  *data = reinterpret_cast<uint8_t*>(
      const_cast<void*>(hadoopRzBufferGet(cached_buffer_)));
  *length = hadoopRzBufferLength(cached_buffer_);
}

int64_t HdfsFileReader::ReadDataCache(DataCache* remote_data_cache, int64_t file_offset,
    uint8_t* buffer, int64_t bytes_to_read) {
  int64_t cached_read = remote_data_cache->Lookup(*scan_range_->file_string(),
      scan_range_->mtime(), file_offset, bytes_to_read, buffer);
  if (LIKELY(cached_read > 0)) {
    scan_range_->reader_->data_cache_hit_bytes_counter_->Add(cached_read);
    if (LIKELY(cached_read == bytes_to_read)) {
      scan_range_->reader_->data_cache_hit_counter_->Add(1);
    } else {
      scan_range_->reader_->data_cache_partial_hit_counter_->Add(1);
    }
    ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES->Increment(cached_read);
    ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_COUNT->Increment(1);
  }
  return cached_read;
}

void HdfsFileReader::WriteDataCache(DataCache* remote_data_cache, int64_t file_offset,
    const uint8_t* buffer, int64_t buffer_len, int64_t bytes_missed) {
  // Intentionally leave out the return value as cache insertion is opportunistic.
  // It can fail for various reasons:
  // - multiple threads inserting the same entry at the same time
  // - the entry is too large to be accomodated in the cache
  remote_data_cache->Store(*scan_range_->file_string(), scan_range_->mtime(),
      file_offset, buffer, buffer_len);
  scan_range_->reader_->data_cache_miss_bytes_counter_->Add(bytes_missed);
  scan_range_->reader_->data_cache_miss_counter_->Add(1);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES->Increment(bytes_missed);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_COUNT->Increment(1);
}

void HdfsFileReader::Close() {
  unique_lock<SpinLock> hdfs_lock(lock_);
  if (exclusive_hdfs_fh_ != nullptr) {
    GetHdfsStatistics(exclusive_hdfs_fh_->file(), false);

    if (cached_buffer_ != nullptr) {
      hadoopRzBufferFree(exclusive_hdfs_fh_->file(), cached_buffer_);
      cached_buffer_ = nullptr;
    }

    // Destroy the file handle.
    scan_range_->io_mgr_->ReleaseExclusiveHdfsFileHandle(std::move(exclusive_hdfs_fh_));
    ImpaladMetrics::IO_MGR_NUM_OPEN_FILES->Increment(-1L);
  }

  if (FLAGS_use_hdfs_pread && IsHdfsPath(scan_range_->file())) {
    // Update Hedged Read Metrics.
    // We call it only if the --use_hdfs_pread flag is set, to avoid having the
    // libhdfs client malloc and free a hdfsHedgedReadMetrics object unnecessarily
    // otherwise. 'hedged_metrics' is only set upon success.
    // We also avoid calling hdfsGetHedgedReadMetrics() when the file is not on HDFS
    // (see HDFS-13417).
    struct hdfsHedgedReadMetrics* hedged_metrics;
    int success = hdfsGetHedgedReadMetrics(hdfs_fs_, &hedged_metrics);
    if (success == 0) {
      ImpaladMetrics::HEDGED_READ_OPS->SetValue(hedged_metrics->hedgedReadOps);
      ImpaladMetrics::HEDGED_READ_OPS_WIN->SetValue(
          hedged_metrics->hedgedReadOpsWin);
      hdfsFreeHedgedReadMetrics(hedged_metrics);
    }
  }

  if (num_remote_bytes_ > 0) {
    scan_range_->reader_->num_remote_ranges_.Add(1L);
    if (expected_local_) {
      scan_range_->reader_->unexpected_remote_bytes_.Add(num_remote_bytes_);
      VLOG_FILE << "Unexpected remote HDFS read of "
                   << PrettyPrinter::Print(num_remote_bytes_, TUnit::BYTES)
                   << " for file '" << *scan_range_->file_string() << "'";
    }
  }
}

void HdfsFileReader::GetHdfsStatistics(hdfsFile hdfs_file, bool log_stats) {
  struct hdfsReadStatistics* stats;
  if (IsHdfsPath(scan_range_->file())) {
    int success = hdfsFileGetReadStatistics(hdfs_file, &stats);
    if (success == 0) {
      scan_range_->reader_->bytes_read_local_.Add(stats->totalLocalBytesRead);
      scan_range_->reader_->bytes_read_short_circuit_.Add(
          stats->totalShortCircuitBytesRead);
      scan_range_->reader_->bytes_read_dn_cache_.Add(stats->totalZeroCopyBytesRead);
      if (stats->totalLocalBytesRead != stats->totalBytesRead) {
        num_remote_bytes_ += stats->totalBytesRead - stats->totalLocalBytesRead;
      }
      if (log_stats) {
        LOG(INFO) << "Stats for last read by this I/O thread:"
                  << " totalBytesRead=" << stats->totalBytesRead
                  << " totalLocalBytesRead=" << stats->totalLocalBytesRead
                  << " totalShortCircuitBytesRead=" << stats->totalShortCircuitBytesRead
                  << " totalZeroCopyBytesRead=" << stats->totalZeroCopyBytesRead;
      }
      hdfsFileFreeReadStatistics(stats);
    }
    hdfsFileClearReadStatistics(hdfs_file);
  }
}

void HdfsFileReader::ResetState() {
  FileReader::ResetState();
  num_remote_bytes_ = 0;
}

string HdfsFileReader::DebugString() const {
  return FileReader::DebugString() + Substitute(
      " exclusive_hdfs_fh=$0 num_remote_bytes=$1",
      exclusive_hdfs_fh_.get(), num_remote_bytes_);
}

}
}

