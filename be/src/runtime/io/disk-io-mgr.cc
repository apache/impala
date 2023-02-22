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

#include "runtime/io/disk-io-mgr.h"

#include "common/global-flags.h"
#include "common/names.h"
#include "common/thread-debug-info.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/data-cache.h"
#include "runtime/io/disk-file.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/error-converter.h"
#include "runtime/io/file-writer.h"
#include "runtime/io/handle-cache.inline.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "gutil/strings/substitute.h"
#include "util/bit-util.h"
#include "util/collection-metrics.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/metrics.h"
#include "util/os-util.h"
#include "util/test-info.h"
#include "util/time.h"

#ifndef NDEBUG
DECLARE_int32(stress_scratch_write_delay_ms);
#endif

#include "common/names.h"

using namespace impala;
using namespace impala::io;
using namespace strings;

using boost::shared_mutex;
using boost::filesystem::path;
using boost::uuids::random_generator;

// Control the number of disks on the machine.  If 0, this comes from the system
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IoMgr configs:
// The maximum number of the threads per disk is also the max queue depth per disk.
DEFINE_int32(num_threads_per_disk, 0, "Number of I/O threads per disk");
// Data cache configuration
DEFINE_string(data_cache, "", "The configuration string for IO data cache. "
    "Default to be an empty string so it's disabled. The configuration string is "
    "expected to be a list of directories, separated by ',', followed by a ':' and "
    "a capacity quota per directory. For example /data/0,/data/1:1TB means the cache "
    "may use up to 2TB, with 1TB max in /data/0 and /data/1 respectively. Please note "
    "that each Impala daemon on a host must have a unique caching directory.");
DEFINE_int32(data_cache_num_async_write_threads, 0,
    "(Experimental) Number of data cache async write threads. Write threads will write "
    "the cache asynchronously after IO thread read data, so IO thread will return more "
    "quickly. The extra memory for temporary buffers is limited by "
    "--data_cache_async_write_buffer_limit. If this's 0, then write will be "
    "synchronous.");

DEFINE_bool(data_cache_keep_across_restarts, false,
    "(Experimental) If this is true, the data cache metadata is dumped to the same "
    "directory as the cached files on disk when the process gracefully shutdowns. The "
    "metadata will be reloaded the next time the process starts, so that the previous "
    "cached data can be reused as if the process had never shutdown. If loading fails, "
    "it will proceed with regular initialization.");

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_SOLID_STATE_DISK = 8;

const int64_t DiskIoMgr::IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE;

// The maximum number of the threads per rotational disk is also the max queue depth per
// rotational disk.
static const string num_io_threads_per_rotational_disk_help_msg = Substitute("Number of "
    "I/O threads per rotational disk. Has priority over num_threads_per_disk. If neither"
    " is set, defaults to $0 thread(s) per rotational disk", THREADS_PER_ROTATIONAL_DISK);
DEFINE_int32(num_io_threads_per_rotational_disk, 0,
    num_io_threads_per_rotational_disk_help_msg.c_str());

// The maximum number of the threads per solid state disk is also the max queue depth per
// solid state disk.
static const string num_io_threads_per_solid_state_disk_help_msg = Substitute("Number of"
    " I/O threads per solid state disk. Has priority over num_threads_per_disk. If "
    "neither is set, defaults to $0 thread(s) per solid state disk",
    THREADS_PER_SOLID_STATE_DISK);
DEFINE_int32(num_io_threads_per_solid_state_disk, 0,
    num_io_threads_per_solid_state_disk_help_msg.c_str());

// The maximum number of remote HDFS I/O threads.  HDFS access that are expected to be
// remote are placed on a separate remote disk queue.  This is the queue depth for that
// queue.  If 0, then the remote queue is not used and instead ranges are round-robined
// across the local disk queues.
DEFINE_int32(num_remote_hdfs_io_threads, 8, "Number of remote HDFS I/O threads");

// The maximum number of HDFS I/O threads for doing file operations, such as file
// uploading. The default value of 8 was chosen empirically to maximize HDFS throughput.
DEFINE_int32(num_remote_hdfs_file_oper_io_threads, 8,
    "Number of remote HDFS file operations I/O threads");

// The maximum number of S3 I/O threads. The default value of 16 was chosen emperically
// to maximize S3 throughput. Maximum throughput is achieved with multiple connections
// open to S3 and use of multiple CPU cores since S3 reads are relatively compute
// expensive (SSL and JNI buffer overheads).
DEFINE_int32(num_s3_io_threads, 16, "Number of S3 I/O threads");

// The maximum number of S3 I/O threads for doing file operations, such as file
// uploading. The default value of 16 was chosen empirically to maximize S3 throughput.
DEFINE_int32(num_s3_file_oper_io_threads, 16, "Number of S3 file operations I/O threads");

// The maximum number of ABFS I/O threads. TODO: choose the default empirically.
DEFINE_int32(num_abfs_io_threads, 16, "Number of ABFS I/O threads");

// The maximum number of OSS/JindoFS I/O threads. TODO: choose the default empirically.
DEFINE_int32(num_oss_io_threads, 16, "Number of OSS/JindoFS I/O threads");

// The maximum number of ADLS I/O threads. This number is a good default to have for
// clusters that may vary widely in size, due to an undocumented concurrency limit
// enforced by ADLS for a cluster, which spans between 500-700. For smaller clusters
// (~10 nodes), 64 threads would be more ideal.
DEFINE_int32(num_adls_io_threads, 16, "Number of ADLS I/O threads");

// The maximum number of GCS I/O threads. TODO: choose the default empirically.
DEFINE_int32(num_gcs_io_threads, 16, "Number of GCS I/O threads");

// The maximum number of GCS I/O threads. TODO: choose the default empirically.
DEFINE_int32(num_cos_io_threads, 16, "Number of COS I/O threads");

// The maximum number of Ozone I/O threads. TODO: choose the default empirically.
DEFINE_int32(num_ozone_io_threads, 16, "Number of Ozone I/O threads");

// The maximum number of SFS I/O threads.
DEFINE_int32(num_sfs_io_threads, 16, "Number of SFS I/O threads");

// The maximum number of OBS I/O threads.
DEFINE_int32(num_obs_io_threads, 16, "Number of OBS I/O threads");

// The number of cached file handles defines how much memory can be used per backend for
// caching frequently used file handles. Measurements indicate that a single file handle
// uses about 6kB of memory. 20k file handles will thus reserve ~120MB of memory.
// The actual amount of memory that is associated with a file handle can be larger
// or smaller, depending on the replication factor for this file or the path name.
DEFINE_uint64(max_cached_file_handles, 20000, "Maximum number of HDFS file handles "
    "that will be cached. Disabled if set to 0.");

// The unused file handle timeout specifies how long a file handle will remain in the
// cache if it is not being used. Aging out unused handles ensures that the cache is not
// wasting memory on handles that aren't useful. This allows users to specify a larger
// cache size, as the system will only use the memory on useful file handles.
// Additionally, cached file handles keep an open file descriptor for local files.
// If a file is deleted through HDFS, this open file descriptor can keep the disk space
// from being freed. When the metadata sees that a file has been deleted, the file handle
// will no longer be used by future queries. Aging out this file handle allows the
// disk space to be freed in an appropriate period of time. The default value is
// 6 hours. This was chosen to be less than a typical value for HDFS's fs.trash.interval.
// This means that when files are deleted via the trash, the file handle cache will
// have evicted the file handle before the files are flushed from the trash. This
// means that the file handle cache won't impact available disk space.
DEFINE_uint64(unused_file_handle_timeout_sec, 21600, "Maximum time, in seconds, that an "
    "unused HDFS file handle will remain in the file handle cache. Disabled if set "
    "to 0.");

// The file handle cache is split into multiple independent partitions, each with its
// own lock and structures. A larger number of partitions reduces contention by
// concurrent accesses, but it also reduces the efficiency of the cache due to
// separate LRU lists.
// TODO: Test different number of partitions to determine an appropriate default
DEFINE_uint64(num_file_handle_cache_partitions, 16, "Number of partitions used by the "
    "file handle cache.");

// This parameter controls whether remote HDFS file handles are cached. It does not impact
// S3, ADLS, or ABFS file handles.
DEFINE_bool(cache_remote_file_handles, true, "Enable the file handle cache for "
    "remote HDFS files.");

// This parameter controls whether S3 file handles are cached.
DEFINE_bool(cache_s3_file_handles, true, "Enable the file handle cache for "
    "S3 files.");

// This parameter controls whether ABFS file handles are cached.
DEFINE_bool(cache_abfs_file_handles, true, "Enable the file handle cache for "
    "ABFS files.");

DEFINE_bool(cache_ozone_file_handles, true, "Enable the file handle cache for Ozone "
    "files.");

DECLARE_int64(min_buffer_size);

static const char* DEVICE_NAME_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.device-name";
static const char* READ_LATENCY_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.read-latency";
static const char* READ_SIZE_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.read-size";
static const char* WRITE_LATENCY_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.write-latency";
static const char* WRITE_SIZE_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.write-size";
static const char* WRITE_IO_ERR_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.queue-$0.write-io-error";

AtomicInt32 DiskIoMgr::next_disk_id_;

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "Disks: " << endl;
  for (DiskQueue* disk_queue : disk_queues_) {
    disk_queue->DebugString(&ss);
    ss << endl;
  }
  return ss.str();
}

WriteRange::WriteRange(
    const string& file, int64_t file_offset, int disk_id, WriteDoneCallback callback)
  : RequestRange(RequestType::WRITE), callback_(callback) {
  SetRange(file, file_offset, disk_id);
}

void WriteRange::SetRange(
    const std::string& file, int64_t file_offset, int disk_id) {
  file_ = file;
  offset_ = file_offset;
  disk_id_ = disk_id;
}

void WriteRange::SetData(const uint8_t* buffer, int64_t len) {
  data_ = buffer;
  len_ = len;
}

void WriteRange::SetOffset(int64_t file_offset) {
  offset_ = file_offset;
}

Status WriteRange::DoWrite() {
  Status ret_status = Status::OK();
  Status close_status = Status::OK();
  DiskQueue* queue = io_ctx_->parent_->disk_queues_[disk_id_];
  int64_t written_bytes = 0;
  FileWriter* file_writer = disk_file_->GetFileWriter();
  /// DoWrite is used for Spilling.
  /// For spilling to local, we will open and close the file handle for
  /// each write range, which is the WriteOne() function, since it could
  /// be a random write in each file.
  /// For spilling to remote, we will keep the file handle open, until the
  /// write range is the last one of the file to do the sequential write.
  if (disk_file_->disk_type() == DiskFileType::LOCAL) {
    return file_writer->WriteOne(this);
  }
  {
    ScopedHistogramTimer write_timer(queue->write_latency());
    shared_lock<shared_mutex> lock(disk_file_->physical_file_lock_);
    ret_status = file_writer->Open();
    if (!ret_status.ok()) return DoWriteEnd(queue, ret_status);
    ret_status = file_writer->Write(this, &written_bytes);
    disk_file_->UpdateReadBufferMetaDataIfNeeded(written_bytes - len_);
    int64_t actual_file_size = disk_file_->actual_file_size();
    // actual_file_size is only set once, otherwise it is 0 by default. If it is still
    // not set, it is impossible to be full.
    if (actual_file_size != 0) {
      DCHECK_LE(written_bytes, disk_file_->actual_file_size());
      is_full_ = written_bytes == actual_file_size;
    } else {
      // If the actual size hasn't been set, the written bytes must be less than the
      // default file size.
      DCHECK_LT(written_bytes, disk_file_->file_size());
    }
    if (is_full_) close_status = file_writer->Close();
    if (ret_status.ok() && !close_status.ok()) ret_status = close_status;
    if (ret_status.ok() && is_full_) {
      // If the file is full, the file handle should be closed,
      // so set the file to persisted status.
      disk_file_->SetStatus(io::DiskFileStatus::PERSISTED);
    }
  }
  return DoWriteEnd(queue, ret_status);
}

Status WriteRange::DoWriteEnd(DiskQueue* queue, const Status& ret_status) {
  if (ret_status.ok()) {
    queue->write_size()->Update(len());
  } else {
    queue->write_io_err()->Increment(1);
  }
  return ret_status;
}

RemoteOperRange::RemoteOperRange(DiskFile* src_file, DiskFile* dst_file,
    int64_t block_size, int disk_id, RequestType::type type, DiskIoMgr* io_mgr,
    RemoteOperDoneCallback callback, int64_t file_offset)
  : RequestRange(type, disk_id, file_offset),
    callback_(callback),
    io_mgr_(io_mgr),
    disk_file_src_(src_file),
    disk_file_dst_(dst_file),
    block_size_(block_size) {}

Status RemoteOperRange::DoUpload(uint8_t* buffer, int64_t buffer_size) {
  DCHECK(disk_file_src_ != nullptr);
  DCHECK(disk_file_dst_ != nullptr);
  hdfsFS hdfs_conn = disk_file_dst_->hdfs_conn_;
  int64_t file_size = disk_file_src_->actual_file_size();
  DCHECK(hdfs_conn != nullptr);
  DCHECK(file_size != 0);
  const string& remote_file_path = disk_file_dst_->path();
  const string& local_file_path = disk_file_src_->path();
  DiskQueue* queue = io_mgr_->disk_queues_[disk_id_];
  Status status = Status::OK();
  int64_t ret, offset = 0;
  FILE* local_file = nullptr;

  // To get the shared lock to protect the physical files from deleting during the
  // upload. The sequence is to acquire the local file lock, then remote file lock,
  // or it might cause deadlocks.
  shared_lock<shared_mutex> srcl(disk_file_src_->physical_file_lock_);
  shared_lock<shared_mutex> dstl(disk_file_dst_->physical_file_lock_);

  // If it is not persisted, the cases could be the query is cancelled or finished.
  auto src_status = disk_file_src_->GetFileStatus();
  if (src_status != io::DiskFileStatus::PERSISTED) {
    DCHECK(src_status == io::DiskFileStatus::DELETED);
    return Status::OK();
  }

  RETURN_IF_ERROR(io_mgr_->local_file_system_->OpenForRead(
      local_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR, &local_file));
  hdfsFile remote_hdfs_file =
      hdfsOpenFile(hdfs_conn, remote_file_path.c_str(), O_WRONLY, 0, 0, buffer_size);

  if (remote_hdfs_file == nullptr) {
    status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Could not open file: $0: $1", remote_file_path, GetStrErrMsg()));
    goto end;
  }

  /// Read the blocks from the local buffer file and write the blocks
  /// to the remote file.
  while (file_size != offset) {
    // If to_delete flag is set, we will quit the upload process, close the local file
    // but leave the deletion work to the thread which sets the to_delete flag.
    if (disk_file_src_->is_to_delete()) goto end;
    int bytes = min(file_size - offset, buffer_size);
    status = io_mgr_->local_file_system_->Fread(
        local_file, buffer, bytes, local_file_path.c_str());
    if (!status.ok()) goto end;
    {
      ScopedHistogramTimer write_timer(queue->write_latency());
      // Write local buffer to the remote file.
      ret = hdfsWrite(hdfs_conn, remote_hdfs_file, buffer, bytes);
    }
    if (ret == -1 || ret != bytes) {
      queue->write_io_err()->Increment(1);
      status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute("Error writing to $0 in file: $1 $2", offset, remote_file_path,
              GetHdfsErrorMsg("")));
      goto end;
    }
    offset += bytes;
    queue->write_size()->Update(bytes);
  }

end:
  if (remote_hdfs_file != nullptr) {
    // We count file close as write latency, as it takes time for
    // S3 to close to finish the upload procedure.
    ScopedHistogramTimer write_timer(queue->write_latency());
    if (hdfsCloseFile(hdfs_conn, remote_hdfs_file) != 0) {
      // Try to close the local file if error happens.
      RETURN_IF_ERROR(
          io_mgr_->local_file_system_->Fclose(local_file, local_file_path.c_str()));
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute(
              "Failed to close HDFS file: $0", remote_file_path, GetHdfsErrorMsg("")));
    }
  }
  RETURN_IF_ERROR(
      io_mgr_->local_file_system_->Fclose(local_file, local_file_path.c_str()));
  if (status.ok()) {
    disk_file_dst_->SetStatus(io::DiskFileStatus::PERSISTED);
    disk_file_dst_->SetActualFileSize(file_size);
  } else {
    LOG(WARNING) << "File upload failed, msg:" << status.msg().msg();
  }
  return status;
}

Status RemoteOperRange::DoFetch() {
  hdfsFS hdfs_conn = disk_file_src_->hdfs_conn_;
  DCHECK(hdfs_conn != nullptr);
  // Fetch the data from the source file (remote) to the destination file (local).
  DCHECK(disk_file_dst_ != nullptr);
  DCHECK(disk_file_src_ != nullptr);
  int64_t buffer_idx = disk_file_dst_->GetReadBufferIndex(offset_);
  int64_t local_file_size = disk_file_dst_->GetReadBuffActualSize(buffer_idx);
  const string& remote_file_path = disk_file_src_->path();
  DiskQueue* queue = io_mgr_->disk_queues_[disk_id_];
  Status status = Status::OK();

  // Get the shared lock to prevent the physical files from deletion during the fetching.
  // The sequence is to get the local file lock, then remote file lock, or it might meet
  // deadlocks.
  shared_lock<shared_mutex> dstl(disk_file_dst_->physical_file_lock_);
  shared_lock<shared_mutex> srcl(disk_file_src_->physical_file_lock_);

  // Check if the remote file is deleted.
  auto src_status = disk_file_src_->GetFileStatus();
  if (src_status != io::DiskFileStatus::PERSISTED) {
    DCHECK(src_status == io::DiskFileStatus::DELETED);
    return Status(Substitute("File has been deleted, path: '$0'", remote_file_path));
  }

  unique_lock<SpinLock> read_buffer_lock(
      *(disk_file_dst_->GetBufferBlockLock(buffer_idx)));
  MemBlock* read_buffer_bloc = disk_file_dst_->GetBufferBlock(buffer_idx);
  if (disk_file_dst_->IsReadBufferBlockStatus(
          read_buffer_bloc, MemBlockStatus::DISABLED, dstl, &read_buffer_lock)) {
    // If the read block is disabled, the status doesn't allow any writes to
    // the block, probably the query ends or is cancelled.
    return Status(Substitute(
        "Mem block '$0' has been deleted, path: '$1'", buffer_idx, remote_file_path));
  }
  RETURN_IF_ERROR(disk_file_dst_->AllocReadBufferBlockLocked(
      read_buffer_bloc, local_file_size, dstl, read_buffer_lock));
  DCHECK(read_buffer_bloc->data() != nullptr);
  hdfsFile remote_hdfs_file =
      hdfsOpenFile(hdfs_conn, remote_file_path.c_str(), O_RDONLY, 0, 0, block_size_);
  if (remote_hdfs_file == nullptr) {
    status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Could not open file: $0: $1", remote_file_path, GetStrErrMsg()));
  } else {
    int ret = [&]() {
      ScopedHistogramTimer read_timer(queue->read_latency());
      return hdfsPreadFully(hdfs_conn, remote_hdfs_file, offset_,
          read_buffer_bloc->data(), local_file_size);
    }();
    if (ret != -1) {
      queue->read_size()->Update(local_file_size);
      disk_file_dst_->SetReadBufferBlockStatus(
          read_buffer_bloc, MemBlockStatus::WRITTEN, dstl, &read_buffer_lock);
    } else {
      // The caller may need to handle the error, and deal with the read buffer block.
      status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          GetHdfsErrorMsg("Error reading from HDFS file: ", remote_file_path));
    }
  }

  // Try to close the remote file.
  if (remote_hdfs_file != nullptr && hdfsCloseFile(hdfs_conn, remote_hdfs_file) != 0) {
    // If there was an error during reading, keep the old status.
    string close_err_msg = Substitute(
        "Failed to close HDFS file: $0", remote_file_path, GetHdfsErrorMsg(""));
    if (status.ok()) {
      return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(), close_err_msg);
    } else {
      LOG(WARNING) << close_err_msg;
    }
  }
  return status;
}

static void CheckSseSupport() {
  if (!CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    LOG(WARNING) << "This machine does not support sse4_2.  The default IO system "
                    "configurations are suboptimal for this hardware.  Consider "
                    "increasing the number of threads per disk by restarting impalad "
                    "using the --num_threads_per_disk flag with a higher value";
  }
}

// Utility function to select flag that is set (has a positive value) based on precedence
static inline int GetFirstPositiveVal(const int first_val, const int second_val,
    const int default_val) {
  return first_val > 0 ? first_val : (second_val > 0 ? second_val : default_val);
}

DiskIoMgr::DiskIoMgr() :
    num_io_threads_per_rotational_disk_(GetFirstPositiveVal(
        FLAGS_num_io_threads_per_rotational_disk, FLAGS_num_threads_per_disk,
        THREADS_PER_ROTATIONAL_DISK)),
    num_io_threads_per_solid_state_disk_(GetFirstPositiveVal(
        FLAGS_num_io_threads_per_solid_state_disk, FLAGS_num_threads_per_disk,
        THREADS_PER_SOLID_STATE_DISK)),
    max_buffer_size_(BitUtil::RoundUpToPowerOfTwo(FLAGS_read_size)),
    min_buffer_size_(BitUtil::RoundDownToPowerOfTwo(FLAGS_min_buffer_size)),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec, &hdfs_monitor_) {
  DCHECK_LE(READ_SIZE_MIN_VALUE, FLAGS_read_size);
  int num_local_disks = DiskInfo::num_disks();
  if (FLAGS_num_disks < 0 || FLAGS_num_disks > DiskInfo::num_disks()) {
    LOG(WARNING) << "Number of disks specified should be between 0 and the number of "
        "logical disks on the system. Defaulting to system setting of " <<
        DiskInfo::num_disks() << " disks";
  } else if (FLAGS_num_disks > 0) {
    num_local_disks = FLAGS_num_disks;
  }
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
  local_file_system_.reset(new LocalFileSystem());
}

DiskIoMgr::DiskIoMgr(int num_local_disks, int threads_per_rotational_disk,
    int threads_per_solid_state_disk, int64_t min_buffer_size, int64_t max_buffer_size) :
    num_io_threads_per_rotational_disk_(threads_per_rotational_disk),
    num_io_threads_per_solid_state_disk_(threads_per_solid_state_disk),
    max_buffer_size_(BitUtil::RoundUpToPowerOfTwo(max_buffer_size)),
    min_buffer_size_(BitUtil::RoundDownToPowerOfTwo(min_buffer_size)),
    file_handle_cache_(min(FLAGS_max_cached_file_handles,
        FileSystemUtil::MaxNumFileHandles()),
        FLAGS_num_file_handle_cache_partitions,
        FLAGS_unused_file_handle_timeout_sec, &hdfs_monitor_) {
  if (num_local_disks == 0) num_local_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_local_disks + REMOTE_NUM_DISKS);
  CheckSseSupport();
  local_file_system_.reset(new LocalFileSystem());
}

DiskIoMgr::~DiskIoMgr() {
  // Signal all threads to shut down, then wait for them to do so.
  for (DiskQueue* disk_queue : disk_queues_) {
    if (disk_queue != nullptr) disk_queue->ShutDown();
  }
  disk_thread_group_.JoinAll();
  for (DiskQueue* disk_queue : disk_queues_) delete disk_queue;
  if (cached_read_options_ != nullptr) hadoopRzOptionsFree(cached_read_options_);
  if (remote_data_cache_) remote_data_cache_->ReleaseResources();
}

Status DiskIoMgr::Init() {
  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    int num_threads_per_disk;
    string device_name;
    if (i == RemoteDfsDiskId()) {
      num_threads_per_disk = FLAGS_num_remote_hdfs_io_threads;
      device_name = "HDFS remote";
    } else if (i == RemoteS3DiskId()) {
      num_threads_per_disk = FLAGS_num_s3_io_threads;
      device_name = "S3 remote";
    } else if (i == RemoteAbfsDiskId()) {
      num_threads_per_disk = FLAGS_num_abfs_io_threads;
      device_name = "ABFS remote";
    } else if (i == RemoteAdlsDiskId()) {
      num_threads_per_disk = FLAGS_num_adls_io_threads;
      device_name = "ADLS remote";
    } else if (i == RemoteOSSDiskId()) {
      num_threads_per_disk = FLAGS_num_oss_io_threads;
      device_name = "OSS remote";
    } else if (i == RemoteGcsDiskId()) {
      num_threads_per_disk = FLAGS_num_gcs_io_threads;
      device_name = "GCS remote";
    }  else if (i == RemoteCosDiskId()) {
      num_threads_per_disk = FLAGS_num_cos_io_threads;
      device_name = "COS remote";
    } else if (i == RemoteOzoneDiskId()) {
      num_threads_per_disk = FLAGS_num_ozone_io_threads;
      device_name = "Ozone remote";
    } else if (i == RemoteDfsDiskFileOperId()) {
      num_threads_per_disk = FLAGS_num_remote_hdfs_file_oper_io_threads;
      device_name = "HDFS remote file operations";
    } else if (i == RemoteS3DiskFileOperId()) {
      num_threads_per_disk = FLAGS_num_s3_file_oper_io_threads;
      device_name = "S3 remote file operations";
    } else if (i == RemoteSFSDiskId()) {
      num_threads_per_disk = FLAGS_num_sfs_io_threads;
      device_name = "SFS remote";
    } else if (i == RemoteOBSDiskId()) {
      num_threads_per_disk = FLAGS_num_obs_io_threads;
      device_name = "OBS remote";
    } else if (DiskInfo::is_rotational(i)) {
      num_threads_per_disk = num_io_threads_per_rotational_disk_;
      // During tests, i may not point to an existing disk.
      device_name =
          i < DiskInfo::num_disks() ? DiskInfo::device_name(i) : std::to_string(i);
    } else {
      num_threads_per_disk = num_io_threads_per_solid_state_disk_;
      // During tests, i may not point to an existing disk.
      device_name =
          i < DiskInfo::num_disks() ? DiskInfo::device_name(i) : std::to_string(i);
    }
    const string& i_string = Substitute("$0", i);

    // Unit tests may create multiple DiskIoMgrs, so we need to avoid re-registering the
    // same metrics.
    if (!TestInfo::is_test()
        || ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<StringProperty>(
               Substitute(DEVICE_NAME_METRIC_KEY_TEMPLATE, i_string))
            == nullptr) {
      ImpaladMetrics::IO_MGR_METRICS->AddProperty<string>(
          DEVICE_NAME_METRIC_KEY_TEMPLATE, device_name, i_string);
    }

    HistogramMetric* read_latency = nullptr;
    HistogramMetric* read_size = nullptr;
    HistogramMetric* write_latency = nullptr;
    HistogramMetric* write_size = nullptr;
    IntCounter* write_io_err = nullptr;

    if (TestInfo::is_test()) {
      read_latency =
        ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
            Substitute(READ_LATENCY_METRIC_KEY_TEMPLATE, i_string));
      read_size =
        ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
            Substitute(READ_SIZE_METRIC_KEY_TEMPLATE, i_string));
      write_latency =
          ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
              Substitute(WRITE_LATENCY_METRIC_KEY_TEMPLATE, i_string));
      write_size = ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          Substitute(WRITE_SIZE_METRIC_KEY_TEMPLATE, i_string));
      write_io_err = ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<IntCounter>(
          Substitute(WRITE_IO_ERR_METRIC_KEY_TEMPLATE, i_string));
    }

    int64_t ONE_HOUR_IN_NS = 60L * 60L * NANOS_PER_SEC;
    int64_t ONE_GB = 1024L * 1024L * 1024L;

    if (read_latency == nullptr) {
      read_latency = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(
          new HistogramMetric(MetricDefs::Get(READ_LATENCY_METRIC_KEY_TEMPLATE, i_string),
              ONE_HOUR_IN_NS, 3));
    }
    if (read_size == nullptr) {
      read_size = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
          MetricDefs::Get(READ_SIZE_METRIC_KEY_TEMPLATE, i_string), ONE_GB, 3));
    }
    if (write_latency == nullptr) {
      write_latency = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
          MetricDefs::Get(WRITE_LATENCY_METRIC_KEY_TEMPLATE, i_string), ONE_HOUR_IN_NS,
          3));
    }
    if (write_size == nullptr) {
      write_size = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
          MetricDefs::Get(WRITE_SIZE_METRIC_KEY_TEMPLATE, i_string), ONE_GB, 3));
    }
    if (write_io_err == nullptr) {
      write_io_err = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(
          new IntCounter(MetricDefs::Get(WRITE_IO_ERR_METRIC_KEY_TEMPLATE, i_string), 0));
    }

    disk_queues_[i]->set_read_latency(read_latency);
    disk_queues_[i]->set_read_size(read_size);
    disk_queues_[i]->set_write_latency(write_latency);
    disk_queues_[i]->set_write_size(write_size);
    disk_queues_[i]->set_write_io_err(write_io_err);

    for (int j = 0; j < num_threads_per_disk; ++j) {
      stringstream ss;
      ss << "work-loop(Disk: " << device_name << ", Thread: " << j << ")";
      std::unique_ptr<Thread> t;
      RETURN_IF_ERROR(Thread::Create("disk-io-mgr", ss.str(), &DiskQueue::DiskThreadLoop,
          disk_queues_[i], this, &t));
      disk_thread_group_.AddThread(move(t));
    }
  }
  // The file handle cache depends on the HDFS monitor, so initialize it first.
  // Use the same number of threads for the HDFS monitor as there are Disk IO threads.
  RETURN_IF_ERROR(hdfs_monitor_.Init(disk_thread_group_.Size()));
  RETURN_IF_ERROR(file_handle_cache_.Init());

  cached_read_options_ = hadoopRzOptionsAlloc();
  DCHECK(cached_read_options_ != nullptr);
  // Disable checksumming for cached reads.
  int ret = hadoopRzOptionsSetSkipChecksum(cached_read_options_, true);
  DCHECK_EQ(ret, 0);
  // Disable automatic fallback for cached reads.
  ret = hadoopRzOptionsSetByteBufferPool(cached_read_options_, nullptr);
  DCHECK_EQ(ret, 0);

  if (!FLAGS_data_cache.empty()) {
    remote_data_cache_.reset(
        new DataCache(FLAGS_data_cache, FLAGS_data_cache_num_async_write_threads));
    RETURN_IF_ERROR(remote_data_cache_->Init());
  }
  return Status::OK();
}

unique_ptr<RequestContext> DiskIoMgr::RegisterContext() {
  return unique_ptr<RequestContext>(new RequestContext(this, disk_queues_));
}

void DiskIoMgr::UnregisterContext(RequestContext* reader) {
  reader->CancelAndMarkInactive();
}

Status DiskIoMgr::ValidateScanRange(ScanRange* range) {
  int disk_id = range->disk_id();
  if (disk_id < 0 || disk_id >= disk_queues_.size()) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Invalid scan range. Bad disk id: $0", disk_id));
  }
  if (range->offset() < 0) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Invalid scan range. Negative offset $0", range->offset()));
  }
  if (range->len() <= 0) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Invalid scan range. Non-positive length $0", range->len()));
  }
  if (range->bytes_to_read() <= 0) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Invalid scan range. Non-positive bytes to read $0",
                   range->bytes_to_read()));
  }
  return Status::OK();
}

Status DiskIoMgr::AllocateBuffersForRange(
    BufferPool::ClientHandle* bp_client, ScanRange* range, int64_t max_bytes) {
  return range->AllocateBuffersForRange(bp_client, max_bytes,
      min_buffer_size_, max_buffer_size_);
}

Status DiskIoMgr::DumpDataCache() {
  if (FLAGS_data_cache_keep_across_restarts && remote_data_cache_) {
    return remote_data_cache_->Dump();
  }
  return Status::Expected("No cache dump is required.");
}

vector<int64_t> DiskIoMgr::ChooseBufferSizes(int64_t scan_range_len, int64_t max_bytes) {
  DCHECK_GE(max_bytes, min_buffer_size_);
  vector<int64_t> buffer_sizes;
  int64_t bytes_allocated = 0;
  while (bytes_allocated < scan_range_len) {
    int64_t bytes_remaining = scan_range_len - bytes_allocated;
    // Either allocate a max-sized buffer or a smaller buffer to fit the rest of the
    // range.
    int64_t next_buffer_size;
    if (bytes_remaining >= max_buffer_size_) {
      next_buffer_size = max_buffer_size_;
    } else {
      next_buffer_size =
          max(min_buffer_size_, BitUtil::RoundUpToPowerOfTwo(bytes_remaining));
    }
    if (next_buffer_size + bytes_allocated > max_bytes) {
      // Can't allocate the desired buffer size. Make sure to allocate at least one
      // buffer.
      if (bytes_allocated > 0) break;
      next_buffer_size = BitUtil::RoundDownToPowerOfTwo(max_bytes);
    }
    DCHECK(BitUtil::IsPowerOf2(next_buffer_size)) << next_buffer_size;
    buffer_sizes.push_back(next_buffer_size);
    bytes_allocated += next_buffer_size;
  }
  return buffer_sizes;
}

int64_t DiskIoMgr::ComputeIdealBufferReservation(int64_t scan_range_len) {
  if (scan_range_len < max_buffer_size_) {
    // Round up to nearest power-of-two buffer size - ideally we should do a single read
    // I/O for this range.
    return max(min_buffer_size_, BitUtil::RoundUpToPowerOfTwo(scan_range_len));
  } else {
    // Round up to the nearest max-sized I/O buffer, capped by
    // IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE - we should do one or more max-sized read
    // I/Os for this range.
    return min(IDEAL_MAX_SIZED_BUFFERS_PER_SCAN_RANGE * max_buffer_size_,
            BitUtil::RoundUpToPowerOf2(scan_range_len, max_buffer_size_));
  }
}

// This function gets the next RequestRange to work on for this disk. It blocks until
// work is available or the thread is shut down.
// Work is available if there is a RequestContext with
//  - A ScanRange with a buffer available, or
//  - A WriteRange in unstarted_write_ranges_ or
//  - A RemoteOperRange in unstarted_remote_file_op_ranges_.
RequestRange* DiskQueue::GetNextRequestRange(RequestContext** request_context) {
  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *request_context = nullptr;
    {
      unique_lock<mutex> disk_lock(lock_);
      while (!shut_down_ && request_contexts_.empty()) {
        // wait if there are no readers on the queue
        work_available_.Wait(disk_lock);
      }
      if (shut_down_) break;
      DCHECK(!request_contexts_.empty());

      // Get the next reader and remove the reader so that another disk thread
      // can't pick it up. It will be enqueued before issuing the read to HDFS
      // so this is not a big deal (i.e. multiple disk threads can read for the
      // same reader).
      *request_context = request_contexts_.front();
      request_contexts_.pop_front();
      DCHECK(*request_context != nullptr);
      // Must increment refcount to keep RequestContext after dropping 'disk_lock'
      (*request_context)->IncrementDiskThreadAfterDequeue(disk_id_);
    }
    // Get the next range to process for this reader. If this context does not have a
    // range, rinse and repeat.
    RequestRange* range = (*request_context)->GetNextRequestRange(disk_id_);
    if (range != nullptr) return range;
  }
  DCHECK(shut_down_);
  return nullptr;
}

void DiskQueue::DiskThreadLoop(DiskIoMgr* io_mgr) {
  // The thread waits until there is work or the queue is shut down. If there is work,
  // performs the read or write requested. Locks are not taken when reading from or
  // writing to disk.
  while (true) {
    RequestContext* worker_context = nullptr;
    RequestRange* range = GetNextRequestRange(&worker_context);
    if (range == nullptr) {
      DCHECK(shut_down_);
      return;
    }
    // We are now working on behalf of a query, so set thread state appropriately.
    // See also IMPALA-6254 and IMPALA-6417.
    ScopedThreadContext tdi_scope(GetThreadDebugInfo(), worker_context->query_id(),
        worker_context->instance_id());

    switch (range->request_type()) {
      case RequestType::READ: {
        ScanRange* scan_range = static_cast<ScanRange*>(range);
        ReadOutcome outcome = scan_range->DoRead(this, disk_id_);
        worker_context->ReadDone(disk_id_, outcome, scan_range);
        break;
      }
      case RequestType::WRITE: {
        WriteRange* write_range = static_cast<WriteRange*>(range);
        Status status = write_range->DoWrite();
        worker_context->OperDone(write_range, status);
        break;
      }
      case RequestType::FILE_UPLOAD: {
        RemoteOperRange* oper_range = static_cast<RemoteOperRange*>(range);
        int64_t size = oper_range->block_size();
        // Use malloc to get the memory in case there is no available space
        // in the buffer pool because spilling to disk happens when scarcity
        // of memory in the buffer pool. Be better to preserve memory than
        // malloc.
        uint8_t* buffer = static_cast<uint8_t*>(malloc(size));
        if (UNLIKELY(buffer == nullptr)) {
          worker_context->OperDone(oper_range,
              Status(Substitute("Couldn't allocate memory for remote file operations, "
                                "block size: '$0'",
                  size)));
        } else {
          Status oper_status = oper_range->DoUpload(buffer, size);
          worker_context->OperDone(oper_range, oper_status);
          free(buffer);
        }
        break;
      }
      case RequestType::FILE_FETCH: {
        RemoteOperRange* oper_range = static_cast<RemoteOperRange*>(range);
        Status oper_status = oper_range->DoFetch();
        worker_context->OperDone(oper_range, oper_status);
        break;
      }
      default:
        DCHECK(false) << "Invalid request type: " << range->request_type();
    }
  }
}

void DiskIoMgr::Write(RequestContext* writer_context, WriteRange* write_range) {
  Status ret_status = Status::OK();
  FILE* file_handle = nullptr;
  Status close_status = Status::OK();
  DiskQueue* queue = disk_queues_[write_range->disk_id()];

  {
    ScopedHistogramTimer write_timer(queue->write_latency());
    ret_status = local_file_system_->OpenForWrite(
        write_range->file(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR, &file_handle);
    if (!ret_status.ok()) goto end;

    ret_status = WriteRangeHelper(file_handle, write_range);

    close_status = local_file_system_->Fclose(file_handle, write_range->file());
    if (ret_status.ok() && !close_status.ok()) ret_status = close_status;
  }

end:
  if (ret_status.ok()) {
    queue->write_size()->Update(write_range->len());
  } else {
    queue->write_io_err()->Increment(1);
  }
  writer_context->OperDone(write_range, ret_status);
}

Status DiskIoMgr::WriteRangeHelper(FILE* file_handle, WriteRange* write_range) {
  // Seek to the correct offset and perform the write.
  RETURN_IF_ERROR(local_file_system_->Fseek(
      file_handle, write_range->offset(), SEEK_SET, write_range));

#ifndef NDEBUG
  if (FLAGS_stress_scratch_write_delay_ms > 0) {
    SleepForMs(FLAGS_stress_scratch_write_delay_ms);
  }
#endif
  RETURN_IF_ERROR(local_file_system_->Fwrite(file_handle, write_range));

  ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len());
  return Status::OK();
}

int DiskIoMgr::AssignQueue(
    const char* file, int disk_id, bool expected_local, bool check_default_fs) {
  // If it's a remote range, check for an appropriate remote disk queue.
  if (!expected_local) {
    if (IsHdfsPath(file, check_default_fs) && FLAGS_num_remote_hdfs_io_threads > 0) {
      return RemoteDfsDiskId();
    }
    if (IsS3APath(file, check_default_fs)) return RemoteS3DiskId();
    if (IsABFSPath(file, check_default_fs)) return RemoteAbfsDiskId();
    if (IsADLSPath(file, check_default_fs)) return RemoteAdlsDiskId();
    if (IsOSSPath(file, check_default_fs)) return RemoteOSSDiskId();
    if (IsGcsPath(file, check_default_fs)) return RemoteGcsDiskId();
    if (IsCosPath(file, check_default_fs)) return RemoteCosDiskId();
    if (IsOzonePath(file, check_default_fs)) return RemoteOzoneDiskId();
    if (IsSFSPath(file, check_default_fs)) return RemoteSFSDiskId();
    if (IsOBSPath(file, check_default_fs)) return RemoteOBSDiskId();
  }
  // Assign to a local disk queue.
  DCHECK(!IsS3APath(file, check_default_fs)); // S3 is always remote.
  DCHECK(!IsABFSPath(file, check_default_fs)); // ABFS is always remote.
  DCHECK(!IsADLSPath(file, check_default_fs)); // ADLS is always remote.
  DCHECK(!IsOSSPath(file, check_default_fs)); // OSS/JindoFS is always remote.
  DCHECK(!IsGcsPath(file, check_default_fs)); // GCS is always remote.
  DCHECK(!IsCosPath(file, check_default_fs)); // COS is always remote.
  DCHECK(!IsSFSPath(file, check_default_fs)); // SFS is always remote.
  DCHECK(!IsOBSPath(file, check_default_fs)); // OBS is always remote.
  if (disk_id == -1) {
    // disk id is unknown, assign it an arbitrary one.
    disk_id = next_disk_id_.Add(1);
  }
  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  return disk_id % num_local_disks();
}

Status DiskIoMgr::GetExclusiveHdfsFileHandle(const hdfsFS& fs,
    std::string* fname, int64_t mtime, RequestContext *reader,
    unique_ptr<ExclusiveHdfsFileHandle>& fid_out) {
  SCOPED_TIMER(reader->open_file_timer_);
  unique_ptr<ExclusiveHdfsFileHandle> fid;
  fid.reset(new ExclusiveHdfsFileHandle(fs, fname, mtime));
  RETURN_IF_ERROR(fid->Init(&hdfs_monitor_));
  fid_out.swap(fid);
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  // Every exclusive file handle is considered a cache miss
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(0L);
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT->Increment(1L);
  reader->cached_file_handles_miss_count_.Add(1L);
  return Status::OK();
}

void DiskIoMgr::ReleaseExclusiveHdfsFileHandle(unique_ptr<ExclusiveHdfsFileHandle> fid) {
  DCHECK(fid != nullptr);
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
  fid.reset();
}

Status DiskIoMgr::GetCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname,
    int64_t mtime, RequestContext* reader, FileHandleCache::Accessor* accessor) {
  bool cache_hit;
  SCOPED_TIMER(reader->open_file_timer_);
  RETURN_IF_ERROR(
      file_handle_cache_.GetFileHandle(fs, fname, mtime, false, accessor, &cache_hit));
  if (cache_hit) {
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(1L);
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_COUNT->Increment(1L);
    reader->cached_file_handles_hit_count_.Add(1L);
  } else {
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_HIT_RATIO->Update(0L);
    ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_MISS_COUNT->Increment(1L);
    reader->cached_file_handles_miss_count_.Add(1L);
  }
  return Status::OK();
}

Status DiskIoMgr::ReopenCachedHdfsFileHandle(const hdfsFS& fs, std::string* fname,
    int64_t mtime, RequestContext* reader, FileHandleCache::Accessor* accessor) {
  bool cache_hit;
  SCOPED_TIMER(reader->open_file_timer_);
  ImpaladMetrics::IO_MGR_CACHED_FILE_HANDLES_REOPENED->Increment(1L);

  accessor->Destroy();

  Status status =
      file_handle_cache_.GetFileHandle(fs, fname, mtime, true, accessor, &cache_hit);
  if (!status.ok()) {
    return status;
  }
  DCHECK(!cache_hit);
  return Status::OK();
}

void DiskQueue::ShutDown() {
  {
    unique_lock<mutex> disk_lock(lock_);
    shut_down_ = true;
  }
  // All waiting threads should exit, so wake them all up.
  work_available_.NotifyAll();
}

void DiskQueue::DebugString(stringstream* ss) {
  unique_lock<mutex> lock(lock_);
  *ss << "DiskQueue id=" << disk_id_ << " ptr=" << static_cast<void*>(this) << ":" ;
  if (!request_contexts_.empty()) {
    *ss << " Readers: ";
    for (RequestContext* req_context: request_contexts_) {
      *ss << static_cast<void*>(req_context);
    }
  }
}

DiskQueue::~DiskQueue() {
  for (RequestContext* context : request_contexts_) {
    context->UnregisterDiskQueue(disk_id_);
  }
}
