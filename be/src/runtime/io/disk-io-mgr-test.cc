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

#include <sched.h>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <sys/stat.h>

#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/io/cache-reader-test-stub.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/disk-io-mgr-stress.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/local-file-system-with-fault-injection.h"
#include "runtime/io/request-context.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "runtime/tmp-file-mgr.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/condition-variable.h"
#include "util/debug-util.h"
#include "util/filesystem-util.h"
#include "util/histogram-metric.h"
#include "util/thread.h"
#include "util/time.h"

#include "common/names.h"

using std::mt19937;
using std::uniform_int_distribution;
using std::uniform_real_distribution;

DECLARE_int64(min_buffer_size);
DECLARE_int32(num_remote_hdfs_io_threads);
DECLARE_int32(num_s3_io_threads);
DECLARE_int32(num_adls_io_threads);
DECLARE_int32(num_abfs_io_threads);
DECLARE_int32(num_gcs_io_threads);
DECLARE_int32(num_cos_io_threads);
DECLARE_int32(num_ozone_io_threads);
DECLARE_int32(num_oss_io_threads);
DECLARE_int32(num_remote_hdfs_file_oper_io_threads);
DECLARE_int32(num_s3_file_oper_io_threads);
DECLARE_int32(num_sfs_io_threads);

#ifndef NDEBUG
DECLARE_int32(stress_disk_read_delay_ms);
#endif

DECLARE_string(remote_tmp_file_size);
DECLARE_string(remote_tmp_file_block_size);

const int MIN_BUFFER_SIZE = 128;
const int MAX_BUFFER_SIZE = 1024;
const int64_t LARGE_RESERVATION_LIMIT = 4L * 1024L * 1024L * 1024L;
const int64_t LARGE_INITIAL_RESERVATION = 128L * 1024L * 1024L;
const int64_t BUFFER_POOL_CAPACITY = LARGE_RESERVATION_LIMIT;

/// For testing spill to remote.
static const string HDFS_LOCAL_URL = "hdfs://localhost:20500/tmp";
static const string REMOTE_URL = HDFS_LOCAL_URL;
static const string LOCAL_BUFFER_PATH = "/tmp/tmp-file-mgr-test-buffer";

namespace impala {
namespace io {

static Status CONTEXT_CANCELLED_STATUS =
    Status::CancelledInternal("IoMgr RequestContext");

class DiskIoMgrTest : public testing::Test {
 public:
  virtual void SetUp() {
    FLAGS_remote_tmp_file_block_size = "1K";

    test_env_.reset(new TestEnv);
    // Tests try to allocate arbitrarily small buffers. Ensure Buffer Pool allows it.
    test_env_->SetBufferPoolArgs(1, BUFFER_POOL_CAPACITY);
    ASSERT_OK(test_env_->Init());
    metrics_.reset(new MetricGroup("disk-io-mgr-test"));
    RandTestUtil::SeedRng("DISK_IO_MGR_TEST_SEED", &rng_);
  }

  virtual void TearDown() {
    root_reservation_.Close();
    pool_.Clear();
    test_env_.reset();
  }

  /// Initialises 'root_reservation_'. The reservation is automatically closed in
  /// TearDown().
  void InitRootReservation(int64_t reservation_limit) {
    root_reservation_.InitRootTracker(
        RuntimeProfile::Create(&pool_, "root"), reservation_limit);
  }

  /// Initialise 'client' with the given reservation limit. The client reservation is a
  /// child of 'root_reservation_'.
  void RegisterBufferPoolClient(int64_t reservation_limit, int64_t initial_reservation,
      BufferPool::ClientHandle* client) {
    ASSERT_OK(buffer_pool()->RegisterClient("", nullptr, &root_reservation_, nullptr,
        reservation_limit, RuntimeProfile::Create(&pool_, ""), client));
    if (initial_reservation > 0) {
      ASSERT_TRUE(client->IncreaseReservation(initial_reservation));
    }
  }

  void WriteValidateCallback(int num_writes, WriteRange** written_range,
      DiskIoMgr* io_mgr, RequestContext* reader, BufferPool::ClientHandle* client,
      int32_t* data, Status expected_status, const Status& status) {
    if (expected_status.code() == TErrorCode::CANCELLED_INTERNALLY) {
      EXPECT_TRUE(status.ok() || status.IsCancelled()) << "Error: " << status.GetDetail();
    } else {
      EXPECT_EQ(status.code(), expected_status.code());
    }
    if (status.ok()) {
      ScanRange* scan_range = pool_.Add(new ScanRange());
      scan_range->Reset(nullptr, (*written_range)->file(), (*written_range)->len(),
          (*written_range)->offset(), 0, false, ScanRange::INVALID_MTIME,
          BufferOpts::Uncached());
      ValidateSyncRead(io_mgr, reader, client, scan_range,
          reinterpret_cast<const char*>(data), sizeof(int32_t));
    }

    if (!status.ok()) {
      EXPECT_EQ(expected_status.GetDetail(), status.GetDetail());
    }

    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.NotifyOne();
    }
  }

  void WriteCompleteCallback(int num_writes, const Status& status) {
    EXPECT_OK(status);
    {
      lock_guard<mutex> l(written_mutex_);
      ++num_ranges_written_;
      if (num_ranges_written_ == num_writes) writes_done_.NotifyAll();
    }
  }

  RuntimeProfile* NewProfile() { return RuntimeProfile::Create(&pool_, "test profile"); }

  /// Create a new file group with the default configs.
  TmpFileGroup* NewFileGroup(DiskIoMgr* io_mgr = nullptr) {
    if (io_mgr == nullptr) io_mgr = test_env_->exec_env()->disk_io_mgr();
    TmpFileGroup* file_group = pool_.Add(
        new TmpFileGroup(test_env_->tmp_file_mgr(), io_mgr, NewProfile(), TUniqueId()));
    return file_group;
  }

  /// Create a new file group with spiiling to remote setting.
  TmpFileGroup* NewRemoteFileGroup(TmpFileMgr* tmp_file_mgr, DiskIoMgr* io_mgr) {
    if (io_mgr == nullptr) io_mgr = test_env_->exec_env()->disk_io_mgr();
    vector<string> tmp_dirs({LOCAL_BUFFER_PATH});
    RemoveAndCreateDirs(tmp_dirs);
    tmp_dirs.push_back(REMOTE_URL);
    Status status = tmp_file_mgr->InitCustom(tmp_dirs, true, "", false, metrics_.get());
    if (!status.ok()) return nullptr;
    return pool_.Add(new TmpFileGroup(tmp_file_mgr, io_mgr, NewProfile(), TUniqueId()));
  }

  bool FileExist(const std::string& name) {
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
  }

  bool HdfsFileExist(const std::string& name) {
    return (hdfsExists(hdfsConnect("default", 0), name.c_str()) == 0);
  }

 protected:
  void CreateTempFile(const char* filename, const char* data) {
    CreateTempFile(filename, data, strlen(data));
  }

  void CreateTempFile(const char* filename, const char* data, int64_t data_bytes) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != nullptr);
    fwrite(data, 1, data_bytes, file);
    fclose(file);
  }

  int CreateTempFile(const char* filename, int file_size) {
    FILE* file = fopen(filename, "w");
    EXPECT_TRUE(file != nullptr);
    int success = fclose(file);
    if (success != 0) {
      LOG(ERROR) << "Error closing file " << filename;
      return success;
    }
    return truncate(filename, file_size);
  }

  void RemoveAndCreateDirs(const vector<string>& dirs) {
    for (const string& dir : dirs) {
      ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(dir));
    }
  }

  // Validates that buffer[i] is \0 or expected[i]
  static void ValidateEmptyOrCorrect(const char* expected, const char* buffer, int len) {
    for (int i = 0; i < len; ++i) {
      if (buffer[i] != '\0') {
        EXPECT_EQ(expected[i], buffer[i]) << (int)expected[i] << " != " << (int)buffer[i];
      }
    }
  }

  static void ValidateSyncRead(DiskIoMgr* io_mgr, RequestContext* reader,
      BufferPool::ClientHandle* client, ScanRange* range, const char* expected,
      int expected_len = -1) {
    unique_ptr<BufferDescriptor> buffer;
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    if (needs_buffers) {
      ASSERT_OK(io_mgr->AllocateBuffersForRange(
          client, range, io_mgr->max_buffer_size()));
    }
    ASSERT_OK(range->GetNext(&buffer));
    ASSERT_TRUE(buffer != nullptr);
    EXPECT_EQ(buffer->len(), range->bytes_to_read());
    if (expected_len < 0) expected_len = strlen(expected);
    int cmp = memcmp(buffer->buffer(), expected, expected_len);
    EXPECT_TRUE(cmp == 0);
    range->ReturnBuffer(move(buffer));
  }

  static void ValidateScanRange(DiskIoMgr* io_mgr, ScanRange* range,
      const char* expected, int expected_len, const Status& expected_status) {
    char result[expected_len + 1];
    memset(result, 0, expected_len + 1);
    int64_t scan_range_offset = 0;

    while (true) {
      unique_ptr<BufferDescriptor> buffer;
      Status status = range->GetNext(&buffer);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (buffer == nullptr || !status.ok()) {
        if (buffer != nullptr) range->ReturnBuffer(move(buffer));
        break;
      }
      ASSERT_LE(buffer->len(), expected_len);
      memcpy(result + range->offset() + scan_range_offset,
          buffer->buffer(), buffer->len());
      scan_range_offset += buffer->len();
      range->ReturnBuffer(move(buffer));
    }
    ValidateEmptyOrCorrect(expected, result, expected_len);
  }

  // Continues pulling scan ranges from the io mgr until they are all done.
  // Updates num_ranges_processed with the number of ranges seen by this thread.
  static void ScanRangeThread(DiskIoMgr* io_mgr, RequestContext* reader,
      BufferPool::ClientHandle* client, const char* expected_result, int expected_len,
      const Status& expected_status, int max_ranges, AtomicInt32* num_ranges_processed) {
    int num_ranges = 0;
    while (max_ranges == 0 || num_ranges < max_ranges) {
      ScanRange* range;
      bool needs_buffers;
      Status status = reader->GetNextUnstartedRange(&range, &needs_buffers);
      ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
      if (range == nullptr) break;
      if (needs_buffers) {
        ASSERT_OK(io_mgr->AllocateBuffersForRange(
            client, range, io_mgr->max_buffer_size() * 3));
      }
      ValidateScanRange(io_mgr, range, expected_result, expected_len, expected_status);
      num_ranges_processed->Add(1);
      ++num_ranges;
    }
  }

  static void SetReaderStub(ScanRange* scan_range, unique_ptr<FileReader> reader_stub) {
    scan_range->SetFileReader(move(reader_stub));
  }

  ScanRange* InitRange(ObjectPool* pool, const char* file_path, int offset, int len,
      int disk_id, int64_t mtime, void* meta_data = nullptr, bool is_hdfs_cached = false,
      std::vector<ScanRange::SubRange> sub_ranges = {}) {
    ScanRange* range = pool->Add(new ScanRange);
    int cache_options =
        is_hdfs_cached ? BufferOpts::USE_HDFS_CACHE : BufferOpts::NO_CACHING;
    range->Reset(nullptr, file_path, len, offset, disk_id, true, mtime,
        BufferOpts(cache_options), move(sub_ranges), meta_data);
    EXPECT_EQ(mtime, range->mtime());
    return range;
  }

  // Function to trigger Write() on DiskIoMgr with the purpose of deliberately making it
  // fail through a failure injection interface. The purpose is to test the error outputs
  // in the cases when open(), fdopen(), fseek(), fwrite() and fclose() fail.
  // 'function_name' specifies the name of the function where the failure is injected.
  //  e.g. "fdopen"
  void ExecuteWriteFailureTest(DiskIoMgr* io_mgr, const string& file_name,
      const string& function_name, int err_no, const string& expected_error);

  // Auxiliary function to ExecuteWriteFailureTest(). Handles the
  // AddWriteRange() call.
  void AddWriteRange(int num_of_writes, int32_t* data, const string& tmp_file, int offset,
      RequestContext* writer, const string& expected_output, TmpFileGroup* file_group);

  void SingleReaderTestBody(const char* data, const char* expected_result,
      vector<ScanRange::SubRange> sub_ranges = {});

  void CachedReadsTestBody(const char* data, const char* expected,
      bool fake_cache, vector<ScanRange::SubRange> sub_ranges = {});

  /// Convenience function to get a reference to the buffer pool.
  BufferPool* buffer_pool() const { return ExecEnv::GetInstance()->buffer_pool(); }

  boost::scoped_ptr<TestEnv> test_env_;

  /// Per-test random number generator. Seeded before every test.
  mt19937 rng_;

  ObjectPool pool_;

  scoped_ptr<MetricGroup> metrics_;

  /// The file groups created - closed at end of each test.
  vector<TmpFileGroup*> file_groups_;

  ReservationTracker root_reservation_;

  mutex written_mutex_;
  ConditionVariable writes_done_;
  int num_ranges_written_;

  mutex oper_mutex_;
  ConditionVariable oper_done_;
  int num_oper_;
};

TEST_F(DiskIoMgrTest, TestDisk) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 1;
  int64_t cur_offset = 0;

  scoped_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 1, 10));
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
  ASSERT_OK(read_io_mgr->Init());
  unique_ptr<RequestContext> reader = read_io_mgr->RegisterContext();
  TmpFileGroup* tmp_file_grp = NewFileGroup();
  // Pool for temporary objects from this iteration only.
  ObjectPool tmp_pool;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
  for (int i = 0; i < 1; ++i) {
    int32_t* data = tmp_pool.Add(new int32_t);
    *data = 5;
    WriteRange** new_range = tmp_pool.Add(new WriteRange*);
    WriteRange::WriteDoneCallback callback =
        bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_ranges, new_range,
            read_io_mgr.get(), reader.get(), &read_client, data, Status::OK(), _1);
    *new_range = tmp_pool.Add(new WriteRange(tmp_file, cur_offset, 0, callback));
    (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
    *new_tmp_file_obj = tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
    (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
    EXPECT_OK(writer->AddWriteRange(*new_range));
    cur_offset += sizeof(int32_t);
  }

  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < 1) writes_done_.Wait(lock);
  }
  num_ranges_written_ = 0;
  io_mgr.UnregisterContext(writer.get());

  tmp_file_grp->Close();
  read_io_mgr->UnregisterContext(reader.get());
  buffer_pool()->DeregisterClient(&read_client);
}

// Test a single writer with multiple disks and threads per disk. Each WriteRange
// writes random 4-byte integers, and upon completion, the written data is validated
// by reading the data back via a separate IoMgr instance. All writes are expected to
// complete successfully.
TEST_F(DiskIoMgrTest, SingleWriter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 100;
  int64_t file_size = 1024 * 1024;
  int64_t cur_offset = 0;
  int success = CreateTempFile(tmp_file.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " <<
        file_size;
    EXPECT_TRUE(false);
  }

  scoped_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 1, 10));
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
  ASSERT_OK(read_io_mgr->Init());
  unique_ptr<RequestContext> reader = read_io_mgr->RegisterContext();
  TmpFileGroup* tmp_file_grp = NewFileGroup();
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init());
      unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
      for (int i = 0; i < num_ranges; ++i) {
        int32_t* data = tmp_pool.Add(new int32_t);
        *data = rand();
        WriteRange** new_range = tmp_pool.Add(new WriteRange*);
        WriteRange::WriteDoneCallback callback = bind(
            mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_ranges, new_range,
            read_io_mgr.get(), reader.get(), &read_client, data, Status::OK(), _1);
        *new_range = tmp_pool.Add(
            new WriteRange(tmp_file, cur_offset, num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
        *new_tmp_file_obj = tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
        (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
        EXPECT_OK(writer->AddWriteRange(*new_range));
        cur_offset += sizeof(int32_t);
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges) writes_done_.Wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer.get());
    }
  }

  tmp_file_grp->Close();
  read_io_mgr->UnregisterContext(reader.get());
  buffer_pool()->DeregisterClient(&read_client);
}

// Perform invalid writes (e.g. file in non-existent directory, negative offset) and
// validate that an error status is returned via the write callback.
TEST_F(DiskIoMgrTest, InvalidWrite) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  int num_of_writes = 2;
  num_ranges_written_ = 0;
  string tmp_file = "/non-existent/file.txt";
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
  int32_t* data = pool_.Add(new int32_t);
  *data = rand();
  TmpFileGroup* tmp_file_grp = NewFileGroup();

  // Write to file in non-existent directory.
  WriteRange** new_range = pool_.Add(new WriteRange*);
  TmpFile** new_tmp_file_obj = pool_.Add(new TmpFile*);
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_of_writes, new_range,
          nullptr, nullptr, nullptr, data,
          Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
            "open() failed for /non-existent/file.txt. "
            "The given path doesn't exist. errno=2"), _1);
  *new_range = pool_.Add(new WriteRange(tmp_file, rand(), 0, callback));

  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  *new_tmp_file_obj = pool_.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
  (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
  EXPECT_OK(writer->AddWriteRange(*new_range));

  // Write to a bad location in a file that exists.
  tmp_file = "/tmp/disk_io_mgr_test.txt";
  int success = CreateTempFile(tmp_file.c_str(), 100);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size 100";
    EXPECT_TRUE(false);
  }

  new_range = pool_.Add(new WriteRange*);
  new_tmp_file_obj = pool_.Add(new TmpFile*);
  callback = bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_of_writes,
      new_range, nullptr, nullptr, nullptr, data,
      Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          "fseek() failed for /tmp/disk_io_mgr_test.txt. "
          "Invalid inputs. errno=22, offset=-1"), _1);

  *new_range = pool_.Add(new WriteRange(tmp_file, -1, 0, callback));
  *new_tmp_file_obj = pool_.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
  (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  EXPECT_OK(writer->AddWriteRange(*new_range));

  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < num_of_writes) writes_done_.Wait(lock);
  }
  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(writer.get());
}

// Tests the error messages when some of the disk I/O related low level function fails in
// DiskIoMgr::Write()
TEST_F(DiskIoMgrTest, WriteErrors) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  string file_name = "/tmp/disk_io_mgr_test.txt";

  // Fail open()
  string expected_error = Substitute("open() failed for $0. Either the path length or a "
      "path component exceeds the maximum length. errno=36", file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "open", ENAMETOOLONG, expected_error);

  // Fail fdopen()
  expected_error = Substitute("fdopen() failed for $0. Not enough memory. errno=12",
      file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "fdopen", ENOMEM, expected_error);

  // Fail fseek()
  expected_error = Substitute("fseek() failed for $0. Maximum file size reached. "
      "errno=27, offset=0", file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "fseek", EFBIG, expected_error);

  // Fail fwrite()
  expected_error = Substitute("fwrite() failed for $0. Disk level I/O error occured. "
      "errno=5, range_length=4", file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "fwrite", EIO, expected_error);

  // Fail fclose()
  expected_error = Substitute("fclose() failed for $0. Device doesn't exist. errno=6",
      file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "fclose", ENXIO, expected_error);

  // Fail open() with unknown error code to the ErrorConverter. This results falling back
  // to the GetStrErrMsg() logic.
  expected_error = Substitute("open() failed for $0. errno=49, description=Error(49): "
                              "Protocol driver not attached", file_name);
  ExecuteWriteFailureTest(&io_mgr, file_name, "open", EUNATCH, expected_error);
}

void DiskIoMgrTest::ExecuteWriteFailureTest(DiskIoMgr* io_mgr, const string& file_name,
    const string& function_name, int err_no, const string& expected_error) {
  int num_of_writes = 1;
  num_ranges_written_ = 0;
  unique_ptr<RequestContext> writer = io_mgr->RegisterContext();
  int32_t data = rand();
  int success = CreateTempFile(file_name.c_str(), 100);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << file_name << " of size 100";
    EXPECT_TRUE(false);
  }
  TmpFileGroup* tmp_file_grp = NewFileGroup(io_mgr);

  std::unique_ptr<LocalFileSystemWithFaultInjection> fs(
      new LocalFileSystemWithFaultInjection());
  fs->SetWriteFaultInjection(function_name, err_no);
  // DiskIoMgr takes responsibility of fs from this point.
  io_mgr->SetLocalFileSystem(std::move(fs));
  AddWriteRange(
      num_of_writes, &data, file_name, 0, writer.get(), expected_error, tmp_file_grp);

  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < num_of_writes) writes_done_.Wait(lock);
  }
  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr->UnregisterContext(writer.get());
}

void DiskIoMgrTest::AddWriteRange(int num_of_writes, int32_t* data,
    const string& file_name, int offset, RequestContext* writer,
    const string& expected_output, TmpFileGroup* file_group) {
  WriteRange::WriteDoneCallback callback =
      bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this, num_of_writes,
          nullptr, nullptr, nullptr, nullptr, data,
          Status(TErrorCode::DISK_IO_ERROR, GetBackendString(), expected_output), _1);
  WriteRange* write_range = pool_.Add(new WriteRange(file_name, offset, 0, callback));
  write_range->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  TmpFile** new_tmp_file_obj = pool_.Add(new TmpFile*);
  *new_tmp_file_obj = pool_.Add(new TmpFileLocal(file_group, 0, file_name));
  write_range->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
  EXPECT_OK(writer->AddWriteRange(write_range));
}

// Issue a number of writes, cancel the writer context and issue more writes.
// AddWriteRange() is expected to succeed before the cancel and fail after it.
// The writes themselves may finish with status cancelled or ok.
TEST_F(DiskIoMgrTest, SingleWriterCancel) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 100;
  int num_ranges_before_cancel = 25;
  int64_t file_size = 1024 * 1024;
  int64_t cur_offset = 0;
  int success = CreateTempFile(tmp_file.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " <<
        file_size;
    EXPECT_TRUE(false);
  }
  TmpFileGroup* tmp_file_grp = NewFileGroup();

  scoped_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 1, 10));
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
  ASSERT_OK(read_io_mgr->Init());
  unique_ptr<RequestContext> reader = read_io_mgr->RegisterContext();
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 10);
      ASSERT_OK(io_mgr.Init());
      unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
      Status validate_status = Status::OK();
      for (int i = 0; i < num_ranges; ++i) {
        if (i == num_ranges_before_cancel) {
          writer->Cancel();
          validate_status = CONTEXT_CANCELLED_STATUS;
        }
        int32_t* data = tmp_pool.Add(new int32_t);
        *data = rand();
        WriteRange** new_range = tmp_pool.Add(new WriteRange*);
        WriteRange::WriteDoneCallback callback =
            bind(mem_fn(&DiskIoMgrTest::WriteValidateCallback), this,
                num_ranges_before_cancel, new_range, read_io_mgr.get(), reader.get(),
                &read_client, data, CONTEXT_CANCELLED_STATUS, _1);
        *new_range = tmp_pool.Add(
            new WriteRange(tmp_file, cur_offset, num_ranges % num_disks, callback));
        (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
        TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
        *new_tmp_file_obj = tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
        (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
        cur_offset += sizeof(int32_t);
        Status add_status = writer->AddWriteRange(*new_range);
        EXPECT_TRUE(add_status.code() == validate_status.code());
      }

      {
        unique_lock<mutex> lock(written_mutex_);
        while (num_ranges_written_ < num_ranges_before_cancel) writes_done_.Wait(lock);
      }
      num_ranges_written_ = 0;
      io_mgr.UnregisterContext(writer.get());
    }
  }

  tmp_file_grp->Close();
  read_io_mgr->UnregisterContext(reader.get());
  buffer_pool()->DeregisterClient(&read_client);
}

void DiskIoMgrTest::SingleReaderTestBody(const char* data, const char* expected_result,
    vector<ScanRange::SubRange> sub_ranges) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  int data_len = strlen(data);
  int expected_result_len = strlen(expected_result);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      for (int num_read_threads = 1; num_read_threads <= 5; ++num_read_threads) {
        ObjectPool tmp_pool;
        LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                  << " num_disk=" << num_disks
                  << " num_read_threads=" << num_read_threads;

        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

        ASSERT_OK(io_mgr.Init());
        BufferPool::ClientHandle read_client;
        RegisterBufferPoolClient(
            LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
        unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

        vector<ScanRange*> ranges;
        for (int i = 0; i < data_len; ++i) {
          int disk_id = i % num_disks;
          ranges.push_back(InitRange(&tmp_pool, tmp_file, 0, data_len, disk_id,
              stat_val.st_mtime, nullptr, false, sub_ranges));
        }
        ASSERT_OK(reader->AddScanRanges(ranges, EnqueueLocation::TAIL));

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < num_read_threads; ++i) {
          threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(),
              &read_client, expected_result, expected_result_len, Status::OK(), 0,
              &num_ranges_processed));
        }
        threads.join_all();

        EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
        io_mgr.UnregisterContext(reader.get());
        EXPECT_EQ(read_client.GetUsedReservation(), 0);
        buffer_pool()->DeregisterClient(&read_client);
      }
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* data = "abcdefghijklm";
  SingleReaderTestBody(data, data);
}

TEST_F(DiskIoMgrTest, SingleReaderSubRanges) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* data = "abcdefghijklm";
  int64_t data_len = strlen(data);
  SingleReaderTestBody(data, data, {{0, data_len}});
  SingleReaderTestBody(data, "abdef", {{0, 2}, {3, 3}});
  SingleReaderTestBody(data, "bceflm", {{1, 2}, {4, 2}, {11, 2}});
}

// This test issues adding additional scan ranges while there are some still in flight.
TEST_F(DiskIoMgrTest, AddScanRangeTest) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      vector<ScanRange*> ranges_first_half;
      vector<ScanRange*> ranges_second_half;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        if (i > len / 2) {
          ranges_second_half.push_back(
              InitRange(&tmp_pool, tmp_file, i, 1, disk_id, stat_val.st_mtime));
        } else {
          ranges_first_half.push_back(
              InitRange(&tmp_pool, tmp_file, i, 1, disk_id, stat_val.st_mtime));
        }
      }
      AtomicInt32 num_ranges_processed;

      // Issue first half the scan ranges.
      ASSERT_OK(reader->AddScanRanges(ranges_first_half, EnqueueLocation::TAIL));

      // Read a couple of them
      ScanRangeThread(&io_mgr, reader.get(), &read_client, data, strlen(data),
          Status::OK(), 2, &num_ranges_processed);

      // Issue second half
      ASSERT_OK(reader->AddScanRanges(ranges_second_half, EnqueueLocation::TAIL));

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(
            new thread(ScanRangeThread, &io_mgr, reader.get(), &read_client, data,
                strlen(data), Status::CancelledInternal(""), 0, &num_ranges_processed));
      }

      threads.join_all();
      EXPECT_EQ(num_ranges_processed.Load(), len);
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test to make sure that sync reads and async reads work together
// Note: this test is constructed so the number of buffers is greater than the
// number of scan ranges.
TEST_F(DiskIoMgrTest, SyncReadTest) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk,
          MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      ScanRange* complete_range =
          InitRange(&tmp_pool, tmp_file, 0, strlen(data), 0, stat_val.st_mtime);

      // Issue some reads before the async ones are issued
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      vector<ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(
            InitRange(&tmp_pool, tmp_file, 0, len, disk_id, stat_val.st_mtime));
      }
      ASSERT_OK(reader->AddScanRanges(ranges, EnqueueLocation::TAIL));

      AtomicInt32 num_ranges_processed;
      thread_group threads;
      for (int i = 0; i < 5; ++i) {
        threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(),
            &read_client, data, strlen(data), Status::OK(), 0, &num_ranges_processed));
      }

      // Issue some more sync ranges
      for (int i = 0; i < 5; ++i) {
        sched_yield();
        ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      }

      threads.join_all();

      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, data);

      EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Tests a single reader cancelling half way through scan ranges.
TEST_F(DiskIoMgrTest, SingleReaderCancel) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "abcdefghijklm";
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  int64_t iters = 0;
  for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
    for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
      // Pool for temporary objects from this iteration only.
      ObjectPool tmp_pool;
      LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                << " num_disk=" << num_disks;

      if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
      DiskIoMgr io_mgr(num_disks, num_threads_per_disk, num_threads_per_disk, 1, 1);

      ASSERT_OK(io_mgr.Init());
      BufferPool::ClientHandle read_client;
      RegisterBufferPoolClient(
          LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
      unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

      vector<ScanRange*> ranges;
      for (int i = 0; i < len; ++i) {
        int disk_id = i % num_disks;
        ranges.push_back(
            InitRange(&tmp_pool, tmp_file, 0, len, disk_id, stat_val.st_mtime));
      }
      ASSERT_OK(reader->AddScanRanges(ranges, EnqueueLocation::TAIL));

      AtomicInt32 num_ranges_processed;
      int num_succesful_ranges = ranges.size() / 2;
      // Read half the ranges
      for (int i = 0; i < num_succesful_ranges; ++i) {
        ScanRangeThread(&io_mgr, reader.get(), &read_client, data, strlen(data),
            Status::OK(), 1, &num_ranges_processed);
      }
      EXPECT_EQ(num_ranges_processed.Load(), num_succesful_ranges);

      // Start up some threads and then cancel
      thread_group threads;
      for (int i = 0; i < 3; ++i) {
        threads.add_thread(
            new thread(ScanRangeThread, &io_mgr, reader.get(), &read_client, data,
                strlen(data), Status::CancelledInternal(""), 0, &num_ranges_processed));
      }

      reader->Cancel();
      sched_yield();

      threads.join_all();
      EXPECT_TRUE(reader->IsCancelled());
      io_mgr.UnregisterContext(reader.get());
      EXPECT_EQ(read_client.GetUsedReservation(), 0);
      buffer_pool()->DeregisterClient(&read_client);
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test readers running with different amounts of memory and getting blocked on scan
// ranges that have run out of buffers.
TEST_F(DiskIoMgrTest, MemScarcity) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  // File is 2.5 max buffers so that we can scan file without returning buffers
  // when we get the max reservation below.
  const int64_t DATA_BYTES = MAX_BUFFER_SIZE * 5 / 2;
  char data[DATA_BYTES];
  for (int i = 0; i < DATA_BYTES; ++i) {
    data[i] = uniform_int_distribution<uint8_t>(0, 255)(rng_);
  }
  CreateTempFile(tmp_file, data, DATA_BYTES);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int RESERVATION_LIMIT_NUM_BUFFERS = 20;
  const int64_t RESERVATION_LIMIT = RESERVATION_LIMIT_NUM_BUFFERS * MAX_BUFFER_SIZE;
  InitRootReservation(RESERVATION_LIMIT);

  thread_group threads;
  // Allocate enough ranges so that the total buffers exceeds the limit.
  const int num_ranges = 25;
  {
    DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

    ASSERT_OK(io_mgr.Init());
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(RESERVATION_LIMIT, RESERVATION_LIMIT, &read_client);
    unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

    vector<ScanRange*> ranges;
    for (int i = 0; i < num_ranges; ++i) {
      ranges.push_back(InitRange(&pool_, tmp_file, 0, DATA_BYTES, 0, stat_val.st_mtime));
    }
    ASSERT_OK(reader->AddScanRanges(ranges, EnqueueLocation::TAIL));
    // Keep starting new ranges without returning buffers until we run out of
    // reservation.
    while (read_client.GetUnusedReservation() >= MIN_BUFFER_SIZE) {
      ScanRange* range = nullptr;
      bool needs_buffers;
      ASSERT_OK(reader->GetNextUnstartedRange(&range, &needs_buffers));
      if (range == nullptr) break;
      ASSERT_TRUE(needs_buffers);
      // Pick a random amount of memory to reserve.
      int64_t max_bytes_to_alloc = uniform_int_distribution<int64_t>(MIN_BUFFER_SIZE,
          min<int64_t>(read_client.GetUnusedReservation(), MAX_BUFFER_SIZE * 3))(rng_);
      ASSERT_OK(io_mgr.AllocateBuffersForRange(
          &read_client, range, max_bytes_to_alloc));
      // Start a thread fetching from the range. The thread will either finish the
      // range or be cancelled.
      threads.add_thread(new thread([&data, DATA_BYTES, range] {
        // Don't return buffers to force memory pressure.
        vector<unique_ptr<BufferDescriptor>> buffers;
        int64_t data_offset = 0;
        Status status;
        while (true) {
          unique_ptr<BufferDescriptor> buffer;
          status = range->GetNext(&buffer);
          ASSERT_TRUE(status.ok() || status.IsCancelled()) << status.GetDetail();
          if (status.IsCancelled() || buffer == nullptr) break;
          EXPECT_EQ(0, memcmp(data + data_offset, buffer->buffer(), buffer->len()));
          data_offset += buffer->len();
          buffers.emplace_back(move(buffer));
        }
        if (status.ok()) {
          ASSERT_EQ(DATA_BYTES, data_offset);
        }
        for (auto& buffer : buffers) range->ReturnBuffer(move(buffer));
      }));
      // Let the thread start running before starting the next.
      SleepForMs(10);
    }
    // Let the threads run for a bit then cancel everything.
    SleepForMs(500);
    reader->Cancel();
    // Wait until the threads have returned their buffers before unregistering.
    threads.join_all();
    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
}

void DiskIoMgrTest::CachedReadsTestBody(const char* data, const char* expected,
    bool fake_cache, vector<ScanRange::SubRange> sub_ranges) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  uint8_t* cached_data = reinterpret_cast<uint8_t*>(const_cast<char*>(data));
  int len = strlen(data);
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int num_disks = 2;
  {
    DiskIoMgr io_mgr(num_disks, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

    ASSERT_OK(io_mgr.Init());
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
    unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

    ScanRange* complete_range =
        InitRange(&pool_, tmp_file, 0, strlen(data), 0, stat_val.st_mtime, nullptr, true,
            sub_ranges);
    if (fake_cache) {
      SetReaderStub(complete_range, make_unique<CacheReaderTestStub>(
          complete_range, cached_data, len));
    }

    // Issue some reads before the async ones are issued
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, expected);
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, expected);

    vector<ScanRange*> ranges;
    for (int i = 0; i < len; ++i) {
      int disk_id = i % num_disks;
      ScanRange* range = InitRange(&pool_, tmp_file, 0, len, disk_id, stat_val.st_mtime,
          nullptr, true, sub_ranges);
      ranges.push_back(range);
      if (fake_cache) {
        SetReaderStub(range, make_unique<CacheReaderTestStub>(range, cached_data, len));
      }
    }
    ASSERT_OK(reader->AddScanRanges(ranges, EnqueueLocation::TAIL));

    AtomicInt32 num_ranges_processed;
    thread_group threads;
    for (int i = 0; i < 5; ++i) {
      threads.add_thread(new thread(ScanRangeThread, &io_mgr, reader.get(), &read_client,
          expected, strlen(expected), Status::OK(), 0, &num_ranges_processed));
    }

    // Issue some more sync ranges
    for (int i = 0; i < 5; ++i) {
      sched_yield();
      ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, expected);
    }

    threads.join_all();

    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, expected);
    ValidateSyncRead(&io_mgr, reader.get(), &read_client, complete_range, expected);

    EXPECT_EQ(num_ranges_processed.Load(), ranges.size());
    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test when some scan ranges are marked as being cached.
TEST_F(DiskIoMgrTest, CachedReads) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* data = "abcdefghijklm";
  // Don't fake the cache, i.e. test the fallback mechanism
  CachedReadsTestBody(data, data, false);
  // Fake the test with a file reader stub.
  CachedReadsTestBody(data, data, true);
}

// Test when some scan ranges are marked as being cached and there
// are sub-ranges as well.
TEST_F(DiskIoMgrTest, CachedReadsSubRanges) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* data = "abcdefghijklm";
  int64_t data_len = strlen(data);

  // first iteration tests the fallback mechanism with sub-ranges
  // second iteration fakes a cache
  for (bool fake_cache : {false, true}) {
    CachedReadsTestBody(data, data, fake_cache, {{0, data_len}});
    CachedReadsTestBody(data, "bc", fake_cache, {{1, 2}});
    CachedReadsTestBody(data, "abchilm", fake_cache, {{0, 3}, {7, 2}, {11, 2}});
  }
}

TEST_F(DiskIoMgrTest, MultipleReaderWriter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const int ITERATIONS = 1;
  const char* data = "abcdefghijklmnopqrstuvwxyz";
  const int num_contexts = 5;
  const int file_size = 4 * 1024;
  const int num_writes_queued = 5;
  const int num_reads_queued = 5;

  string file_name = "/tmp/disk_io_mgr_test.txt";
  int success = CreateTempFile(file_name.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << file_name.c_str() << " of size " <<
        file_size;
    ASSERT_TRUE(false);
  }

  // Get mtime for file
  struct stat stat_val;
  stat(file_name.c_str(), &stat_val);

  int64_t iters = 0;
  vector<unique_ptr<RequestContext>> contexts(num_contexts);
  unique_ptr<BufferPool::ClientHandle[]> clients(
      new BufferPool::ClientHandle[num_contexts]);
  Status status;
  TmpFileGroup* tmp_file_grp = NewFileGroup();
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        // Pool for temporary objects from this iteration only.
        ObjectPool tmp_pool;
        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        ASSERT_OK(io_mgr.Init());
        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          RegisterBufferPoolClient(
              LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &clients[file_index]);
          contexts[file_index] = io_mgr.RegisterContext();
        }
        int read_offset = 0;
        int write_offset = 0;
        while (read_offset < file_size) {
          for (int context_index = 0; context_index < num_contexts; ++context_index) {
            if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
            AtomicInt32 num_ranges_processed;
            thread_group threads;
            vector<ScanRange*> ranges;
            int num_scan_ranges = min<int>(num_reads_queued, write_offset - read_offset);
            for (int i = 0; i < num_scan_ranges; ++i) {
              ranges.push_back(InitRange(
                  &tmp_pool, file_name.c_str(), read_offset, 1, i % num_disks, stat_val.st_mtime));
              threads.add_thread(new thread(ScanRangeThread, &io_mgr,
                  contexts[context_index].get(), &clients[context_index],
                  reinterpret_cast<const char*>(data + (read_offset % strlen(data))),
                  1, Status::OK(), num_scan_ranges, &num_ranges_processed));
              ++read_offset;
            }

            num_ranges_written_ = 0;
            int num_write_ranges = min<int>(num_writes_queued, file_size - write_offset);
            for (int i = 0; i < num_write_ranges; ++i) {
              WriteRange::WriteDoneCallback callback =
                  bind(mem_fn(&DiskIoMgrTest::WriteCompleteCallback),
                      this, num_write_ranges, _1);
              WriteRange* new_range = tmp_pool.Add(new WriteRange(
                  file_name, write_offset, i % num_disks, callback));
              new_range->SetData(
                  reinterpret_cast<const uint8_t*>(data + (write_offset % strlen(data))),
                  1);
              TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
              *new_tmp_file_obj =
                  tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, file_name));
              new_range->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
              status = contexts[context_index]->AddWriteRange(new_range);
              ++write_offset;
            }

            {
              unique_lock<mutex> lock(written_mutex_);
              while (num_ranges_written_ < num_write_ranges) writes_done_.Wait(lock);
            }

            threads.join_all();
          } // for (int context_index
        } // while (read_offset < file_size)
        for (int file_index = 0; file_index < num_contexts; ++file_index) {
          io_mgr.UnregisterContext(contexts[file_index].get());
          buffer_pool()->DeregisterClient(&clients[file_index]);
        }
      } // for (int num_disks
    } // for (int threads_per_disk
  } // for (int iteration
  tmp_file_grp->Close();
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const int NUM_READERS = 5;
  const int DATA_LEN = 50;
  const int ITERATIONS = 25;
  const int NUM_THREADS_PER_READER = 3;

  vector<string> file_names(NUM_READERS);
  vector<int64_t> mtimes(NUM_READERS);
  vector<string> data(NUM_READERS);
  unique_ptr<BufferPool::ClientHandle[]> clients(
      new BufferPool::ClientHandle[NUM_READERS]);
  vector<unique_ptr<RequestContext>> readers(NUM_READERS);
  vector<unique_ptr<char[]>> results(NUM_READERS);

  // Initialize data for each reader.  The data will be
  // 'abcd...' for reader one, 'bcde...' for reader two (wrapping around at 'z')
  for (int i = 0; i < NUM_READERS; ++i) {
    char buf[DATA_LEN];
    for (int j = 0; j < DATA_LEN; ++j) {
      int c = (j + i) % 26;
      buf[j] = 'a' + c;
    }
    data[i] = string(buf, DATA_LEN);

    stringstream ss;
    ss << "/tmp/disk_io_mgr_test" << i << ".txt";
    file_names[i] = ss.str();
    CreateTempFile(ss.str().c_str(), data[i].c_str());

    // Get mtime for file
    struct stat stat_val;
    stat(file_names[i].c_str(), &stat_val);
    mtimes[i] = stat_val.st_mtime;

    results[i].reset(new char[DATA_LEN + 1]);
    memset(results[i].get(), 0, DATA_LEN + 1);
  }

  // This exercises concurrency, run the test multiple times
  int64_t iters = 0;
  for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
    for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
      for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
        // Pool for temporary objects from this iteration only.
        ObjectPool tmp_pool;
        LOG(INFO) << "Starting test with num_threads_per_disk=" << threads_per_disk
                  << " num_disk=" << num_disks;
        if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;

        DiskIoMgr io_mgr(num_disks, threads_per_disk, threads_per_disk, MIN_BUFFER_SIZE,
            MAX_BUFFER_SIZE);
        ASSERT_OK(io_mgr.Init());

        for (int i = 0; i < NUM_READERS; ++i) {
          RegisterBufferPoolClient(
              LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &clients[i]);
          readers[i] = io_mgr.RegisterContext();

          vector<ScanRange*> ranges;
          for (int j = 0; j < DATA_LEN; ++j) {
            int disk_id = j % num_disks;
            ranges.push_back(InitRange(&tmp_pool, file_names[i].c_str(), j, 1, disk_id, mtimes[i]));
          }
          ASSERT_OK(readers[i]->AddScanRanges(ranges, EnqueueLocation::TAIL));
        }

        AtomicInt32 num_ranges_processed;
        thread_group threads;
        for (int i = 0; i < NUM_READERS; ++i) {
          for (int j = 0; j < NUM_THREADS_PER_READER; ++j) {
            threads.add_thread(new thread(ScanRangeThread, &io_mgr, readers[i].get(),
                &clients[i], data[i].c_str(), data[i].size(), Status::OK(), 0,
                &num_ranges_processed));
          }
        }
        threads.join_all();
        EXPECT_EQ(num_ranges_processed.Load(), DATA_LEN * NUM_READERS);
        for (int i = 0; i < NUM_READERS; ++i) {
          io_mgr.UnregisterContext(readers[i].get());
          buffer_pool()->DeregisterClient(&clients[i]);
        }
      }
    }
  }
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Stress test for multiple clients with cancellation
// TODO: the stress app should be expanded to include sync reads and adding scan
// ranges in the middle.
TEST_F(DiskIoMgrTest, StressTest) {
  // Run the test with 5 disks, 5 threads per disk, 10 clients and with cancellation
  DiskIoMgrStress test(5, 5, 10, true);
  test.Run(2); // In seconds
}

// IMPALA-2366: handle partial read where range goes past end of file.
TEST_F(DiskIoMgrTest, PartialRead) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = len + 1000; // Read past end of file.
  // Test with various buffer sizes to exercise different code paths, e.g.
  // * the truncated data ends exactly on a buffer boundary
  // * the data is split between many buffers
  // * the data fits in one buffer
  const int64_t MIN_BUFFER_SIZE = 2;
  vector<int64_t> max_buffer_sizes{4, 16, 32, 128, 1024, 4096};
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  for (int64_t max_buffer_size : max_buffer_sizes) {
    DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, max_buffer_size);
    ASSERT_OK(io_mgr.Init());
    unique_ptr<RequestContext> reader;
    reader = io_mgr.RegisterContext();

    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);

    // We should not read past the end of file.
    ScanRange* range = InitRange(&pool_, tmp_file, 0, read_len, 0, stat_val.st_mtime);
    unique_ptr<BufferDescriptor> buffer;
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    if (needs_buffers) {
      ASSERT_OK(io_mgr.AllocateBuffersForRange(
          &read_client, range, 3 * max_buffer_size));
    }

    int64_t bytes_read = 0;
    bool eosr = false;
    do {
      ASSERT_OK(range->GetNext(&buffer));
      ASSERT_GE(buffer->buffer_len(), MIN_BUFFER_SIZE);
      ASSERT_LE(buffer->buffer_len(), max_buffer_size);
      ASSERT_LE(buffer->len(), len - bytes_read);
      ASSERT_TRUE(memcmp(buffer->buffer(), data + bytes_read, buffer->len()) == 0);
      bytes_read += buffer->len();
      eosr = buffer->eosr();
      // Should see eosr if we've read past the end of the file. If the data is an exact
      // multiple of the max buffer size then we may read to the end of the file without
      // noticing that it is eosr. Eosr will be returned on the next read in that case.
      ASSERT_TRUE(bytes_read < len || buffer->eosr()
          || (buffer->len() == max_buffer_size && len % max_buffer_size == 0))
          << "max_buffer_size " << max_buffer_size << " bytes_read " << bytes_read
          << "len " << len << " buffer->len() " << buffer->len()
          << " buffer->buffer_len() " << buffer->buffer_len();
      ASSERT_TRUE(buffer->len() > 0 || buffer->eosr());
      range->ReturnBuffer(move(buffer));
    } while (!eosr);

    io_mgr.UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }
}

// Test zero-length scan range.
TEST_F(DiskIoMgrTest, ZeroLengthScanRange) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

  ScanRange* range = InitRange(&pool_, tmp_file, 0, 0, 0, stat_val.st_mtime);
  bool needs_buffers;
  Status status = reader->StartScanRange(range, &needs_buffers);
  ASSERT_EQ(TErrorCode::DISK_IO_ERROR, status.code());

  status = reader->AddScanRanges(vector<ScanRange*>({range}), EnqueueLocation::TAIL);
  ASSERT_EQ(TErrorCode::DISK_IO_ERROR, status.code());

  io_mgr.UnregisterContext(reader.get());
}

// Test what happens if don't call AllocateBuffersForRange() after trying to start a
// range.
TEST_F(DiskIoMgrTest, SkipAllocateBuffers) {
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

  ASSERT_OK(io_mgr.Init());
  MemTracker reader_mem_tracker;
  unique_ptr<RequestContext> reader = io_mgr.RegisterContext();

  // We should not read past the end of file.
  vector<ScanRange*> ranges;
  for (int i = 0; i < 4; ++i) {
    ranges.push_back(InitRange(&pool_, tmp_file, 0, len, 0, stat_val.st_mtime));
  }
  bool needs_buffers;
  // Test StartScanRange().
  ASSERT_OK(reader->StartScanRange(ranges[0], &needs_buffers));
  EXPECT_TRUE(needs_buffers);
  ASSERT_OK(reader->StartScanRange(ranges[1], &needs_buffers));
  EXPECT_TRUE(needs_buffers);

  // Test AddScanRanges()/GetNextUnstartedRange().
  ASSERT_OK(reader->AddScanRanges(
      vector<ScanRange*>({ranges[2], ranges[3]}), EnqueueLocation::TAIL));

  // Cancel two directly, cancel the other two indirectly via the context.
  ranges[0]->Cancel(Status::CancelledInternal("foo"));
  ranges[2]->Cancel(Status::CancelledInternal("bar"));
  reader->Cancel();

  io_mgr.UnregisterContext(reader.get());
}

// Regression test for IMPALA-6587 - all buffers should be released after Cancel().
TEST_F(DiskIoMgrTest, CancelReleasesResources) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int NUM_DISK_THREADS = 20;
  DiskIoMgr io_mgr(
      1, NUM_DISK_THREADS, NUM_DISK_THREADS, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
#ifndef NDEBUG
  auto s = ScopedFlagSetter<int32_t>::Make(&FLAGS_stress_disk_read_delay_ms, 5);
#endif

  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> reader = io_mgr.RegisterContext();
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);

  for (int i = 0; i < 10; ++i) {
    ScanRange* range = InitRange(&pool_, tmp_file, 0, len, 0, stat_val.st_mtime);
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    EXPECT_TRUE(needs_buffers);
    ASSERT_OK(io_mgr.AllocateBuffersForRange(&read_client, range, MAX_BUFFER_SIZE));
    // Give disk I/O thread a chance to start read.
    SleepForMs(1);

    range->Cancel(Status::CancelledInternal("bar"));
    // Resources should be released immediately once Cancel() returns.
    EXPECT_EQ(0, read_client.GetUsedReservation()) << " iter " << i;
  }
  buffer_pool()->DeregisterClient(&read_client);
  io_mgr.UnregisterContext(reader.get());
}

// Regression test for IMPALA-7402 - RequestContext::Cancel() propagation via
// ScanRange::GetNext() does not guarantee buffers are released.
TEST_F(DiskIoMgrTest, FinalGetNextReleasesResources) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  const int64_t MIN_BUFFER_SIZE = 2;
  const int64_t MAX_BUFFER_SIZE = 1024;
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  const int NUM_DISK_THREADS = 20;
  DiskIoMgr io_mgr(
      1, NUM_DISK_THREADS, NUM_DISK_THREADS, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
#ifndef NDEBUG
  auto s = ScopedFlagSetter<int32_t>::Make(&FLAGS_stress_disk_read_delay_ms, 5);
#endif

  ASSERT_OK(io_mgr.Init());
  BufferPool::ClientHandle read_client;
  RegisterBufferPoolClient(
      LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);

  for (int i = 0; i < 10; ++i) {
    unique_ptr<RequestContext> reader = io_mgr.RegisterContext();
    ScanRange* range = InitRange(&pool_, tmp_file, 0, len, 0, stat_val.st_mtime);
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    EXPECT_TRUE(needs_buffers);
    ASSERT_OK(io_mgr.AllocateBuffersForRange(&read_client, range, MAX_BUFFER_SIZE));
    // Give disk I/O thread a chance to start read.
    SleepForMs(1);

    reader->Cancel();
    // The scan range should hold no resources once ScanRange::GetNext() returns.
    unique_ptr<BufferDescriptor> buffer;
    Status status = range->GetNext(&buffer);
    if (status.ok()) {
      DCHECK(buffer->eosr());
      range->ReturnBuffer(move(buffer));
    }
    EXPECT_EQ(0, read_client.GetUsedReservation()) << " iter " << i << ": "
                                                   << status.GetDetail();
    io_mgr.UnregisterContext(reader.get());
  }
  buffer_pool()->DeregisterClient(&read_client);
}

// Test reading into a client-allocated buffer.
TEST_F(DiskIoMgrTest, ReadIntoClientBuffer) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  int len = strlen(data);
  int read_len = 4; // Make buffer size smaller than client-provided buffer.
  CreateTempFile(tmp_file, data);

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, read_len, read_len));

  ASSERT_OK(io_mgr->Init());
  // Reader doesn't need to provide client if it's providing buffers.
  unique_ptr<RequestContext> reader = io_mgr->RegisterContext();

  for (int buffer_len : vector<int>({len - 1, len, len + 1})) {
    vector<uint8_t> client_buffer(buffer_len);
    int scan_len = min(len, buffer_len);
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(nullptr, tmp_file, scan_len, 0, 0, true, ScanRange::INVALID_MTIME,
        BufferOpts::ReadInto(client_buffer.data(), buffer_len, BufferOpts::NO_CACHING));
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    ASSERT_FALSE(needs_buffers);

    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_OK(range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    ASSERT_EQ(scan_len, io_buffer->len());
    ASSERT_EQ(client_buffer.data(), io_buffer->buffer());
    ASSERT_EQ(memcmp(io_buffer->buffer(), data, scan_len), 0);

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
    range->ReturnBuffer(move(io_buffer));
  }

  io_mgr->UnregisterContext(reader.get());
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test reading into a client-allocated buffer using sub-ranges.
TEST_F(DiskIoMgrTest, ReadIntoClientBufferSubRanges) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
  const char* data = "the quick brown fox jumped over the lazy dog";
  uint8_t* cache = reinterpret_cast<uint8_t*>(const_cast<char*>(data));
  int data_len = strlen(data);
  int read_len = 4; // Make buffer size smaller than client-provided buffer.
  CreateTempFile(tmp_file, data);

  // Get mtime for file
  struct stat stat_val;
  stat(tmp_file, &stat_val);

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, read_len, read_len));
  ASSERT_OK(io_mgr->Init());
  // Reader doesn't need to provide client if it's providing buffers.
  unique_ptr<RequestContext> reader = io_mgr->RegisterContext();

  auto test_case = [&](bool fake_cache, const char* expected_result,
      vector<ScanRange::SubRange> sub_ranges) {
    int result_len = strlen(expected_result);
    vector<uint8_t> client_buffer(result_len);
    ScanRange* range = pool_.Add(new ScanRange);
    int cache_options = fake_cache ? BufferOpts::USE_HDFS_CACHE : BufferOpts::NO_CACHING;
    range->Reset(nullptr, tmp_file, data_len, 0, 0, true, stat_val.st_mtime,
        BufferOpts::ReadInto(cache_options, client_buffer.data(), result_len),
        move(sub_ranges));
    if (fake_cache) {
      SetReaderStub(range, make_unique<CacheReaderTestStub>(range, cache, data_len));
    }
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    ASSERT_FALSE(needs_buffers);

    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_OK(range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    ASSERT_EQ(result_len, io_buffer->len());
    ASSERT_EQ(client_buffer.data(), io_buffer->buffer());
    ASSERT_EQ(memcmp(io_buffer->buffer(), expected_result, result_len), 0);

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
    range->ReturnBuffer(move(io_buffer));
  };

  for (bool fake_cache : {false, true}) {
    test_case(fake_cache, data, {{0, data_len}});
    test_case(fake_cache, data, {{0, 15}, {15, data_len - 15}});
    test_case(fake_cache, "quick fox", {{4, 5}, {15, 4}});
    test_case(fake_cache, "the brown dog", {{0, 3}, {9, 6}, {data_len - 4, 4}});
  }

  io_mgr->UnregisterContext(reader.get());
  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test reading into a client-allocated buffer where the read fails.
TEST_F(DiskIoMgrTest, ReadIntoClientBufferError) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const char* tmp_file = "/file/that/does/not/exist";
  const int SCAN_LEN = 128;

  scoped_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, 1, SCAN_LEN, SCAN_LEN));

  ASSERT_OK(io_mgr->Init());
  vector<uint8_t> client_buffer(SCAN_LEN);
  for (int i = 0; i < 1000; ++i) {
    // Reader doesn't need to provide mem tracker if it's providing buffers.
    BufferPool::ClientHandle read_client;
    RegisterBufferPoolClient(
        LARGE_RESERVATION_LIMIT, LARGE_INITIAL_RESERVATION, &read_client);
    unique_ptr<RequestContext> reader = io_mgr->RegisterContext();
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(nullptr, tmp_file, SCAN_LEN, 0, 0, true, ScanRange::INVALID_MTIME,
        BufferOpts::ReadInto(client_buffer.data(), SCAN_LEN, BufferOpts::NO_CACHING));
    bool needs_buffers;
    ASSERT_OK(reader->StartScanRange(range, &needs_buffers));
    ASSERT_FALSE(needs_buffers);

    /// Also test the cancellation path. Run multiple iterations since it is racy whether
    /// the read fails before the cancellation.
    if (i >= 1) reader->Cancel();

    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_FALSE(range->GetNext(&io_buffer).ok());

    // DiskIoMgr should not have allocated memory.
    EXPECT_EQ(read_client.GetUsedReservation(), 0);

    io_mgr->UnregisterContext(reader.get());
    EXPECT_EQ(read_client.GetUsedReservation(), 0);
    buffer_pool()->DeregisterClient(&read_client);
  }

  EXPECT_EQ(root_reservation_.GetChildReservations(), 0);
}

// Test to verify configuration parameters for number of I/O threads per disk.
TEST_F(DiskIoMgrTest, VerifyNumThreadsParameter) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  const int num_io_threads_for_remote_disks = FLAGS_num_remote_hdfs_io_threads
      + FLAGS_num_s3_io_threads + FLAGS_num_adls_io_threads + FLAGS_num_abfs_io_threads
      + FLAGS_num_ozone_io_threads + FLAGS_num_oss_io_threads
      + FLAGS_num_remote_hdfs_file_oper_io_threads
      + FLAGS_num_s3_file_oper_io_threads + FLAGS_num_gcs_io_threads
      + FLAGS_num_cos_io_threads
      + FLAGS_num_sfs_io_threads;

  // Verify num_io_threads_per_rotational_disk and num_io_threads_per_solid_state_disk.
  // Since we do not have control over which disk is used, we check for either type
  // (rotational/solid state)
  const int num_io_threads_per_rotational_or_ssd = 2;
  DiskIoMgr io_mgr(1, num_io_threads_per_rotational_or_ssd,
      num_io_threads_per_rotational_or_ssd, 1, 10);
  ASSERT_OK(io_mgr.Init());
  const int num_io_threads = io_mgr.disk_thread_group_.Size();
  ASSERT_TRUE(num_io_threads ==
      num_io_threads_per_rotational_or_ssd + num_io_threads_for_remote_disks);
}

// Test to verify that the correct buffer sizes are chosen given different
// of scan range lengths and max_bytes values.
TEST_F(DiskIoMgrTest, BufferSizeSelection) {
  DiskIoMgr io_mgr(1, 1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
  ASSERT_OK(io_mgr.Init());

  // Scan range doesn't fit in max_bytes - allocate as many max-sized buffers as possible.
  EXPECT_EQ(vector<int64_t>(3, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>(4, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(10 * MAX_BUFFER_SIZE, 4 * MAX_BUFFER_SIZE));

  // Scan range fits in max_bytes - allocate as many max-sized buffers as possible, then
  // a smaller buffer to fit the remainder.
  EXPECT_EQ(vector<int64_t>(2, MAX_BUFFER_SIZE),
      io_mgr.ChooseBufferSizes(2 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(2 * MAX_BUFFER_SIZE + 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, 2 * MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(
        2 * MAX_BUFFER_SIZE + MIN_BUFFER_SIZE + 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MAX_BUFFER_SIZE, 2 * MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(
        2 * MAX_BUFFER_SIZE + 2 * MIN_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));

  // Scan range is smaller than max buffer size - allocate a single buffer that fits
  // the range.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE - 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE - 1, MAX_BUFFER_SIZE / 2));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2 - 1, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 2}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2- 1, MAX_BUFFER_SIZE / 2));
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE));
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE, MIN_BUFFER_SIZE));

  // Scan range is smaller than max buffer size and max bytes is smaller still -
  // should allocate a single smaller buffer.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE / 4}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE / 2, MAX_BUFFER_SIZE / 2 - 1));

  // Non power-of-two size > max buffer size.
  EXPECT_EQ(vector<int64_t>({MAX_BUFFER_SIZE, MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MAX_BUFFER_SIZE + 7, 3 * MAX_BUFFER_SIZE));
  // Non power-of-two size < min buffer size.
  EXPECT_EQ(vector<int64_t>({MIN_BUFFER_SIZE}),
      io_mgr.ChooseBufferSizes(MIN_BUFFER_SIZE - 7, 3 * MAX_BUFFER_SIZE));
}

// Issue a number of writes then read if the metrics record
// the write operations.
TEST_F(DiskIoMgrTest, MetricsOfWriteSizeAndLatency) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/tmp/disk_io_mgr_test.txt";
  int num_ranges = 100;
  int64_t file_size = 1024 * 1024;
  int64_t cur_offset = 0;
  int num_disks = 5;
  int success = CreateTempFile(tmp_file.c_str(), file_size);
  if (success != 0) {
    LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size "
               << file_size;
    EXPECT_TRUE(false);
  }

  // Reset the Metric if it exists.
  for (int i = 0; i < num_disks; i++) {
    string key_prefix = "impala-server.io-mgr.queue-";
    string write_size_postfix = ".write-size";
    string write_latency_postfix = ".write-latency";
    string i_str = std::to_string(i);
    auto write_size_org =
        ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
            key_prefix + i_str + write_size_postfix);
    auto write_latency_org =
        ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
            key_prefix + i_str + write_latency_postfix);
    if (write_size_org != nullptr) write_size_org->Reset();
    if (write_latency_org != nullptr) write_latency_org->Reset();
  }

  WriteRange::WriteDoneCallback callback = [=](const Status& status) {
    lock_guard<mutex> l(written_mutex_);
    ++num_ranges_written_;
    if (num_ranges_written_ == num_ranges) writes_done_.NotifyOne();
  };

  // Issue a number of writes to the disks.
  ObjectPool tmp_pool;
  TmpFileGroup* tmp_file_grp = NewFileGroup();
  DiskIoMgr io_mgr(num_disks, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
  for (int i = 0; i < num_ranges; ++i) {
    int32_t* data = tmp_pool.Add(new int32_t);
    *data = rand();
    WriteRange** new_range = tmp_pool.Add(new WriteRange*);
    *new_range =
        tmp_pool.Add(new WriteRange(tmp_file, cur_offset, i % num_disks, callback));
    (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
    *new_tmp_file_obj = tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
    (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
    cur_offset += sizeof(int32_t);
    ASSERT_OK(writer->AddWriteRange(*new_range));
  }
  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < num_ranges) writes_done_.Wait(lock);
  }

  // Check the count and max/min of the histogram metric.
  size_t write_size_len = sizeof(int32_t);
  auto exam_fuc = [&](HistogramMetric* metric, const string& keyname) {
    uint64_t total_cnt = metric->TotalCount();
    uint64_t min_value = metric->MinValue();
    uint64_t max_value = metric->MaxValue();
    // The count should be added by num_ranges/num_disks per disk.
    EXPECT_EQ(total_cnt, num_ranges / num_disks);
    // Check if the min and max of write size are the same as the written len.
    if (keyname == "write_size") {
      EXPECT_EQ(min_value, write_size_len);
      EXPECT_EQ(max_value, write_size_len);
    }
  };

  for (int i = 0; i < num_disks; i++) {
    auto write_size = io_mgr.disk_queues_[i]->write_size();
    auto write_latency = io_mgr.disk_queues_[i]->write_latency();
    exam_fuc(write_size, "write_size");
    exam_fuc(write_latency, "write_latency");
  }

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(writer.get());
}

// Issue a writing operation to a non-existent tmp file path.
// Test if the write IO errors can be recorded.
TEST_F(DiskIoMgrTest, MetricsOfWriteIoError) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string tmp_file = "/non-existent/file.txt";

  // Reset the Metric if it exists.
  auto write_io_err = ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<IntCounter>(
      "impala-server.io-mgr.queue-0.write-io-error");
  if (write_io_err != nullptr) write_io_err->SetValue(0);
  TmpFileGroup* tmp_file_grp = NewFileGroup();

  vector<string> tmp_path;
  tmp_path.push_back(tmp_file);
  // Remove the path in case it exists.
  Status rm_status = FileSystemUtil::RemovePaths(tmp_path);
  ObjectPool tmp_pool;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  unique_ptr<RequestContext> writer = io_mgr.RegisterContext();
  int32_t* data = tmp_pool.Add(new int32_t);
  *data = rand();
  WriteRange** new_range = tmp_pool.Add(new WriteRange*);
  WriteRange::WriteDoneCallback callback = [=](const Status& status) {
    ASSERT_EQ(TErrorCode::DISK_IO_ERROR, status.code());
    lock_guard<mutex> l(written_mutex_);
    num_ranges_written_ = 1;
    writes_done_.NotifyOne();
  };
  *new_range = tmp_pool.Add(new WriteRange(tmp_file, 0, 0, callback));
  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  TmpFile** new_tmp_file_obj = tmp_pool.Add(new TmpFile*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileLocal(tmp_file_grp, 0, tmp_file));
  (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
  EXPECT_OK(writer->AddWriteRange(*new_range));
  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < 1) writes_done_.Wait(lock);
  }
  // One IO Error should be added to the metrics counter.
  EXPECT_EQ(write_io_err->GetValue(), 1);

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(writer.get());
}

// Issue writing operations to a remote directory.
// Test if the temporary file can be uploaded and read correctly.
TEST_F(DiskIoMgrTest, WriteToRemoteSuccess) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string remote_file_path = REMOTE_URL + "/test";
  string new_file_path_local_buffer = LOCAL_BUFFER_PATH + "/test";
  int32_t file_size = 1024;
  FLAGS_remote_tmp_file_size = "1K";

  // Delete the file if it exists.
  hdfsDelete(hdfsConnect("default", 0), remote_file_path.c_str(), 1);

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  string key_prefix = "impala-server.io-mgr.queue-";
  string write_size_postfix = ".write-size";
  string write_latency_postfix = ".write-latency";
  string i_str = std::to_string(io_mgr.RemoteDfsDiskFileOperId());

  auto write_size_remote =
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          key_prefix + i_str + write_size_postfix);
  auto write_latency_remote =
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          key_prefix + i_str + write_latency_postfix);
  if (write_size_remote != nullptr) write_size_remote->Reset();
  if (write_latency_remote != nullptr) write_latency_remote->Reset();
  size_t write_size_len = sizeof(int32_t);

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote** new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote(tmp_file_grp, 0, remote_file_path,
      new_file_path_local_buffer, false, REMOTE_URL.c_str()));

  vector<WriteRange*> ranges;
  vector<int32_t> datas;

  (*new_tmp_file_obj)->GetWriteFile()->SetActualFileSize(file_size);

  for (int i = 0; i < file_size / write_size_len; i++) {
    int32_t* data = tmp_pool.Add(new int32_t);
    *data = rand();
    datas.push_back(*data);
    WriteRange** new_range = tmp_pool.Add(new WriteRange*);
    WriteRange::WriteDoneCallback callback = [=](const Status& status) {
      ASSERT_EQ(0, status.code());
      lock_guard<mutex> l(written_mutex_);
      num_ranges_written_ = 1;
      writes_done_.NotifyOne();
    };

    *new_range = tmp_pool.Add(new WriteRange(remote_file_path, 0, 0, callback));
    (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
    ranges.push_back(*new_range);
    EXPECT_OK(io_ctx->AddWriteRange(*new_range));
    {
      unique_lock<mutex> lock(written_mutex_);
      while (num_ranges_written_ < 1) writes_done_.Wait(lock);
    }
    num_ranges_written_ = 0;
    if (i == file_size / write_size_len - 1) {
      (*new_tmp_file_obj)->SetAtCapacity();
    }
  }

  EXPECT_TRUE(FileExist(new_file_path_local_buffer));

  // Do upload the file.
  auto disk_id = io_mgr.RemoteDfsDiskFileOperId();
  RemoteOperRange::RemoteOperDoneCallback u_callback = [](const Status& upload_status) {};
  RemoteOperRange* upload_range = tmp_pool.Add(new RemoteOperRange(
      (*new_tmp_file_obj)->DiskBufferFile(), (*new_tmp_file_obj)->DiskFile(), file_size,
      disk_id, RequestType::FILE_UPLOAD, &io_mgr, u_callback));
  EXPECT_OK(io_ctx->AddRemoteOperRange(upload_range));

  int wait_times = 10;
  while (true) {
    if ((*new_tmp_file_obj)->DiskFile()->GetFileStatus() == DiskFileStatus::PERSISTED) {
      break;
    }
    // Suppose the upload should be finished in two seconds.
    ASSERT_TRUE(wait_times-- > 0);
    usleep(200 * 1000);
  }

  EXPECT_TRUE(HdfsFileExist(remote_file_path));

  auto exam_fuc = [&](HistogramMetric* metric, const string& keyname) {
    uint64_t total_cnt = metric->TotalCount();
    uint64_t min_value = metric->MinValue();
    uint64_t max_value = metric->MaxValue();
    if (keyname == "write_latency") {
      // Write latency counts not only file writing, but also file close.
      // Since it is supposed to be slow to close remote files, due to the
      // close is doing at least a part of upload, so we expect the total
      // count is 2 here.
      EXPECT_EQ(total_cnt, 2);
    } else {
      // The block size is the same as the file size, so writing is finished
      // by one time file writing.
      EXPECT_EQ(total_cnt, 1);
    }
    // Check if the min and max of write size are the same as the written len.
    if (keyname == "write_size") {
      EXPECT_EQ(min_value, file_size);
      EXPECT_EQ(max_value, file_size);
    }
  };

  exam_fuc(write_size_remote, "write_size");
  exam_fuc(write_latency_remote, "write_latency");

  // Bogus value
  int64_t mtime = 100000;

  // Test reading from local buffer, since the scratch file is in
  // the local file system.
  for (int i = 0; i < ranges.size(); i++) {
    ScanRange* scan_range = tmp_pool.Add(new ScanRange);
    auto range = ranges.at(i);
    auto data = datas.at(i);
    size_t buffer_len = sizeof(int32_t);
    vector<uint8_t> client_buffer(buffer_len);
    scan_range->Reset(hdfsConnect("default", 0), range->file(), range->len(),
        range->offset(), 0, false, mtime,
        BufferOpts::ReadInto(client_buffer.data(), buffer_len, BufferOpts::NO_CACHING),
        nullptr, (*new_tmp_file_obj)->DiskFile(), (*new_tmp_file_obj)->DiskBufferFile());
    bool needs_buffers;
    ASSERT_OK(io_ctx->StartScanRange(scan_range, &needs_buffers));
    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_OK(scan_range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    EXPECT_EQ(range->len(), io_buffer->len());
    EXPECT_EQ(client_buffer.data(), io_buffer->buffer());
    EXPECT_EQ(*(int32_t*)client_buffer.data(), data);
    EXPECT_TRUE(scan_range->use_local_buffer());
    scan_range->ReturnBuffer(move(io_buffer));
  }

  // Delete the local buffer file, try to read from the remote.
  (*new_tmp_file_obj)->DiskBufferFile()->SetStatus(io::DiskFileStatus::DELETED);
  EXPECT_OK(FileSystemUtil::RemovePaths({(*new_tmp_file_obj)->DiskBufferFile()->path()}));

  for (int i = 0; i < ranges.size(); i++) {
    ScanRange* scan_range = tmp_pool.Add(new ScanRange);
    auto range = ranges.at(i);
    auto data = datas.at(i);
    size_t buffer_len = sizeof(int32_t);
    vector<uint8_t> client_buffer(buffer_len);
    scan_range->Reset(hdfsConnect("default", 0), range->file(), range->len(),
        range->offset(), 0, false, mtime,
        BufferOpts::ReadInto(client_buffer.data(), buffer_len, BufferOpts::NO_CACHING),
        nullptr, (*new_tmp_file_obj)->DiskFile(), (*new_tmp_file_obj)->DiskBufferFile());
    bool needs_buffers;
    ASSERT_OK(io_ctx->StartScanRange(scan_range, &needs_buffers));
    unique_ptr<BufferDescriptor> io_buffer;
    ASSERT_OK(scan_range->GetNext(&io_buffer));
    ASSERT_TRUE(io_buffer->eosr());
    EXPECT_EQ(range->len(), io_buffer->len());
    EXPECT_EQ(client_buffer.data(), io_buffer->buffer());
    EXPECT_EQ(*(int32_t*)client_buffer.data(), data);
    EXPECT_FALSE(scan_range->use_local_buffer());
    scan_range->ReturnBuffer(move(io_buffer));
  }

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}

// Issue a writing operation to a remote directory.
// Test if it can be read correctly when the temporary file is not full and not uploaded.
TEST_F(DiskIoMgrTest, WriteToRemotePartialFileSuccess) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string remote_file_path = REMOTE_URL + "/test";
  string new_file_path_local_buffer = LOCAL_BUFFER_PATH + "/test";
  FLAGS_remote_tmp_file_size = "1K";

  // Delete the file in hdfs if it exists.
  hdfsDelete(hdfsConnect("default", 0), remote_file_path.c_str(), 1);

  // Delete the file in local file system
  vector<string> tmp_path;
  tmp_path.push_back(new_file_path_local_buffer);
  Status rm_status = FileSystemUtil::RemovePaths(tmp_path);

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  string metric_name = "tmp-file-mgr.local-buff-bytes-used.dir-0";

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote** new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote(tmp_file_grp, 0, remote_file_path,
      new_file_path_local_buffer, false, REMOTE_URL.c_str()));

  int32_t* data = tmp_pool.Add(new int32_t);
  *data = rand();
  WriteRange** new_range = tmp_pool.Add(new WriteRange*);
  WriteRange::WriteDoneCallback callback = [=](const Status& status) {
    ASSERT_EQ(0, status.code());
    lock_guard<mutex> l(written_mutex_);
    num_ranges_written_ = 1;
    writes_done_.NotifyOne();
  };

  *new_range = tmp_pool.Add(new WriteRange(remote_file_path, 0, 0, callback));
  (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
  (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
  EXPECT_OK(io_ctx->AddWriteRange(*new_range));
  {
    unique_lock<mutex> lock(written_mutex_);
    while (num_ranges_written_ < 1) writes_done_.Wait(lock);
  }
  num_ranges_written_ = 0;
  (*new_tmp_file_obj)->SetAtCapacity();

  EXPECT_TRUE(FileExist(new_file_path_local_buffer));
  EXPECT_FALSE(HdfsFileExist(remote_file_path));

  // Bogus value
  int64_t mtime = 100000;

  // Test reading from remote scatch space.
  ScanRange* scan_range = tmp_pool.Add(new ScanRange);
  size_t buffer_len = sizeof(int32_t);
  vector<uint8_t> client_buffer(buffer_len);
  scan_range->Reset(hdfsConnect("default", 0), (*new_range)->file(), (*new_range)->len(),
      (*new_range)->offset(), 0, false, mtime,
      BufferOpts::ReadInto(client_buffer.data(), buffer_len, BufferOpts::NO_CACHING),
      nullptr, (*new_tmp_file_obj)->DiskFile(), (*new_tmp_file_obj)->DiskBufferFile());
  bool needs_buffers;
  ASSERT_OK(io_ctx->StartScanRange(scan_range, &needs_buffers));
  unique_ptr<BufferDescriptor> io_buffer;
  ASSERT_OK(scan_range->GetNext(&io_buffer));
  ASSERT_TRUE(io_buffer->eosr());
  EXPECT_EQ((*new_range)->len(), io_buffer->len());
  EXPECT_EQ(client_buffer.data(), io_buffer->buffer());
  EXPECT_EQ(*(int32_t*)client_buffer.data(), *data);
  scan_range->ReturnBuffer(move(io_buffer));

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}

// Upload an non-existent file and failed.
TEST_F(DiskIoMgrTest, WriteToRemoteUploadFailed) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_oper_ = 0;
  string remote_file_path = REMOTE_URL + "/test";
  string non_existent_dir = "/non-existent-dir/test";
  FLAGS_remote_tmp_file_size = "1K";
  int64_t file_size = 1024;
  int64_t block_size = 1024;

  // Delete the file if it exists.
  hdfsDelete(hdfsConnect("default", 0), remote_file_path.c_str(), 1);

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote** new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote(
      tmp_file_grp, 0, remote_file_path, non_existent_dir, false, REMOTE_URL.c_str()));

  DiskFile* remote_file = (*new_tmp_file_obj)->DiskFile();
  DiskFile* local_buffer_file = (*new_tmp_file_obj)->DiskBufferFile();
  local_buffer_file->SetStatus(DiskFileStatus::PERSISTED);
  local_buffer_file->SetActualFileSize(file_size);

  auto disk_id = io_mgr.RemoteDfsDiskFileOperId();
  RemoteOperRange::RemoteOperDoneCallback callback = [=](const Status& status) {
    ASSERT_NE(0, status.code());
    lock_guard<mutex> l(oper_mutex_);
    num_oper_ = 1;
    oper_done_.NotifyOne();
  };

  auto oper_range = tmp_pool.Add(new RemoteOperRange(local_buffer_file, remote_file,
      block_size, disk_id, RequestType::FILE_UPLOAD, &io_mgr, callback));
  Status add_status = io_ctx->AddRemoteOperRange(oper_range);
  ASSERT_OK(add_status);

  {
    unique_lock<mutex> lock(oper_mutex_);
    while (num_oper_ < 1) oper_done_.Wait(lock);
  }

  // None of the files should exist.
  EXPECT_FALSE(FileExist(non_existent_dir));
  EXPECT_FALSE(HdfsFileExist(remote_file_path));

  num_oper_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}

// Upload file to the remote successfully, then evict the local buffer.
TEST_F(DiskIoMgrTest, WriteToRemoteEvictLocal) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string remote_file_path = REMOTE_URL + "/test1";
  string local_buffer_file_path = LOCAL_BUFFER_PATH + "/test1";
  int32_t file_size = 1024;
  FLAGS_remote_tmp_file_size = "1K";

  // Delete the file if it exists.
  hdfsDelete(hdfsConnect("default", 0), remote_file_path.c_str(), 1);

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  size_t write_size_len = sizeof(int32_t);

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote* new_tmp_file_obj = new TmpFileRemote(tmp_file_grp, 0, remote_file_path,
      local_buffer_file_path, false, REMOTE_URL.c_str());
  shared_ptr<TmpFileRemote> shared_tmp_file;
  shared_tmp_file.reset(move(new_tmp_file_obj));

  vector<WriteRange*> ranges;
  vector<int32_t> datas;

  // Set the actual file size which is needed by the upload process.
  shared_tmp_file->GetWriteFile()->SetActualFileSize(file_size);

  for (int i = 0; i < file_size / write_size_len; i++) {
    int32_t* data = tmp_pool.Add(new int32_t);
    *data = rand();
    datas.push_back(*data);
    WriteRange** new_range = tmp_pool.Add(new WriteRange*);
    WriteRange::WriteDoneCallback callback = [=](const Status& status) {
      ASSERT_EQ(0, status.code());
      lock_guard<mutex> l(written_mutex_);
      num_ranges_written_ = 1;
      writes_done_.NotifyOne();
    };

    *new_range = tmp_pool.Add(new WriteRange(remote_file_path, 0, 0, callback));
    (*new_range)->SetData(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    (*new_range)->SetDiskFile(shared_tmp_file->GetWriteFile());
    ranges.push_back(*new_range);
    EXPECT_OK(io_ctx->AddWriteRange(*new_range));
    {
      unique_lock<mutex> lock(written_mutex_);
      while (num_ranges_written_ < 1) writes_done_.Wait(lock);
    }
    num_ranges_written_ = 0;
    if (i == file_size / write_size_len - 1) shared_tmp_file->SetAtCapacity();
  }

  EXPECT_TRUE(FileExist(local_buffer_file_path));

  // Do upload the file.
  RemoteOperRange::RemoteOperDoneCallback u_callback = [&](const Status& upload_status) {
    if (upload_status.ok()) {
      auto shared_tmp_file_ptr = std::dynamic_pointer_cast<TmpFile>(shared_tmp_file);
      tmp_file_mgr.EnqueueTmpFilesPool(shared_tmp_file_ptr, true);
    }
  };
  RemoteOperRange* upload_range = tmp_pool.Add(
      new RemoteOperRange(shared_tmp_file->DiskBufferFile(), shared_tmp_file->DiskFile(),
          file_size, 0, RequestType::FILE_UPLOAD, &io_mgr, u_callback));
  Status add_status = io_ctx->AddRemoteOperRange(upload_range);

  int wait_times = 10;
  while (true) {
    if (shared_tmp_file->DiskFile()->GetFileStatus() == DiskFileStatus::PERSISTED) {
      break;
    }
    // Suppose the upload should be finished in two seconds.
    ASSERT_TRUE(wait_times-- > 0);
    usleep(200 * 1000);
  }

  // TryEvictFile and the local buffer file should be evicted for releasing local
  // scratch space.
  std::shared_ptr<TmpFile> tmp_file;
  ASSERT_OK(tmp_file_mgr.DequeueTmpFilesPool(&tmp_file, false));
  ASSERT_TRUE(tmp_file != nullptr);
  Status try_evict_status = tmp_file_mgr.TryEvictFile(tmp_file.get());
  ASSERT_TRUE(try_evict_status.ok());
  EXPECT_TRUE(HdfsFileExist(remote_file_path));
  EXPECT_FALSE(FileExist(local_buffer_file_path));
  EXPECT_EQ(shared_tmp_file->DiskBufferFile()->GetFileStatus(), DiskFileStatus::DELETED);

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}

// Use an invalid block size to emulate the case when memory allocation failed.
TEST_F(DiskIoMgrTest, WriteToRemoteFailMallocBlock) {
  num_ranges_written_ = 0;
  string remote_file_path = REMOTE_URL + "/test1";
  string local_buffer_file_path = LOCAL_BUFFER_PATH + "/test1";
  int64_t invalid_block_size = -1;

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote** new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote(tmp_file_grp, 0, remote_file_path,
      local_buffer_file_path, false, REMOTE_URL.c_str()));

  RemoteOperRange::RemoteOperDoneCallback u_callback = [this](
                                                           const Status& upload_status) {
    // Assert the upload failed due to a failed malloc with an invalid block size.
    ASSERT_TRUE(!upload_status.ok());
    lock_guard<mutex> l(oper_mutex_);
    num_ranges_written_ = 1;
    oper_done_.NotifyOne();
  };

  auto disk_id = io_mgr.RemoteDfsDiskFileOperId();
  std::unique_ptr<io::RemoteOperRange> upload_range;
  upload_range.reset(new RemoteOperRange(nullptr, nullptr, invalid_block_size, disk_id,
      RequestType::FILE_UPLOAD, &io_mgr, u_callback));
  Status add_status = io_ctx->AddRemoteOperRange(upload_range.get());
  ASSERT_TRUE(add_status.ok());

  {
    unique_lock<mutex> lock(oper_mutex_);
    while (num_ranges_written_ < 1) oper_done_.Wait(lock);
  }

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}

// Issue writing operations with different page sizes, the actual file size is
// over the default file size. Test if the upload is successful.
TEST_F(DiskIoMgrTest, WriteToRemoteDiffPagesSuccess) {
  InitRootReservation(LARGE_RESERVATION_LIMIT);
  num_ranges_written_ = 0;
  string remote_file_path = REMOTE_URL + "/test";
  string new_file_path_local_buffer = LOCAL_BUFFER_PATH + "/test";
  int32_t block_size = 1024;
  FLAGS_remote_tmp_file_size = "1K";
  const int32_t page_size[2] = {768, 512};
  char page_1[page_size[0]];
  char page_2[page_size[1]];
  char* page[2] = {page_1, page_2};
  int32_t actual_size = page_size[0] + page_size[1];

  // Delete the file if it exists.
  hdfsDelete(hdfsConnect("default", 0), remote_file_path.c_str(), 1);

  TmpFileMgr tmp_file_mgr;
  DiskIoMgr io_mgr(1, 1, 1, 1, 10);
  ASSERT_OK(io_mgr.Init());
  TmpFileGroup* tmp_file_grp = NewRemoteFileGroup(&tmp_file_mgr, &io_mgr);
  ASSERT_TRUE(tmp_file_grp != nullptr);

  ObjectPool tmp_pool;
  unique_ptr<RequestContext> io_ctx = io_mgr.RegisterContext();

  TmpFileRemote** new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote*);
  *new_tmp_file_obj = tmp_pool.Add(new TmpFileRemote(tmp_file_grp, 0, remote_file_path,
      new_file_path_local_buffer, false, REMOTE_URL.c_str()));

  WriteRange::WriteDoneCallback callback = [=](const Status& status) {
    ASSERT_EQ(0, status.code());
    lock_guard<mutex> l(written_mutex_);
    num_ranges_written_ = 1;
    writes_done_.NotifyOne();
  };

  // Add pages, and writes them to the local buffer.
  for (int i = 0; i < 2; i++) {
    if (i == 1) {
      (*new_tmp_file_obj)->SetAtCapacity();
      (*new_tmp_file_obj)->GetWriteFile()->SetActualFileSize(actual_size);
    }
    WriteRange** new_range = tmp_pool.Add(new WriteRange*);
    *new_range = tmp_pool.Add(new WriteRange(remote_file_path, 0, 0, callback));
    (*new_range)->SetData(reinterpret_cast<uint8_t*>(page[i]), page_size[i]);
    (*new_range)->SetDiskFile((*new_tmp_file_obj)->GetWriteFile());
    EXPECT_OK(io_ctx->AddWriteRange(*new_range));
    {
      unique_lock<mutex> lock(written_mutex_);
      while (num_ranges_written_ < 1) writes_done_.Wait(lock);
    }
    num_ranges_written_ = 0;
  }

  EXPECT_TRUE(FileExist(new_file_path_local_buffer));
  struct stat stat_val;
  int rc = stat(new_file_path_local_buffer.c_str(), &stat_val);
  ASSERT_TRUE(rc == 0);
  EXPECT_EQ(stat_val.st_size, actual_size);

  // Do upload the local buffer file.
  auto disk_id = io_mgr.RemoteDfsDiskFileOperId();
  RemoteOperRange::RemoteOperDoneCallback u_callback = [](const Status& upload_status) {};
  RemoteOperRange* upload_range = tmp_pool.Add(new RemoteOperRange(
      (*new_tmp_file_obj)->DiskBufferFile(), (*new_tmp_file_obj)->DiskFile(), block_size,
      disk_id, RequestType::FILE_UPLOAD, &io_mgr, u_callback));
  EXPECT_OK(io_ctx->AddRemoteOperRange(upload_range));

  int wait_times = 10;
  while (true) {
    if ((*new_tmp_file_obj)->DiskFile()->GetFileStatus() == DiskFileStatus::PERSISTED) {
      break;
    }
    // Suppose the upload should be finished in two seconds.
    ASSERT_TRUE(wait_times-- > 0);
    usleep(200 * 1000);
  }

  // Assert remote file actual size is the same as the local actual size.
  EXPECT_TRUE(HdfsFileExist(remote_file_path));
  ASSERT_EQ((*new_tmp_file_obj)->DiskFile()->actual_file_size(), actual_size);

  num_ranges_written_ = 0;
  tmp_file_grp->Close();
  io_mgr.UnregisterContext(io_ctx.get());
}
}
}
