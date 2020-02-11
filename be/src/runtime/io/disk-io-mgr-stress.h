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


#ifndef IMPALA_RUNTIME_DISK_IO_MGR_STRESS_H
#define IMPALA_RUNTIME_DISK_IO_MGR_STRESS_H

#include <memory>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "common/object-pool.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"

namespace impala {
namespace io {

/// Test utility to stress the disk io mgr.  It allows for a configurable
/// number of clients.  The clients continuously issue work to the io mgr and
/// asynchronously get cancelled.  The stress test can be run forever or for
/// a fixed duration.  The unit test runs this for a fixed duration.
class DiskIoMgrStress {
 public:
  DiskIoMgrStress(int num_disks, int num_threads_per_disk, int num_clients,
      bool includes_cancellation);
  ~DiskIoMgrStress();

  /// Run the test for 'sec'.  If 0, run forever
  void Run(int sec);

  static constexpr float ABORT_CHANCE = .10f;
  static const int MIN_READ_LEN = 1;
  static const int MAX_READ_LEN = 20;

  static const int MIN_FILE_LEN = 10;
  static const int MAX_FILE_LEN = 1024;

  // Make sure this is between MIN/MAX FILE_LEN to test more cases
  static const int MIN_READ_BUFFER_SIZE = 64;
  static const int MAX_READ_BUFFER_SIZE = 128;

  // Maximum bytes to allocate per scan range.
  static const int MAX_BUFFER_BYTES_PER_SCAN_RANGE = MAX_READ_BUFFER_SIZE * 3;

  static const int CANCEL_READER_PERIOD_MS = 20;
 private:
  struct Client;

  struct File {
    std::string filename;
    std::string data; // the data in the file, used to validate
  };

  /// Files used for testing.  These are created at startup and recycled
  /// during the test
  std::vector<File> files_;

  /// Root mem tracker.
  MemTracker mem_tracker_;

  /// io manager
  boost::scoped_ptr<DiskIoMgr> io_mgr_;

  /// Thread group for reader threads
  boost::thread_group readers_;

  /// Array of clients
  int num_clients_;
  std::unique_ptr<Client[]> clients_;

  /// Client MemTrackers, one per client.
  std::vector<std::unique_ptr<MemTracker>> client_mem_trackers_;

  /// Buffer pool clients, one per client.
  std::unique_ptr<BufferPool::ClientHandle[]> buffer_pool_clients_;

  /// If true, tests cancelling readers
  bool includes_cancellation_;

  /// Flag to signal that client reader threads should exit
  AtomicBool shutdown_;

  /// Helper to initialize a new reader client, registering a new reader with the
  /// io mgr and initializing the scan ranges
  void NewClient(int i);

  /// Thread running the reader.  When the current reader is done (either normally
  /// or cancelled), it picks up a new reader
  void ClientThread(int client_id);

  /// Possibly cancels a random reader.
  void CancelRandomReader();

  static std::string GenerateRandomData();
};
}
}

#endif
