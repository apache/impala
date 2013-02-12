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

#include <gtest/gtest.h>

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "simple-scheduler.h"

using namespace std;
using namespace boost;
using namespace impala;

namespace impala {

class SimpleSchedulerTest : public testing::Test {
 protected:
  SimpleSchedulerTest() {
    // The simple scheduler tries to resolve all backend hostnames to
    // IP addresses to compute locality. For the purposes of this test
    // we need a set of resolvable hostnames, so here we use IP
    // addresses which are always resolvable (to themselves!).

    // Setup localhost_scheduler
    num_backends_ = 3;
    base_port_ = 1000;
    vector<TNetworkAddress> backends;
    backends.resize(num_backends_);
    for (int i = 0; i < num_backends_; ++i) {
      backends.at(i).hostname = "127.0.0.0";
      backends.at(i).port = base_port_ + i;
    }
    localhost_scheduler_.reset(new SimpleScheduler(backends, NULL));

    // Setup remote_scheduler
    for (int i = 0; i < num_backends_; ++i) {
      stringstream ss;
      ss << "127.0.0." << i;
      backends.at(i).hostname = ss.str();
      backends.at(i).port = base_port_ + i;
    }
    remote_scheduler_.reset(new SimpleScheduler(backends, NULL));

    // Setup local_remote_scheduler_
    backends.resize(4);
    int k = 0;
    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 2; ++j) {
        stringstream ss;
        ss << "127.0.0." << i;
        backends.at(k).hostname = ss.str();
        backends.at(k).port = base_port_ + j;
        ++k;
      }
    }
    local_remote_scheduler_.reset(new SimpleScheduler(backends, NULL));
  }

  int base_port_;
  int num_backends_;

  // This scheduler has 3 backends, all on localhost but have 3 different ports.
  boost::scoped_ptr<SimpleScheduler> localhost_scheduler_;

  // This scheduler has 3 backends on different ipaddresses and has 3 different ports.
  boost::scoped_ptr<SimpleScheduler> remote_scheduler_;

  // This scheduler has 4 backends; 2 on each ipaddresses and has 4 different ports.
  boost::scoped_ptr<SimpleScheduler> local_remote_scheduler_;
};


TEST_F(SimpleSchedulerTest, LocalMatches) {
  // If data location matches some back end, use the matched backends in round robin.
  vector<TNetworkAddress> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).hostname = "127.0.0.1";
    data_locations.at(i).port = 0;
  }
  SimpleScheduler::HostList hostports;

  local_remote_scheduler_->GetHosts(data_locations, &hostports);

  // Expect 5 round robin hostports
  EXPECT_EQ(5, hostports.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(hostports.at(i).hostname, "127.0.0.1");
    EXPECT_EQ(hostports.at(i).port, base_port_ + i % 2);
  }
}

TEST_F(SimpleSchedulerTest, NonLocalHost) {
  // If data location doesn't match any of the ipaddress,
  // expect round robin ipaddress/port list
  vector<TNetworkAddress> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).hostname = "non exists ipaddress";
    data_locations.at(i).port = 0;
  }
  SimpleScheduler::HostList hostports;

  local_remote_scheduler_->GetHosts(data_locations, &hostports);

  // Expect 5 round robin on ipaddress, then on port
  // 1. 127.0.0.1:1000
  // 2. 127.0.0.0:1000
  // 3. 127.0.0.1:1001
  // 4. 127.0.0.0:1001
  // 5. 127.0.0.1:1000
  EXPECT_EQ(5, hostports.size());
  EXPECT_EQ(hostports.at(0).hostname, "127.0.0.1");
  EXPECT_EQ(hostports.at(0).port, 1000);
  EXPECT_EQ(hostports.at(1).hostname, "127.0.0.0");
  EXPECT_EQ(hostports.at(1).port, 1000);
  EXPECT_EQ(hostports.at(2).hostname, "127.0.0.1");
  EXPECT_EQ(hostports.at(2).port, 1001);
  EXPECT_EQ(hostports.at(3).hostname, "127.0.0.0");
  EXPECT_EQ(hostports.at(3).port, 1001);
  EXPECT_EQ(hostports.at(4).hostname, "127.0.0.1");
  EXPECT_EQ(hostports.at(4).port, 1000);
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
