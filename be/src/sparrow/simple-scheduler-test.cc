// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <gtest/gtest.h>

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "simple-scheduler.h"
#include "subscription-manager.h"

using namespace std;
using namespace boost;
using namespace impala;

namespace sparrow {

class SimpleSchedulerTest : public testing::Test {
 protected:
  SimpleSchedulerTest() {
    // Setup localhost_scheduler
    num_backends_ = 3;
    base_port_ = 1000;
    vector<THostPort> backends;
    backends.resize(num_backends_);
    for (int i = 0; i < num_backends_; ++i) {
      backends.at(i).ipaddress = "127.0.0.1";
      backends.at(i).port = base_port_ + i;
    }
    localhost_scheduler_.reset(new SimpleScheduler(backends, NULL));

    // Setup remote_scheduler
    for (int i = 0; i < num_backends_; ++i) {
      stringstream ss;
      ss << "host_" << i;
      backends.at(i).ipaddress = ss.str();
      backends.at(i).port = base_port_ + i;
    }
    remote_scheduler_.reset(new SimpleScheduler(backends, NULL));

    // Setup local_remote_scheduler_
    backends.resize(4);
    int k = 0;
    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 2; ++j) {
        stringstream ss;
        ss << "host_" << i;
        backends.at(k).ipaddress = ss.str();
        backends.at(k).port = base_port_ + j;
        ++k;
      }
    }
    local_remote_scheduler_.reset(new SimpleScheduler(backends, NULL));
  }

  virtual void TearDown() {
    localhost_scheduler_->Close();
    remote_scheduler_->Close();
    local_remote_scheduler_->Close();
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
  vector<THostPort> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).ipaddress = "host_1";
    data_locations.at(i).port = 0;
  }
  vector<pair<string, int> > hostports;

  local_remote_scheduler_->GetHosts(data_locations, &hostports);

  // Expect 5 round robin hostports
  EXPECT_EQ(5, hostports.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(hostports.at(i).first, "host_1");
    EXPECT_EQ(hostports.at(i).second, base_port_ + i % 2);
  }
}

TEST_F(SimpleSchedulerTest, NonLocalHost) {
  // If data location doesn't match any of the ipaddress,
  // expect round robin ipaddress/port list
  vector<THostPort> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).ipaddress = "non exists ipaddress";
    data_locations.at(i).port = 0;
  }
  vector<pair<string, int> > hostports;

  local_remote_scheduler_->GetHosts(data_locations, &hostports);

  // Expect 5 round robin on ipaddress, then on port
  // 1. host_0:1000
  // 2. host_1:1000
  // 3. host_0:1001
  // 4. host_1:1001
  // 5. host_0:1000
  EXPECT_EQ(5, hostports.size());
  EXPECT_EQ(hostports.at(0).first, "host_1");
  EXPECT_EQ(hostports.at(0).second, 1000);
  EXPECT_EQ(hostports.at(1).first, "host_0");
  EXPECT_EQ(hostports.at(1).second, 1000);
  EXPECT_EQ(hostports.at(2).first, "host_1");
  EXPECT_EQ(hostports.at(2).second, 1001);
  EXPECT_EQ(hostports.at(3).first, "host_0");
  EXPECT_EQ(hostports.at(3).second, 1001);
  EXPECT_EQ(hostports.at(4).first, "host_1");
  EXPECT_EQ(hostports.at(4).second, 1000);
}

 TEST_F(SimpleSchedulerTest, CleanShutdownWithoutInit) {
   SubscriptionManager subscription_manager;
   SimpleScheduler simple_scheduler(&subscription_manager, "dummy_service_id", NULL);
   // We're checking that the SimpleScheduler destructor finishes cleanly if Init is not
   // called, so just allow simple_scheduler to go out of scope.
 }

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
