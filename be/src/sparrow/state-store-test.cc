// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <vector>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/thread/thread_time.hpp>
#include <boost/unordered_set.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <transport/TSocket.h>

#include "common/status.h"
#include "sparrow/state-store-service.h"
#include "sparrow/state-store-subscriber-service.h"
#include "sparrow/util.h"
#include "util/thrift-util.h"
#include "gen-cpp/SparrowTypes_types.h"
#include "gen-cpp/Types_types.h"

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace boost;
using namespace std;
using impala::Status;
using impala::THostPort;

namespace sparrow {

class StateStoreTest : public testing::Test {
 public:
  struct UpdateCondition {
    mutex mut;
    condition_variable condition;
    ServiceStateMap expected_state;
    bool correctly_called;
    system_time time_last_called;

    UpdateCondition() : correctly_called(false) {};
  };

  // Callback to use when registering for updates.
  static void Update(UpdateCondition* update_condition, const ServiceStateMap& state) {
    // Update may be called before the expected instances have registered.
    {
      lock_guard<mutex> lock(update_condition->mut);
      if ((update_condition->expected_state != state) &&
          !(update_condition->expected_state.empty() && state.empty())) {
        return;
      }

      VLOG_CONNECTION << "satisfied with size "
                      << update_condition->expected_state.size();
      update_condition->correctly_called = true;
      update_condition->time_last_called = get_system_time();
    }
    update_condition->condition.notify_one();
  }

 protected:
  static int next_port_;
  static const char* host_;

  THostPort state_store_host_port_;
  shared_ptr<StateStore> state_store_;

  vector<shared_ptr<StateStoreSubscriber> > subscribers_;

  StateStoreTest() : state_store_(new StateStore()) {
    state_store_host_port_.host = "localhost";
    state_store_host_port_.port = next_port_++;
  }

  virtual void SetUp() {
    impala::InitThriftLogging();
    state_store_->Start(state_store_host_port_.port);
    Status status =
      impala::WaitForServer(state_store_host_port_.host,
                            state_store_host_port_.port, 3, 2000);
    EXPECT_TRUE(status.ok());
  }

  shared_ptr<StateStoreSubscriber> StartStateStoreSubscriber() {
    int port = next_port_++;
    subscribers_.push_back(shared_ptr<StateStoreSubscriber>(new StateStoreSubscriber(
        host_, port, state_store_host_port_.host, state_store_host_port_.port)));
    subscribers_.back()->Start();
    Status status = impala::WaitForServer("localhost", port, 10, 100);
    EXPECT_TRUE(status.ok());
    return subscribers_.back();
  }

  void UnregisterAllSubscribers() {
    for (int i = 0; i < subscribers_.size(); ++i) {
      if (subscribers_[i]->IsRunning()) {
        subscribers_[i]->UnregisterAll();
      }
    }
  }

};

const char* StateStoreTest::host_ = "localhost";
int StateStoreTest::next_port_ = 23000;

TEST_F(StateStoreTest, SingleRegister) {
  const string service_id = "test_service";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();

  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Address where service_id is running.
  THostPort service_address;
  service_address.host = host_;
  service_address.port = next_port_++;

  // We expect the membership to include just one running instance of service_id.
  UpdateCondition update_condition;
  update_condition.expected_state[service_id].membership = Membership();
  const SubscriberId expected_assigned_id = 1;
  update_condition.expected_state[service_id].membership[expected_assigned_id] =
      service_address;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &update_condition, _1));

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services;
  update_services.insert(service_id);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_TRUE(status.ok());

  // Register a running service on running_subscriber, and wait for the
  // listening_subscriber to receive the update.
  status = running_subscriber->RegisterService(service_id, service_address);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock(update_condition.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!update_condition.correctly_called) {
      ASSERT_TRUE(update_condition.condition.timed_wait(lock, timeout));
    }
  }

  // Unregister everything, because the UpdateCallback class checks
  // that the callback has been unregistered (which happens with the associated
  // subscrition is unregistered) in its destructor.
  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, MultipleServiceRegister) {
  const string service1 = "test_service_1";
  const string service2 = "test_service_2";

  // Subscriber running for service 1.
  shared_ptr<StateStoreSubscriber> running_subscriber1 = StartStateStoreSubscriber();

  // Subscriber running for service 2.
  shared_ptr<StateStoreSubscriber> running_subscriber2 = StartStateStoreSubscriber();

  // Subscriber (which will register for updates only from service 2).
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Addresses where services are running.
  THostPort service1_address;
  service1_address.host = host_;
  service1_address.port = next_port_++;

  THostPort service2_address;
  service2_address.host = host_;
  service2_address.port = next_port_++;

  // We expect the membership to include just one running instance of service2.
  UpdateCondition update_condition;
  update_condition.expected_state[service2].membership = Membership();
  const SubscriberId expected_assigned_id = 2;
  update_condition.expected_state[service2].membership[expected_assigned_id] =
      service2_address;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &update_condition, _1));

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services;
  update_services.insert(service2);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_TRUE(status.ok());

  // Register both running services, and wait for the listening_subscriber to receive
  // the correct update.
  status = running_subscriber1->RegisterService(service1, service1_address);
  EXPECT_TRUE(status.ok());
  status = running_subscriber2->RegisterService(service2, service2_address);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock(update_condition.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!update_condition.correctly_called) {
      ASSERT_TRUE(update_condition.condition.timed_wait(lock, timeout));
    }
  }
  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, RegisterFailsGracefullyWhenStateStoreUnreachable) {
  // Nonblocking Thrift servers can't be stopped, so just point at a non-open socket
  state_store_host_port_.host = "localhost";
  state_store_host_port_.port = next_port_++;

  const string service_id = "test_service";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Register the subscribers, and ensure that both return errors.

  // Dummy update function, since we need to pass one to RegisterSubscription().
  UpdateCondition update_condition;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &update_condition, _1));
  unordered_set<string> update_services;
  update_services.insert(service_id);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_FALSE(status.ok());

  // Address where service_id is running.
  THostPort service_address;
  service_address.host = host_;
  service_address.port = next_port_++;
  status = running_subscriber->RegisterService(service_id, service_address);
  EXPECT_FALSE(status.ok());

  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, UnregisterService) {
  const string service_id = "test_service";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Address where service_id is running.
  THostPort service_address;
  service_address.host = host_;
  service_address.port = next_port_++;

  // We expect the membership to include just one running instance of service_id.
  UpdateCondition register_condition;
  register_condition.expected_state[service_id].membership = Membership();
  const SubscriberId expected_assigned_id = 1;
  register_condition.expected_state[service_id].membership[expected_assigned_id] =
      service_address;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &register_condition, _1));

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services;
  update_services.insert(service_id);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_TRUE(status.ok());

  // Register a running service on running_subscriber, and wait for the
  // listening_subscriber to receive the update.
  status = running_subscriber->RegisterService(service_id, service_address);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock(register_condition.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition.correctly_called) {
      ASSERT_TRUE(register_condition.condition.timed_wait(lock, timeout));
    }

    // Now, unregister the running instance, and ensure that listening_subscriber gets
    // updated accordingly (with an empty state that contains no registered instances).
    // register_condition is still locked here, so can safely change the expected_state.
    register_condition.expected_state.clear();
    register_condition.correctly_called = false;

    status = running_subscriber->UnregisterService(service_id);
    EXPECT_TRUE(status.ok());

    timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition.correctly_called) {
      ASSERT_TRUE(register_condition.condition.timed_wait(lock, timeout));
    }
  }

  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, UnregisterSubscription) {
  const string service_id = "test_service";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Address where service_id is running.
  THostPort service_address;
  service_address.host = host_;
  service_address.port = next_port_++;

  // We expect the membership to include just one running instance of service_id.
  UpdateCondition register_condition;
  register_condition.expected_state[service_id].membership = Membership();
  const SubscriberId expected_assigned_id = 1;
  register_condition.expected_state[service_id].membership[expected_assigned_id] =
      service_address;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &register_condition, _1));

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services;
  update_services.insert(service_id);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_TRUE(status.ok());

  // Register a running service on running_subscriber, and wait for the
  // listening_subscriber to receive the update.
  status = running_subscriber->RegisterService(service_id, service_address);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock(register_condition.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition.correctly_called) {
      ASSERT_TRUE(register_condition.condition.timed_wait(lock, timeout));
    }
  }

  // Now, unregister the subscription, and ensure that update stops getting called.
  status = listening_subscriber->UnregisterSubscription(id);
  EXPECT_TRUE(status.ok());

  system_time timeout = get_system_time() + posix_time::seconds(10);
  while (true) {
    system_time current_time = get_system_time();
    {
      lock_guard<mutex> lock(register_condition.mut);
      if (current_time - register_condition.time_last_called >
          posix_time::seconds(state_store_->subscriber_update_frequency_ms() 
              * 2 / 1000 )) {
        break;
      }
    }

    // Ensure that the test times out, rather than running indefinitely.
    ASSERT_LT(current_time, timeout);
    usleep(state_store_->subscriber_update_frequency_ms() * 1000);
  }
  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, UnregisterOneOfMultipleSubscriptions) {
  // This test registers 2 subscriptions from the same subscriber, one for services
  // service_id_1 and service_id_2, and the other for service_id_2 and service_id_3.
  // Then, it unregisters the first subscription, and ensures that the subscriber still
  // receives updates for service_id_2 and service_id_3 (but not for service_id_1).
  const string service_id_1 = "test_service_1";
  const string service_id_2 = "test_service_2";
  const string service_id_3 = "test_service_3";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Address where services are running.
  THostPort service_address_1;
  service_address_1.host = host_;
  service_address_1.port = next_port_++;
  THostPort service_address_2;
  service_address_2.host = host_;
  service_address_2.port = next_port_++;
   THostPort service_address_3;
  service_address_3.host = host_;
  service_address_3.port = next_port_++;

  // We expect the membership to include one instance of each service.
  UpdateCondition register_condition_A;
  ServiceStateMap& expected_state_A = register_condition_A.expected_state;
  const SubscriberId expected_first_assigned_id = 1;
  expected_state_A[service_id_1].membership = Membership();
  expected_state_A[service_id_1].membership[expected_first_assigned_id] =
      service_address_1;
  expected_state_A[service_id_2].membership = Membership();
  expected_state_A[service_id_2].membership[expected_first_assigned_id] =
      service_address_2;
  expected_state_A[service_id_3].membership = Membership();
  expected_state_A[service_id_3].membership[expected_first_assigned_id] =
      service_address_3;

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services_A;
  update_services_A.insert(service_id_1);
  update_services_A.insert(service_id_2);
  SubscriptionManager::UpdateCallback update_callback_A(
      bind(&StateStoreTest::Update, &register_condition_A, _1));
  SubscriptionId id_A;
  Status status = listening_subscriber->RegisterSubscription(update_services_A,
                                                             &update_callback_A, &id_A);
  EXPECT_TRUE(status.ok());

  unordered_set<string> update_services_B;
  update_services_B.insert(service_id_2);
  update_services_B.insert(service_id_3);
  UpdateCondition register_condition_B;
  register_condition_B.expected_state = register_condition_A.expected_state;
  SubscriptionManager::UpdateCallback update_callback_B(
      bind(&StateStoreTest::Update, &register_condition_B, _1));
  SubscriptionId id_B;
  status = listening_subscriber->RegisterSubscription(update_services_B,
                                                      &update_callback_B, &id_B);
  EXPECT_TRUE(status.ok());

  // Register the three services on running_subscriber, and wait for both subscriptions
  // to get updated.
  status = running_subscriber->RegisterService(service_id_1, service_address_1);
  EXPECT_TRUE(status.ok());
  status = running_subscriber->RegisterService(service_id_2, service_address_2);
  EXPECT_TRUE(status.ok());
  status = running_subscriber->RegisterService(service_id_3, service_address_3);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock_A(register_condition_A.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition_A.correctly_called) {
      ASSERT_TRUE(register_condition_A.condition.timed_wait(lock_A, timeout));
    }

    unique_lock<mutex> lock_B(register_condition_B.mut);
    timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition_B.correctly_called) {
      ASSERT_TRUE(register_condition_B.condition.timed_wait(lock_B, timeout));
    }
  }

  // Now, unregister the subscription, and ensure that update stops getting called for
  // subscription A, and that update does get called correctly for subscription B.
  status = listening_subscriber->UnregisterSubscription(id_A);
  EXPECT_TRUE(status.ok());


  // First, make sure that Update() gets called correctly for B (with information
  // about only service_id_2 and service_id_3).
  {
    unique_lock<mutex> lock_B(register_condition_B.mut);
    register_condition_B.expected_state.erase(service_id_1);
    register_condition_B.correctly_called = false;
    VLOG_CONNECTION << register_condition_B.expected_state.size();
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition_B.correctly_called) {
      ASSERT_TRUE(register_condition_B.condition.timed_wait(lock_B, timeout));
    }
    VLOG_CONNECTION << "correctly called";
  }

  system_time timeout = get_system_time() + posix_time::seconds(10);
  while (true) {
    system_time current_time = get_system_time();
    {
      lock_guard<mutex> lock(register_condition_A.mut);
      if (current_time - register_condition_A.time_last_called >
          posix_time::microseconds(2 * state_store_->subscriber_update_frequency_ms())) {
        break;
      }
    }

    // Ensure that the test times out, rather than running indefinitely.
    ASSERT_LT(current_time, timeout);
    usleep(state_store_->subscriber_update_frequency_ms());
  }
  UnregisterAllSubscribers();
};

TEST_F(StateStoreTest, UnregisterAll) {
  const string service_id = "test_service";

  shared_ptr<StateStoreSubscriber> running_subscriber = StartStateStoreSubscriber();
  shared_ptr<StateStoreSubscriber> listening_subscriber = StartStateStoreSubscriber();

  // Address where service_id is running.
  THostPort service_address;
  service_address.host = host_;
  service_address.port = next_port_++;

  // We expect the membership to include just one running instance of service_id.
  UpdateCondition register_condition;
  register_condition.expected_state[service_id].membership = Membership();
  const SubscriberId expected_assigned_id = 1;
  register_condition.expected_state[service_id].membership[expected_assigned_id] =
      service_address;
  SubscriptionManager::UpdateCallback update_callback(
      bind(&StateStoreTest::Update, &register_condition, _1));

  // Register the listening_subscriber to receive updates.
  unordered_set<string> update_services;
  update_services.insert(service_id);
  SubscriptionId id;
  Status status = listening_subscriber->RegisterSubscription(update_services,
                                                             &update_callback, &id);
  EXPECT_TRUE(status.ok());

  // Register a running service on running_subscriber, and wait for the
  // listening_subscriber to receive the update.
  status = running_subscriber->RegisterService(service_id, service_address);
  EXPECT_TRUE(status.ok());

  {
    unique_lock<mutex> lock(register_condition.mut);
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition.correctly_called) {
      ASSERT_TRUE(register_condition.condition.timed_wait(lock, timeout));
    }
  }

  // Now, unregister everything on the running instance, and ensure that
  // everything is actually unregistered (by checking that listening_subscriber gets
  // updated accordingly).
  EXPECT_TRUE(status.ok());
  running_subscriber->UnregisterAll();

  {
    unique_lock<mutex> register_lock(register_condition.mut);
    register_condition.correctly_called = false;
    register_condition.expected_state.clear();
    system_time timeout = get_system_time() + posix_time::seconds(10);
    while (!register_condition.correctly_called) {
      ASSERT_TRUE(register_condition.condition.timed_wait(register_lock, timeout));
    }
  }
  UnregisterAllSubscribers();
};

} // namespace sparrow

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
