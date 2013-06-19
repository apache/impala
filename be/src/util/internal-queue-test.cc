// Copyright 2013 Cloudera Inc.
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


#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include "util/internal-queue.h"

using namespace boost;
using namespace std;

namespace impala {

struct IntNode : public InternalQueue<IntNode>::Node {
  IntNode(int value = 0) : value(value) {}
  int value;
};

// Basic single threaded operation.
TEST(InternalQueue, TestBasic) {
  IntNode one(1);
  IntNode two(2);
  IntNode three(3);
  IntNode four(4);

  InternalQueue<IntNode> list;
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(list.size(), 0);
  ASSERT_TRUE(list.Dequeue() == NULL);
  ASSERT_TRUE(list.Validate());

  list.Enqueue(&one);
  ASSERT_TRUE(!list.empty());
  ASSERT_EQ(list.size(), 1);
  IntNode* i = list.Dequeue();
  ASSERT_TRUE(i != NULL);
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(list.size(), 0);
  ASSERT_EQ(i->value, 1);
  ASSERT_TRUE(list.Validate());

  list.Enqueue(&one);
  list.Enqueue(&two);
  list.Enqueue(&three);
  list.Enqueue(&four);
  ASSERT_EQ(list.size(), 4);
  ASSERT_TRUE(list.Validate());

  i = list.Dequeue();
  ASSERT_TRUE(i != NULL);
  ASSERT_EQ(i->value, 1);
  ASSERT_TRUE(list.Validate());
  
  i = list.Dequeue();
  ASSERT_TRUE(i != NULL);
  ASSERT_EQ(i->value, 2);
  ASSERT_TRUE(list.Validate());
  
  i = list.Dequeue();
  ASSERT_TRUE(i != NULL);
  ASSERT_EQ(i->value, 3);
  ASSERT_TRUE(list.Validate());
  
  i = list.Dequeue();
  ASSERT_TRUE(i != NULL);
  ASSERT_EQ(i->value, 4);
  ASSERT_TRUE(list.Validate());
}

// Add all the nodes and then remove every other one.
TEST(InternalQueue, TestRemove) {
  vector<IntNode> nodes;
  nodes.resize(100);

  InternalQueue<IntNode> queue;

  queue.Enqueue(&nodes[0]);
  queue.Remove(&nodes[1]);
  ASSERT_TRUE(queue.Validate());
  queue.Remove(&nodes[0]);
  ASSERT_TRUE(queue.Validate());
  queue.Remove(&nodes[0]);
  ASSERT_TRUE(queue.Validate());

  for (int i = 0; i < nodes.size(); ++i) {
    nodes[i].value = i;
    queue.Enqueue(&nodes[i]);
  }

  for (int i = 0; i < nodes.size(); i += 2) {
    queue.Remove(&nodes[i]);
    ASSERT_TRUE(queue.Validate());
  }

  ASSERT_EQ(queue.size(), nodes.size() / 2);
  for (int i = 0; i < nodes.size() / 2; ++i) {
    IntNode* node = queue.Dequeue();
    ASSERT_TRUE(node != NULL);
    ASSERT_EQ(node->value, i * 2 + 1);
  }
}

const int VALIDATE_INTERVAL = 10000;
void ProducerThread(InternalQueue<IntNode>* queue, int num_inserts, 
    vector<IntNode>& nodes, AtomicInt<int32_t>* counter) {
  for (int i = 0; i < num_inserts; ++i) {
    // Get the next index to queue.
    AtomicInt<int32_t> value = (*counter)++;
    IntNode* node = &nodes[value];
    node->value = value;
    queue->Enqueue(node);
    if (i % VALIDATE_INTERVAL == 0) ASSERT_TRUE(queue->Validate());
  }
}

void ConsumerThread(InternalQueue<IntNode>* queue, int num_consumes, int delta,
    vector<int>* results) {
  // Dequeued nodes should be strictly increasing.
  int previous_value = -1;
  for (int i = 0; i < num_consumes;) {
    IntNode* node = queue->Dequeue();
    if (node == NULL) continue;
    ++i;
    if (delta > 0) {
      ASSERT_EQ(node->value, previous_value + delta);
    } else if (delta == 0) {
      ASSERT_GT(node->value, previous_value);
    }
    results->push_back(node->value);
    previous_value = node->value;
    if (i % VALIDATE_INTERVAL == 0) ASSERT_TRUE(queue->Validate());
  }
}

TEST(InternalQueue, TestSingleProducerSingleConsumer) {
  vector<IntNode> nodes;
  AtomicInt<int32_t> counter;
  nodes.resize(1000000);
  vector<int> results;

  InternalQueue<IntNode> queue;
  ProducerThread(&queue, nodes.size(), nodes, &counter);
  ConsumerThread(&queue, nodes.size(), 1, &results);
  ASSERT_TRUE(queue.empty());
  ASSERT_EQ(results.size(), nodes.size());

  counter = 0;
  results.clear();
  thread producer_thread(ProducerThread, &queue, nodes.size(), nodes, &counter);
  thread consumer_thread(ConsumerThread, &queue, nodes.size(), 1, &results);
  producer_thread.join();
  consumer_thread.join();
  ASSERT_TRUE(queue.empty());
  ASSERT_EQ(results.size(), nodes.size());
}

TEST(InternalQueue, TestMultiProducerMultiConsumer) {
  vector<IntNode> nodes;
  nodes.resize(1000000);

  for (int num_producers = 1; num_producers < 5; num_producers += 3) {
    AtomicInt<int32_t> counter;
    const int NUM_CONSUMERS = 4;
    ASSERT_EQ(nodes.size() % NUM_CONSUMERS, 0);
    ASSERT_EQ(nodes.size() % num_producers, 0);
    const int num_per_consumer = nodes.size() / NUM_CONSUMERS;
    const int num_per_producer = nodes.size() / num_producers;
    
    vector<vector<int> > results;
    results.resize(NUM_CONSUMERS);

    InternalQueue<IntNode> queue;
    thread_group consumers;
    thread_group producers;

    for (int i = 0; i < num_producers; ++i) {
      producers.add_thread(
          new thread(ProducerThread, &queue, num_per_producer, nodes, &counter));
    }

    for (int i = 0; i < NUM_CONSUMERS; ++i) {
      int expected_delta = -1;
      if (NUM_CONSUMERS == 1 && num_producers == 1) {
        // With one producer and consumer, the queue should have sequential values.
        expected_delta = 1;
      } else if (num_producers == 1) {
        // With one producer, the values added are sequential but can be read off
        // with gaps in each consumer thread.  E.g. thread1 reads: 1, 4, 5, 7, etc.
        // but they should be strictly increasing.
        expected_delta = 0;
      } else {
        // With multiple producers there isn't a guarantee on the order values get
        // enqueued.
        expected_delta = -1;
      }
      consumers.add_thread(new thread(
          ConsumerThread, &queue, num_per_consumer, expected_delta, &results[i]));
    }

    producers.join_all();
    consumers.join_all();
    ASSERT_TRUE(queue.empty());

    vector<int> all_results;
    for (int i = 0; i < NUM_CONSUMERS; ++i) {
      ASSERT_EQ(results[i].size(), num_per_consumer);
      all_results.insert(all_results.end(), results[i].begin(), results[i].end());
    }
    ASSERT_EQ(all_results.size(), nodes.size());
    sort(all_results.begin(), all_results.end());
    for (int i = 0; i < all_results.size(); ++i) {
      ASSERT_EQ(i, all_results[i]) << all_results[i -1] << " " << all_results[i + 1];
    }
  }
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
