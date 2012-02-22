// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "common/object-pool.h"
#include "util/runtime-profile.h"

using namespace std;

namespace impala {

TEST(CountersTest, Basic) { 
  ObjectPool pool;
  RuntimeProfile profile_a(&pool, "ProfileA");
  RuntimeProfile profile_a1(&pool, "ProfileA1");
  RuntimeProfile profile_a2(&pool, "ProfileAb");

  TRuntimeProfileTree thrift_profiles;

  profile_a.AddChild(&profile_a1);
  profile_a.AddChild(&profile_a2);

  // Test Empty
  profile_a.ToThrift(&thrift_profiles.nodes);
  EXPECT_EQ(thrift_profiles.nodes.size(), 3);
  thrift_profiles.nodes.clear();

  RuntimeProfile::Counter* counter_a;
  RuntimeProfile::Counter* counter_b;
  RuntimeProfile::Counter* counter_merged;
  
  // Updating/setting counter
  counter_a = profile_a.AddCounter("A", TCounterType::UNIT);
  EXPECT_TRUE(counter_a != NULL);
  counter_a->Update(10);
  counter_a->Update(-5);
  EXPECT_EQ(counter_a->value(), 5);
  counter_a->Set(1);
  EXPECT_EQ(counter_a->value(), 1);
  
  counter_b = profile_a2.AddCounter("B", TCounterType::BYTES);
  EXPECT_TRUE(counter_b != NULL);

  // Serialize/deserialize
  profile_a.ToThrift(&thrift_profiles.nodes);
  RuntimeProfile* from_thrift = RuntimeProfile::CreateFromThrift(&pool, thrift_profiles);
  counter_merged = from_thrift->GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);
  EXPECT_TRUE(from_thrift->GetCounter("Not there") ==  NULL);


  // Merge
  RuntimeProfile merged_profile(&pool, "Merged");
  merged_profile.Merge(*from_thrift);
  counter_merged = merged_profile.GetCounter("A");
  EXPECT_EQ(counter_merged->value(), 1);

  // Merge 2 more times, counters should get aggregated
  merged_profile.Merge(*from_thrift);
  merged_profile.Merge(*from_thrift);
  EXPECT_EQ(counter_merged->value(), 3);
}

void ValidateCounter(RuntimeProfile* profile, const string& name, int64_t value) {
  RuntimeProfile::Counter* counter = profile->GetCounter(name);
  EXPECT_TRUE(counter != NULL);
  EXPECT_EQ(counter->value(), value);
}

TEST(CountersTest, Merge) {
  // Create two trees.  Each tree has two children, one of which has the
  // same name in both trees.  Merging the two trees should result in 3
  // children, with the counters from the shared child aggregated.

  ObjectPool pool;
  RuntimeProfile profile1(&pool, "Parent1");
  RuntimeProfile p1_child1(&pool, "Child1");
  RuntimeProfile p1_child2(&pool, "Child2");
  profile1.AddChild(&p1_child1);
  profile1.AddChild(&p1_child2);
  
  RuntimeProfile profile2(&pool, "Parent2");
  RuntimeProfile p2_child1(&pool, "Child1");
  RuntimeProfile p2_child3(&pool, "Child3");
  profile2.AddChild(&p2_child1);
  profile2.AddChild(&p2_child3);

  // Create parent level counters
  RuntimeProfile::Counter* parent1_shared = 
      profile1.AddCounter("Parent Shared", TCounterType::UNIT);
  RuntimeProfile::Counter* parent2_shared = 
      profile2.AddCounter("Parent Shared", TCounterType::UNIT);
  RuntimeProfile::Counter* parent1_only = 
      profile1.AddCounter("Parent 1 Only", TCounterType::UNIT);
  RuntimeProfile::Counter* parent2_only = 
      profile2.AddCounter("Parent 2 Only", TCounterType::UNIT);
  parent1_shared->Update(1);
  parent2_shared->Update(3);
  parent1_only->Update(2);
  parent2_only->Update(5);

  // Create child level counters
  RuntimeProfile::Counter* p1_c1_shared =
    p1_child1.AddCounter("Child1 Shared", TCounterType::UNIT);
  RuntimeProfile::Counter* p1_c1_only =
    p1_child1.AddCounter("Child1 Parent 1 Only", TCounterType::UNIT);
  RuntimeProfile::Counter* p1_c2 =
    p1_child2.AddCounter("Child2", TCounterType::UNIT);
  RuntimeProfile::Counter* p2_c1_shared =
    p2_child1.AddCounter("Child1 Shared", TCounterType::UNIT);
  RuntimeProfile::Counter* p2_c1_only =
    p1_child1.AddCounter("Child1 Parent 2 Only", TCounterType::UNIT);
  RuntimeProfile::Counter* p2_c3 =
    p2_child3.AddCounter("Child3", TCounterType::UNIT);
  p1_c1_shared->Update(10);
  p1_c1_only->Update(50);
  p2_c1_shared->Update(20);
  p2_c1_only->Update(100);
  p2_c3->Update(30);
  p1_c2->Update(40);

  // Merge the two and validate
  profile1.Merge(profile2);
  EXPECT_EQ(profile1.num_counters(), 4);
  ValidateCounter(&profile1, "Parent Shared", 4);
  ValidateCounter(&profile1, "Parent 1 Only", 2);
  ValidateCounter(&profile1, "Parent 2 Only", 5);

  vector<RuntimeProfile*> children = profile1.children();
  EXPECT_EQ(children.size(), 3);

  for (int i = 0; i < 3; ++i) {
    RuntimeProfile* profile = children[i];
    if (profile->name().compare("Child1") == 0) {
      EXPECT_EQ(profile->num_counters(), 4);
      ValidateCounter(profile, "Child1 Shared", 30);
      ValidateCounter(profile, "Child1 Parent 1 Only", 50);
      ValidateCounter(profile, "Child1 Parent 2 Only", 100);
    } else if (profile->name().compare("Child2") == 0) {
      EXPECT_EQ(profile->num_counters(), 2);
      ValidateCounter(profile, "Child2", 40);
    } else if (profile->name().compare("Child3") == 0) {
      EXPECT_EQ(profile->num_counters(), 2);
      ValidateCounter(profile, "Child3", 30);
    } else {
      EXPECT_TRUE(false);
    }
  }
}

}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

