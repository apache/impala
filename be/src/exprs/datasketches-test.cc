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

#include "thirdparty/datasketches/hll.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"

#include <sstream>
#include <stdlib.h>

#include "testutil/gtest-util.h"

namespace impala {

// This test is meant to cover that the HLL algorithm from the DataSketches library can
// be imported into Impala, builds without errors and the basic functionality is
// available to use.
// The below code is mostly a copy-paste from the example code found on the official
// DataSketches web page: https://datasketches.apache.org/docs/HLL/HllCppExample.html
// The purpose is to create 2 HLL sketches that have overlap in their data, serialize
// them, deserialize them and give a cardinality estimate combining the 2 sketches.
TEST(TestDataSketchesHll, UseDataSketchesInterface) {
  const int lg_k = 11;
  const auto type = datasketches::HLL_4;
  std::stringstream sketch_stream1;
  std::stringstream sketch_stream2;
  // This section generates two sketches with some overlap and serializes them into files
  {
    // 100000 distinct keys
    datasketches::hll_sketch sketch1(lg_k, type);
    for (int key = 0; key < 100000; key++) sketch1.update(key);
    sketch1.serialize_compact(sketch_stream1);

    // 100000 distinct keys where 50000 overlaps with sketch1
    datasketches::hll_sketch sketch2(lg_k, type);
    for (int key = 50000; key < 150000; key++) sketch2.update(key);
    sketch2.serialize_compact(sketch_stream2);
  }

  // This section deserializes the sketches and produces union
  {
    datasketches::hll_sketch sketch1 =
        datasketches::hll_sketch::deserialize(sketch_stream1);
    datasketches::hll_sketch sketch2 =
        datasketches::hll_sketch::deserialize(sketch_stream2);

    datasketches::hll_union union_sketch(lg_k);
    union_sketch.update(sketch1);
    union_sketch.update(sketch2);
    datasketches::hll_sketch sketch = union_sketch.get_result(type);

    // These sketching algorithms are sensitive for the order of the inputs and may
    // return different estimations withing the error bounds of the algorithm. However,
    // the order of the inputs fed to the sketches is fix here so we get the same
    // estimate every time we run this test.
    EXPECT_EQ(152040, (int)sketch.get_estimate());
  }
}


// This test is meant to cover that the KLL algorithm from the DataSketches library can
// be imported into Impala, builds without errors and the basic functionality is
// available to use.
// The below code is mostly a copy-paste from the example code found on the official
// DataSketches web page:
// https://datasketches.apache.org/docs/Quantiles/QuantilesCppExample.html
// The purpose is to create 2 KLL sketches that have overlap in their data, serialize
// them, deserialize them and get an estimate for quantiles after combining the 2
// sketches.
TEST(TestDataSketchesKll, UseDataSketchesInterface) {
  std::stringstream sketch_stream1;
  std::stringstream sketch_stream2;
  {
    datasketches::kll_sketch<float> sketch1;
    for (int i = 0; i < 100000; ++i) sketch1.update(i);
    sketch1.serialize(sketch_stream1);

    datasketches::kll_sketch<float> sketch2;
    for (int i = 30000; i < 130000; ++i) sketch2.update(i);
    sketch2.serialize(sketch_stream2);
  }

  {
    auto sketch1 = datasketches::kll_sketch<float>::deserialize(sketch_stream1);
    auto sketch2 = datasketches::kll_sketch<float>::deserialize(sketch_stream2);
    sketch1.merge(sketch2);

    const double fractions[3] {0, 0.5, 1};
    auto quantiles = sketch1.get_quantiles(fractions, 3);
    EXPECT_EQ(0, quantiles[0]);
    // The median is an approximate. Here we check that it is in 2% error range.
    int exact_median = 65000;
    EXPECT_LE(abs(quantiles[1] - exact_median), exact_median * 0.02);
    EXPECT_EQ(129999, quantiles[2]);
  }
}

}
