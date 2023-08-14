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

import {exportedForTest} from "../../query_timeline/fragment_metrics_diagram.js";

describe("Test initializeFragmentMetrics", () => {
  // Test whether aggregate arrays and time sample arrays are correctly allocated
  // based on counters and max_samples
  var {initializeFragmentMetrics} = exportedForTest;
  test("Basic Test", () => {
    var parent_profile =
    {
      "profile_name": "Coordinator Fragment F31",
      "num_children": 1,
      "child_profiles": [
        {
          "profile_name": "Instance fe45c9c56d1:efd1b2a70000 (host=host-1:27000)",
          "time_series_counters": [{
            "counter_name": "MemoryUsage",
            "unit": "BYTES",
            "num": 6,
            "period": 500,
            "data": "12288,12288,12288,12288,12288,12288"
          }, {
            "counter_name": "ThreadUsage",
            "unit": "UNIT",
            "num": 1,
            "period": 500,
            "data": "4"
          }]
        }
      ]
    };
    var max_samples = {
      allocated : 4,
      period : 0,
      available : 0,
      collected : 0
    };
    var counters = [
        ["MemoryUsage", "memory usage", 0],
        ["ThreadUsage", "thread usage", 0]
    ];
    var timeaxis_name = "fragment metrics timeticks";
    var {fragment_metrics_aggregate, sampled_fragment_metrics_timeseries}
        = initializeFragmentMetrics(parent_profile, counters, max_samples, timeaxis_name);
    expect(fragment_metrics_aggregate).toEqual([
        [null, 0, null, null, null, null, null],
        [null, 0, null, null, null, null, null]
    ]);
    expect(sampled_fragment_metrics_timeseries).toEqual(
        [timeaxis_name, null, null, null, null, null, null]
    );
  });
});
