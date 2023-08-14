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

import {describe, test, expect} from '@jest/globals';
import {exportedForTest} from "../../query_timeline/host_utilization_diagram.js";

describe("Test initializeUtilizationMetrics", () => {
  // Test whether aggregate arrays and time sample arrays are correctly allocated
  // based on counters and max_samples
  var {initializeUtilizationMetrics} = exportedForTest;
  test("Basic Test", () => {
    var parent_profile =
    {
      "profile_name": "Per Node Profiles",
      "num_children": 3,
      "child_profiles": [
        {
          "profile_name": "host-1:27000",
          "time_series_counters": [{
            "counter_name": "HostCpuUserPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "0,0,0,70,0,0,0,0,0,10"
          }, {
            "counter_name": "HostCpuSysPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }, {
            "counter_name": "HostNetworkRx",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        }
      ]
    };
    var max_samples = {
      allocated : 3,
      period : 0,
      available : 0,
      collected : 0
    };
    var counters_y1 = [
        ["HostCpuUserPercentage", "avg io wait", 0],
        ["HostCpuSysPercentage", "avg sys", 0]
    ];
    var counters_y2 = [
        ["HostNetworkRx", "avg network rx", 0]
    ];
    var timeaxis_name = "utilization timeticks";
    var {cpu_nodes_usage_aggregate, read_write_metrics_aggregate,
        sampled_utilization_timeseries} = initializeUtilizationMetrics(
        parent_profile, counters_y1, counters_y2,
        max_samples, timeaxis_name);
    expect(cpu_nodes_usage_aggregate).toEqual([
        [counters_y1[0][1], 0, null, null, null],
        [counters_y1[1][1], 0, null, null, null]
    ]);
    expect(read_write_metrics_aggregate).toEqual([
        [counters_y2[0][1], 0, null, null, null]
    ]);
    expect(sampled_utilization_timeseries).toEqual(
        [timeaxis_name, null, null, null, null]
    );
    expect(counters_y1[0][2]).toBe(0);
    expect(counters_y1[1][2]).toBe(1);
    expect(counters_y2[0][2]).toBe(2);
  });
});
