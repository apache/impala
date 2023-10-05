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
import {exportedForTest, generateTimesamples, clearTimeseriesValues,
    mapTimeseriesCounters, aggregateProfileTimeseries} from
    '../../query_timeline/chart_commons.js';

describe("Test mapTimeseriesCounters", () => {
  // Test whether the method correctly searches and maps indexes of counters based
  // on counter_name
  test("Basic Test (Serial Order)", () => {
    var parent_profile =
    {
      "profile_name": "Per Node Profiles",
      "num_children": 3,
      "child_profiles": [
        {
          "profile_name": "host-1:27000",
          "time_series_counters": [{
            "counter_name": "HostCpuIoWaitPercentage",
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
            "counter_name": "HostCpuUserPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        }
      ]
    };
    var counters = [
        ["HostCpuIoWaitPercentage", "avg io wait", 0],
        ["HostCpuSysPercentage", "avg sys", 0],
        ["HostCpuUserPercentage", "avg user", 0]
    ];
    expect(mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters,
        counters)).toBe(undefined);
    for (var i = 0; i < counters.length; i++) {
      expect(counters[i][2]).toBe(i);
    }
  });

  test("Basic Test (Reverse Order)", () => {
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
            "counter_name": "HostCpuIoWaitPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        }
      ]
    };
    var counters = [
        ["HostCpuIoWaitPercentage", "avg io wait", 0],
        ["HostCpuSysPercentage", "avg sys", 0],
        ["HostCpuUserPercentage", "avg user", 0]
    ];
    expect(mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters,
        counters)).toBe(undefined);
    for (var i = 0; i < counters.length; i++) {
      expect(counters[i][2]).toBe(counters.length - i - 1);
    }
  });

  test("Edge Case (No such 'counter_name' within profile)", () => {
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
            "counter_name": "HostCpuIoWaitPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        }
      ]
    };
    var counters = [
        ["HostPercentage", "avg io wait", 0],
        ["HostSysPercenage", "avg sys", 0],
        ["HostUserPercntage", "avg user", 0]
    ];
    try {
      mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters,
          counters);
    } catch(e) {
      expect(e.message).toBe(`"${counters[0][0]}" not found within profile`);
    }
  });
});

describe("Test accumulateTimeseriesValues", () => {
  // Test whether the method correctly accumlates values after parsing values from 'data'
  // in 'time_series_counters' and correctly updates 'max_samples' even in corner cases
  var {accumulateTimeseriesValues} = exportedForTest;
  var data_type = "value type";
  test("Basic Case (time_series_counter.num > max_samples.collected)", () => {
    var max_samples = {
      allocated : 7,
      period : 0,
      available : 0,
      collected : 0
    };
    var values_array = [data_type, 0, 60, 100, 40, 38, 49, 61, 27];
    var time_series_counter = {
      period: 100,
      num: 2000,
      data: "30, 100, 40"
    };

    expect(accumulateTimeseriesValues(values_array, time_series_counter, max_samples))
        .toBe(undefined);

    expect(values_array).toEqual([data_type, 0, 90, 200, 80, 38, 49, 61, 27]);

    expect(max_samples).toEqual({
      allocated : 7,
      period : 100,
      available : 3,
      collected : 2000
    });
  });

  test("Basic Case (time_series_counter.period > max_samples.period", () => {
    var max_samples = {
      allocated : 7,
      period : 100,
      available : 1000,
      collected : 1000
    };
    var values_array = [data_type, 0, 60, 100, 40, 38, 49, 61, 27];
    var time_series_counter = {
      period: 200,
      num: 300,
      data: "30, 100, 40"
    };

    expect(accumulateTimeseriesValues(values_array, time_series_counter, max_samples))
        .toBe(undefined);

    expect(values_array).toEqual([data_type, 0, 90, 200, 80, 38, 49, 61, 27]);

    expect(max_samples).toEqual({
      allocated : 7,
      period : 200,
      available : 3,
      collected : 300
    });
  });

  test(`Basic Case (time_series_counter.period <= max_samples.period
      && time_series_counter.num <= max_samples.collected)`, () => {
    var max_samples = {
      allocated : 7,
      period : 100,
      available : 1000,
      collected : 1000
    };
    var values_array = [data_type, 0, 60, 100, 40, 38, 49, 61, 27];
    var time_series_counter = {
      period: 100,
      num: 300,
      data: "30, 100, 40"
    };

    expect(accumulateTimeseriesValues(values_array, time_series_counter, max_samples))
        .toBe(undefined);

    expect(values_array).toEqual([data_type, 0, 90, 200, 80, 38, 49, 61, 27]);

    expect(max_samples).toEqual({
      allocated : 7,
      period : 100,
      available : 1000,
      collected : 1000
    });
  });

  test(`Edge Case (values_array length is smaller than collected samples)`, () => {
    var max_samples = {
      allocated : 2,
      period : 100,
      available : 2,
      collected : 1000
    };
    var values_array = [data_type, 0, 60, 100];
    var time_series_counter = {
      period: 100,
      num: 300,
      data: "30, 100, 40"
    };

    expect(accumulateTimeseriesValues(values_array, time_series_counter, max_samples))
        .toBe(undefined);

    expect(values_array).toEqual([data_type, 0, 90, 200]);

    expect(max_samples).toEqual({
      allocated : 2,
      period : 100,
      available : 2,
      collected : 1000
    });
  });
});

describe("Test generateTimesamples", () => {
  // Test whether time sample values generated based on 'max_samples' are correct,
  // even in corner cases, with different 'max_samples' scenarios
  var data_type = "timesample type";
  test("Basic Case (max_samples.allocated > max_samples.available)", () => {
    var max_samples = {
      allocated : 10,
      period : 1000,
      available : 4,
      collected : 10
    };
    var timesamples_array = new Array(max_samples.allocated + 2).fill(null);
    timesamples_array[0] = data_type;

    expect(generateTimesamples(timesamples_array, max_samples)).toBe(undefined);

    expect(timesamples_array).toEqual([data_type, 0, 2.5, 5, 7.5, 10, null, null,
        null, null, null, null]);
  });

  test("Edge Case (max_samples.allocated < max_samples.available)", () => {
    var max_samples = {
      allocated : 10,
      period : 1000,
      available : 20,
      collected : 10
    };
    var timesamples_array = new Array(max_samples.allocated + 2);
    timesamples_array[0] = data_type;

    expect(generateTimesamples(timesamples_array, max_samples)).toBe(undefined);

    expect(timesamples_array).toEqual([data_type, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4,
        4.5, 5]);
  });

  test("Edge Case (max_samples.allocated = max_samples.available)", () => {
    var max_samples = {
      allocated : 10,
      period : 1000,
      available : 10,
      collected : 10
    };
    var timesamples_array = new Array(max_samples.allocated + 2);
    timesamples_array[0] = data_type;

    expect(generateTimesamples(timesamples_array, max_samples)).toBe(undefined);

    expect(timesamples_array).toEqual([data_type, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });
});

describe("Test clearTimeseriesValues", () => {
  // Test whether Timeseries arrays are being properly truncated in the correct range
  var data_type = "value type";
  test("Basic Case (max_samples.available < max_samples.allocated)", () => {
    var max_samples = {
      allocated : 7,
      period : 1000,
      available : 3,
      collected : 10
    };
    var values_array = [data_type, 0, 4, 3, 20, 10, 100, 10];

    expect(clearTimeseriesValues(values_array, max_samples)).toBe(undefined);

    expect(values_array).toEqual([data_type, 0, null, null, null, 10, 100, 10]);
  });

  test("Edge Case (max_samples.available >= max_samples.allocated)", () => {
    var max_samples = {
      allocated : 7,
      period : 1000,
      available : 30,
      collected : 10
    };
    var values_array = [data_type, 0, 3, 4, 3, 20, 10, 100, 10];

    expect(clearTimeseriesValues(values_array, max_samples)).toBe(undefined);

    expect(values_array).toEqual([data_type, 0, null, null, null, null, null, null, null]);
  });
});

describe("Test aggregateProfileTimeseries", () => {
  // Test correctness of values being aggregated from parsing the profile
  test("Basic Case", () => {
    var parent_profile =
    {
      "profile_name": "Per Node Profiles",
      "num_children": 3,
      "child_profiles": [
        {
          "profile_name": "host-1:27000",
          "time_series_counters": [{
            "counter_name": "HostCpuIoWaitPercentage",
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
            "counter_name": "HostCpuUserPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        },
        {
          "profile_name": "host-1:27001",
          "time_series_counters": [{
              "counter_name": "HostCpuIoWaitPercentage",
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
              "counter_name": "HostCpuUserPercentage",
              "unit": "BASIS_POINTS",
              "num": 59,
              "period": 100,
              "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        },
        {
          "profile_name": "host-1:27001",
          "time_series_counters": [{
            "counter_name": "HostCpuIoWaitPercentage",
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
            "counter_name": "HostCpuUserPercentage",
            "unit": "BASIS_POINTS",
            "num": 59,
            "period": 100,
            "data": "312,679,445,440,301,301,312,125,125,437"
          }]
        }
      ]
    };
    var max_samples = {
      allocated : 10,
      period : 0,
      available : 0,
      collected : 0
    };
    var counters = [
        ["HostCpuIoWaitPercentage", "avg io wait", 0],
        ["HostCpuSysPercentage", "avg sys", 0],
        ["HostCpuUserPercentage", "avg user", 0]
    ];
    var aggregate_array = new Array(counters.length);
    for (var i = 0; i < counters.length; ++i) {
      aggregate_array[i] = new Array(max_samples.allocated + 2).fill(0);
      aggregate_array[i][0] = counters[i][1];
    }
    mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters,
        counters);

    expect(aggregateProfileTimeseries(parent_profile, aggregate_array, counters,
        max_samples)).toBe(undefined);

    expect(aggregate_array).toEqual([
      ['avg io wait', 0, 0, 0, 0, 210, 0, 0, 0, 0, 0, 30],
      ['avg sys', 0, 936, 2037, 1335,1320, 903, 903, 936, 375, 375, 1311],
      ['avg user', 0, 936, 2037, 1335, 1320, 903, 903, 936, 375, 375, 1311]
    ]);

    expect(max_samples).toEqual({
      allocated : 10,
      period : 100,
      available : 10,
      collected : 59
    });
  });
});
