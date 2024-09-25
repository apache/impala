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

import {profile, diagram_width, resizeVerticalAll, DIAGRAM_MIN_HEIGHT, MARGIN_CHART_END,
    decimals} from "./global_members.js";
import {name_width, page_additional_height, setTimingDiagramDimensions}
    from "./fragment_diagram.js";
import {ARRAY_VALUES_START_INDEX, aggregateProfileTimeseries, generateTimesamples,
    clearTimeseriesValues, mapTimeseriesCounters, displayWarning, destroyChart}
    from "./chart_commons.js";
import {getFragmentMetricsWrapperHeight} from "./fragment_metrics_diagram.js";
import "./global_dom.js";

export let exportedForTest;

// #host_utilization_diagram
export let host_utilization_visible = true;
export let host_utilization_chart = null;
export let utilization_metrics_parse_successful = false;

const cpu_utilization_counters = [
    ["HostCpuIoWaitPercentage", "avg io wait", 0],
    ["HostCpuSysPercentage", "avg sys", 0],
    ["HostCpuUserPercentage", "avg user", 0]
];
const read_write_metrics_counters = [
    ["HostDiskReadThroughput", "avg disk read", 0],
    ["HostDiskWriteThroughput", "avg disk write", 0],
    ["HostNetworkRx", "avg network rx", 0],
    ["HostNetworkTx", "avg network tx", 0]
];
const UTILIZATION_TIMEAXIS_NAME = "utilization timeticks";
const MIN_NUM_SAMPLES = 3;
const DIAGRAM_CONTROLS_HEIGHT = Math.max(28, window.innerHeight * 0.025);

let cpu_nodes_usage_aggregate;
let read_write_metrics_aggregate;
let sampled_utilization_timeseries;
let prev_utilization_num_samples = 0;
const max_samples_utilization = {
  allocated : 64,
  available : 0,
  collected : 0,
  period : null
};
let host_utilization_resize_factor = 0.2;

const host_utilization_close_btn = document.getElementById("host_utilization_close_btn");
const host_utilization_resize_bar = document.getElementById("host_utilization_resize_bar");

function initializeUtilizationChart() {
  const axis_mappings = {};
  for(let i = 0; i < cpu_utilization_counters.length; ++i) {
    axis_mappings[cpu_utilization_counters[i][1]] = "y";
  }
  for(let i = 0; i < read_write_metrics_counters.length; ++i) {
    axis_mappings[read_write_metrics_counters[i][1]] = "y2";
  }
  const cpu_utilization_counter_group = new Array(cpu_utilization_counters.length);
  for (let i = 0; i < cpu_utilization_counters.length; i++){
    cpu_utilization_counter_group[i] = cpu_utilization_counters[i][1];
  }
  host_utilization_diagram.style = null;
  host_utilization_chart = c3.generate({
    bindto : "#host_utilization_diagram",
    data : {
      columns : [[UTILIZATION_TIMEAXIS_NAME, 0]],
      axes: axis_mappings,
      type : "area",
      groups : [ cpu_utilization_counter_group ],
      order : "asc",
      x : UTILIZATION_TIMEAXIS_NAME
    }, size : {
      height : getUtilizationHeight(),
      width : diagram_width
    }, padding : {
      left : name_width,
      right : MARGIN_CHART_END
    }, axis : {
      x :
      {
        padding : {
          left : 0,
          right : 0
        },
        tick : {
          format : (x) => x.toFixed(decimals)
        }
      },
      y :
      {
        tick : {
          format : (y) => y + '%'
        }
      },
      y2 :
      {
        tick : {
          format : (y2) => `${getReadableSize(y2, 1)}/s`
        },
        show : true
      }
    }, legend : {
      show : false
    }, tooltip : {
      format : {
        value : (value, ratio, id, index) => {
          if (cpu_utilization_counter_group.includes(id)){
            return value.toFixed(decimals) + '%';
          } else {
            return `${getReadableSize(value, decimals)}/s`;
          }
        },
        title : (x, index) => x.toFixed(decimals) + "s"
      }
    }
  });
  host_utilization_chart.load({
    unload : true
  });
  const CHART_WIDTH = diagram_width - MARGIN_CHART_END - name_width;
  prev_utilization_num_samples = 0;
  host_utilization_resize_bar.style.marginLeft = `${name_width + CHART_WIDTH / 4}px`;
  host_utilization_resize_bar.style.width = `${CHART_WIDTH / 2}px`;
  host_utilization_resize_bar.style.marginRight = `${CHART_WIDTH / 4}px`;
}

function dragResizeBar(mousemove_e) {
  if (mousemove_e.target.classList[0] === "c3-event-rect") return;
  const NEXT_HEIGHT = getUtilizationHeight() + (host_utilization_resize_bar.offsetTop -
      mousemove_e.clientY);
  if (NEXT_HEIGHT >= DIAGRAM_MIN_HEIGHT && window.innerHeight - NEXT_HEIGHT
      - DIAGRAM_CONTROLS_HEIGHT >= page_additional_height
      + getFragmentMetricsWrapperHeight() + DIAGRAM_MIN_HEIGHT) {
    host_utilization_resize_factor = NEXT_HEIGHT / window.innerHeight;
    resizeVerticalAll();
  }
}

function initializeUtilizationMetrics(parent_profile, counters_y1, counters_y2,
    max_samples, timeaxis_name) {
  console.assert(parent_profile.profile_name === "Per Node Profiles");
  // user, sys, io and sampled timeticks
  const cpu_nodes_usage_aggregate = new Array(counters_y1.length);
  max_samples.available = 0;
  max_samples.period = 0;
  for (let i = 0; i < counters_y1.length; ++i) {
    cpu_nodes_usage_aggregate[i] = new Array(max_samples.allocated + 2)
        .fill(null);
    cpu_nodes_usage_aggregate[i][0] = counters_y1[i][1];
    cpu_nodes_usage_aggregate[i][1] = 0;
  }
  const read_write_metrics_aggregate = new Array(counters_y2.length);
  for (let i = 0; i < counters_y2.length; ++i) {
    read_write_metrics_aggregate[i] = new Array(max_samples.allocated + 2)
        .fill(null);
    read_write_metrics_aggregate[i][0] = counters_y2[i][1];
    read_write_metrics_aggregate[i][1] = 0;
  }
  const sampled_utilization_timeseries = new Array(max_samples.allocated + 2)
      .fill(null);
  sampled_utilization_timeseries[0] = timeaxis_name;
  mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters, counters_y1);
  mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters, counters_y2);
  return {cpu_nodes_usage_aggregate, read_write_metrics_aggregate,
      sampled_utilization_timeseries};
}

function getUtilizationHeight() {
  return Math.max(DIAGRAM_MIN_HEIGHT, window.innerHeight *
      host_utilization_resize_factor);
}

export function getUtilizationWrapperHeight() {
  return (utilization_metrics_parse_successful && host_utilization_visible) *
      (getUtilizationHeight() + DIAGRAM_CONTROLS_HEIGHT);
}

export function resetUtilizationHeight() {
  host_utilization_resize_factor = 0.2;
}

export function toogleUtilizationVisibility() {
  if (utilization_metrics_parse_successful && host_utilization_visible) {
    host_utilization_wrapper.style.display = "inline-block";
  } else {
    host_utilization_wrapper.style.display = "none";
  }
}

export async function resizeUtilizationChart() {
  if (host_utilization_chart === null) return;
  const CHART_WIDTH = diagram_width - MARGIN_CHART_END - name_width;
  host_utilization_resize_bar.style.marginLeft = `${name_width + CHART_WIDTH / 4}px`;
  host_utilization_resize_bar.style.width = `${CHART_WIDTH / 2}px`;
  host_utilization_resize_bar.style.marginRight = `${CHART_WIDTH / 4}px`;
  host_utilization_chart.resize({
    height : getUtilizationHeight(),
    width : diagram_width
  });
}

export function collectUtilizationFromProfile() {
  // do not collect metrics, in case host utilization is not visible
  if (!host_utilization_visible){
    utilization_metrics_parse_successful = false;
    return;
  }

  // try parsing the metrics from JSON, to seek case of errors in expected format
  // and do not render the chart unless attributes are parsed accurately
  try {
    // initialize the collection arrays for subsequent collections
    // arrays are overwritten without further re-allocation to reduce memory usage
    const per_node_profiles = profile.child_profiles[2].child_profiles[0];
    console.assert(per_node_profiles.profile_name === "Per Node Profiles");
    if (host_utilization_chart === null) {
      ({cpu_nodes_usage_aggregate, read_write_metrics_aggregate,
          sampled_utilization_timeseries} = initializeUtilizationMetrics(
          per_node_profiles, cpu_utilization_counters, read_write_metrics_counters,
          max_samples_utilization, UTILIZATION_TIMEAXIS_NAME));
      initializeUtilizationChart();
    }
    const impala_server_profile = profile.child_profiles[1];
    console.assert(impala_server_profile.profile_name === "ImpalaServer");
    // Update the plot, only when number of samples in SummaryStatsCounter is updated
    if (impala_server_profile.summary_stats_counters[0].num_of_samples ==
        prev_utilization_num_samples) {
      utilization_metrics_parse_successful = true;
      return;
    }

    // For each node's profile seperately aggregate into two different units of metrics
    aggregateProfileTimeseries(per_node_profiles, cpu_nodes_usage_aggregate,
        cpu_utilization_counters, max_samples_utilization);
    aggregateProfileTimeseries(per_node_profiles, read_write_metrics_aggregate,
        read_write_metrics_counters, max_samples_utilization);

    // display warnings in case less samples are available without plotting
    if (max_samples_utilization.available < MIN_NUM_SAMPLES) {
      const UTILIZATION_SAMPLES_MESSAGE = `Warning: Not enough samples for
          CPU utilization plot. Please decrease the value of starting flag
          variable <b><i>periodic_counter_update_period_ms </b></i> to
          increase the granularity of CPU utilization plot.`;
      host_utilization_chart = destroyChart(host_utilization_chart,
          host_utilization_diagram);
      displayWarning(host_utilization_diagram, UTILIZATION_SAMPLES_MESSAGE,
          diagram_width, name_width, MARGIN_CHART_END);
      utilization_metrics_parse_successful = true;
      return;
    }
    // average the aggregated metrics
    cpu_nodes_usage_aggregate.forEach((acc_usage) => {
      for (let i = ARRAY_VALUES_START_INDEX; i < max_samples_utilization.available
          + ARRAY_VALUES_START_INDEX; ++i) {
        acc_usage[i] = acc_usage[i] / (100 * per_node_profiles.child_profiles.length);
      }
    });
    read_write_metrics_aggregate.forEach((acc_usage) => {
      for (let i = ARRAY_VALUES_START_INDEX; i < max_samples_utilization.available
          + ARRAY_VALUES_START_INDEX; ++i) {
        acc_usage[i] = acc_usage[i] / per_node_profiles.child_profiles.length;
      }
    });
    // generate timestamps for utilization values and decide on last timestamp value
    generateTimesamples(sampled_utilization_timeseries, max_samples_utilization, false);

    // load the utilization values to the chart
    host_utilization_chart.load({
      columns : [...cpu_nodes_usage_aggregate, ...read_write_metrics_aggregate,
          sampled_utilization_timeseries]
    });
    // clear utilization value and timestamp samples arrays
    cpu_nodes_usage_aggregate.forEach((acc_usage) => {
      clearTimeseriesValues(acc_usage, max_samples_utilization);
    });
    read_write_metrics_aggregate.forEach((acc_usage) => {
      clearTimeseriesValues(acc_usage, max_samples_utilization);
    });
    prev_utilization_num_samples = profile.child_profiles[1].summary_stats_counters[0]
        .num_of_samples;
    utilization_metrics_parse_successful = true;
  } catch (e) {
    utilization_metrics_parse_successful = false;
    host_utilization_chart = destroyChart(host_utilization_chart,
        host_utilization_diagram);
    console.log(e);
  }
}

export function destroyUtilizationChart() {
  host_utilization_chart = destroyChart(host_utilization_chart, host_utilization_diagram);
  toogleUtilizationVisibility();
  setTimingDiagramDimensions();
}

host_utilization_resize_bar.addEventListener('mousedown',
    function dragResizeBarBegin(mousedown_e) {
  host_utilization_resize_bar.removeEventListener('mousedown', dragResizeBarBegin);
  document.body.addEventListener('mousemove', dragResizeBar);
  document.body.addEventListener('mouseup', function dragResrizeBarEnd() {
    document.body.removeEventListener('mouseup', dragResrizeBarEnd);
    document.body.removeEventListener('mousemove', dragResizeBar);
    host_utilization_resize_bar.addEventListener('mousedown', dragResizeBarBegin);
  });
});


host_utilization_close_btn.addEventListener('click', (e) => {
  host_utilization_visible = false;
  destroyUtilizationChart();
});

host_utilization_close_btn.style.height = `${DIAGRAM_CONTROLS_HEIGHT}px`;
host_utilization_close_btn.style.fontSize = `${DIAGRAM_CONTROLS_HEIGHT / 2}px`;

if (typeof process !== "undefined" && process.env.NODE_ENV === 'test') {
  exportedForTest = {initializeUtilizationMetrics};
}
