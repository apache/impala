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

import {profile, set_maxts, maxts, diagram_width, diagram_controls_height,
    resizeVerticalAll, diagram_min_height, margin_chart_end, decimals}
    from "./global_members.js";
import {name_width, page_additional_height, setTimingDiagramDimensions}
    from "./fragment_diagram.js";
import {array_values_start_index, aggregateProfileTimeseries, generateTimesamples,
    clearTimeseriesValues, mapTimeseriesCounters, displayWarning, destroyChart}
    from "./chart_commons.js";
import {getFragmentMetricsWrapperHeight} from "./fragment_metrics_diagram.js";
import "./global_dom.js";

export var exportedForTest;

// #host_utilization_diagram
export var host_utilization_visible = true;
export var host_utilization_chart = null;
export var utilization_metrics_parse_successful = false;

var cpu_utilization_counters = [
    ["HostCpuIoWaitPercentage", "avg io wait", 0],
    ["HostCpuSysPercentage", "avg sys", 0],
    ["HostCpuUserPercentage", "avg user", 0]
];
var read_write_metrics_counters = [
    ["HostDiskReadThroughput", "avg disk read", 0],
    ["HostDiskWriteThroughput", "avg disk write", 0],
    ["HostNetworkRx", "avg network rx", 0],
    ["HostNetworkTx", "avg network tx", 0]
];
var cpu_nodes_usage_aggregate;
var read_write_metrics_aggregate;
var sampled_utilization_timeseries;
var utilization_timeaxis_name = "utilization timeticks";
var prev_utilization_num_samples = 0;
var max_samples_utilization = {
  allocated : 64,
  available : 0,
  collected : 0,
  period : null
};
var host_utilization_resize_factor = 0.2;
var min_num_samples = 3;

var host_utilization_close_btn = document.getElementById("host_utilization_close_btn");
var host_utilization_resize_bar = document.getElementById("host_utilization_resize_bar");

function initializeUtilizationChart() {
  var axis_mappings = {};
  for(var i = 0; i < cpu_utilization_counters.length; ++i) {
    axis_mappings[cpu_utilization_counters[i][1]] = "y";
  }
  for(var i = 0; i < read_write_metrics_counters.length; ++i) {
    axis_mappings[read_write_metrics_counters[i][1]] = "y2";
  }
  var cpu_utilization_counter_group = new Array(cpu_utilization_counters.length);
  for (var i = 0; i < cpu_utilization_counters.length; i++){
    cpu_utilization_counter_group[i] = cpu_utilization_counters[i][1];
  }
  host_utilization_diagram.style = null;
  host_utilization_chart = c3.generate({
    bindto : "#host_utilization_diagram",
    data : {
      columns : [[utilization_timeaxis_name, 0]],
      axes: axis_mappings,
      type : "area",
      groups : [ cpu_utilization_counter_group ],
      order : "asc",
      x : utilization_timeaxis_name
    }, size : {
      height : getUtilizationHeight(),
      width : diagram_width
    }, padding : {
      left : name_width,
      right : margin_chart_end
    }, axis : {
      x :
      {
        padding : {
          left : 0,
          right : 0
        },
        tick : {
          format : function (x) { return x.toFixed(decimals); }
        }
      },
      y :
      {
        tick : {
          format : function (y) { return y + '%'; }
        }
      },
      y2 :
      {
        tick : {
          format : function (y2) { return `${getReadableSize(y2, 1)}/s`; }
        },
        show : true
      }
    }, legend : {
      show : false
    }, tooltip : {
      format : {
        value : function (value, ratio, id, index) {
          if (cpu_utilization_counter_group.includes(id)){
            return value.toFixed(decimals) + '%';
          } else {
            return `${getReadableSize(value, decimals)}/s`;
          }
        },
        title : function (x, index) {
          return x.toFixed(decimals) + "s";
        }
      }
    }
  });
  host_utilization_chart.load({
    unload : true
  });
  var chart_width = diagram_width - margin_chart_end - name_width;
  prev_utilization_num_samples = 0;
  host_utilization_resize_bar.style.marginLeft = `${name_width + chart_width / 4}px`;
  host_utilization_resize_bar.style.width = `${chart_width / 2}px`;
  host_utilization_resize_bar.style.marginRight = `${chart_width / 4}px`;
}

function dragResizeBar(mousemove_e) {
  if (mousemove_e.target.classList[0] == "c3-event-rect") return;
  var next_height = getUtilizationHeight() + (host_utilization_resize_bar.offsetTop -
      mousemove_e.clientY);
  if (next_height >= diagram_min_height && window.innerHeight - next_height
      - diagram_controls_height >= page_additional_height
      + getFragmentMetricsWrapperHeight() + diagram_min_height) {
    host_utilization_resize_factor = next_height / window.innerHeight;
    resizeVerticalAll();
  }
}

function initializeUtilizationMetrics(parent_profile, counters_y1, counters_y2,
    max_samples, timeaxis_name) {
  console.assert(parent_profile.profile_name == "Per Node Profiles");
  // user, sys, io and sampled timeticks
  var cpu_nodes_usage_aggregate = new Array(counters_y1.length);
  max_samples.available = 0;
  max_samples.period = 0;
  for (var i = 0; i < counters_y1.length; ++i) {
    cpu_nodes_usage_aggregate[i] = new Array(max_samples.allocated + 2)
        .fill(null);
    cpu_nodes_usage_aggregate[i][0] = counters_y1[i][1];
    cpu_nodes_usage_aggregate[i][1] = 0;
  }
  var read_write_metrics_aggregate = new Array(counters_y2.length);
  for (var i = 0; i < counters_y2.length; ++i) {
    read_write_metrics_aggregate[i] = new Array(max_samples.allocated + 2)
        .fill(null);
    read_write_metrics_aggregate[i][0] = counters_y2[i][1];
    read_write_metrics_aggregate[i][1] = 0;
  }
  var sampled_utilization_timeseries = new Array(max_samples.allocated + 2)
      .fill(null);
  sampled_utilization_timeseries[0] = timeaxis_name;
  mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters, counters_y1);
  mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters, counters_y2);
  return {cpu_nodes_usage_aggregate, read_write_metrics_aggregate,
      sampled_utilization_timeseries};
}

function getUtilizationHeight() {
  return Math.max(diagram_min_height, window.innerHeight *
      host_utilization_resize_factor);
}

export function getUtilizationWrapperHeight() {
  return (utilization_metrics_parse_successful && host_utilization_visible) *
      (getUtilizationHeight() + diagram_controls_height);
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
  if (host_utilization_chart == null) return;
  var chart_width = diagram_width - margin_chart_end - name_width;
  host_utilization_resize_bar.style.marginLeft = `${name_width + chart_width / 4}px`;
  host_utilization_resize_bar.style.width = `${chart_width / 2}px`;
  host_utilization_resize_bar.style.marginRight = `${chart_width / 4}px`;
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
    var per_node_profiles = profile.child_profiles[2].child_profiles[0];
    console.assert(per_node_profiles.profile_name == "Per Node Profiles");
    if (host_utilization_chart == null) {
      ({cpu_nodes_usage_aggregate, read_write_metrics_aggregate,
          sampled_utilization_timeseries} = initializeUtilizationMetrics(
          per_node_profiles, cpu_utilization_counters, read_write_metrics_counters,
          max_samples_utilization, utilization_timeaxis_name));
      initializeUtilizationChart();
    }
    var impala_server_profile = profile.child_profiles[1];
    console.assert(impala_server_profile.profile_name == "ImpalaServer");
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
    if (max_samples_utilization.available < min_num_samples) {
      var utilization_samples_message = `Warning: Not enough samples for
          CPU utilization plot. Please decrease the value of starting flag
          variable <b><i>periodic_counter_update_period_ms </b></i> to
          increase the granularity of CPU utilization plot.`;
      host_utilization_chart = destroyChart(host_utilization_chart,
          host_utilization_diagram);
      displayWarning(host_utilization_diagram, utilization_samples_message,
          diagram_width, name_width, margin_chart_end);
      utilization_metrics_parse_successful = true;
      return;
    }
    // average the aggregated metrics
    cpu_nodes_usage_aggregate.forEach(function (acc_usage) {
      for (var i = array_values_start_index; i < max_samples_utilization.available
          + array_values_start_index; ++i) {
        acc_usage[i] = acc_usage[i] / (100 * per_node_profiles.child_profiles.length);
      }
    });
    read_write_metrics_aggregate.forEach(function (acc_usage) {
      for (var i = array_values_start_index; i < max_samples_utilization.available
          + array_values_start_index; ++i) {
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
    cpu_nodes_usage_aggregate.forEach(function (acc_usage) {
      clearTimeseriesValues(acc_usage, max_samples_utilization);
    });
    read_write_metrics_aggregate.forEach(function (acc_usage) {
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


host_utilization_close_btn.addEventListener('click', function(e) {
  host_utilization_visible = false;
  destroyUtilizationChart();
});

host_utilization_close_btn.style.height = `${diagram_controls_height}px`;
host_utilization_close_btn.style.fontSize = `${diagram_controls_height / 2}px`;

if (typeof process != "undefined" && process.env.NODE_ENV === 'test') {
  exportedForTest = {initializeUtilizationMetrics};
}
