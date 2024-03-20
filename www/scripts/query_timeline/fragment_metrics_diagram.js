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

import {name_width, page_additional_height, setTimingDiagramDimensions}
    from "./fragment_diagram.js"
import {profile, diagram_width, diagram_controls_height, diagram_min_height,
    resizeVerticalAll, margin_chart_end, decimals} from "./global_members.js";
import {aggregateProfileTimeseries, generateTimesamples, clearTimeseriesValues,
    mapTimeseriesCounters, displayWarning, destroyChart} from "./chart_commons.js";
import {resetUtilizationHeight, resizeUtilizationChart, getUtilizationWrapperHeight}
    from "./host_utilization_diagram.js";
import "./global_dom.js";

export var exportedForTest;

export var fragment_id_selections = new Map();
export var fragment_metrics_parse_successful = false;
export var fragment_metrics_chart = null;

var fragment_metrics_aggregate;
var fragment_metrics_counters = [
  ["MemoryUsage", "memory usage", 0],
  ["ThreadUsage", "thread usage", 0]
];
var sampled_fragment_metrics_timeseries;
var fragment_metrics_timeaxis_name = "fragment metrics timeticks";
var max_samples_fragment_metrics = {
  allocated : 1000,
  available : 0,
  collected : 0,
  period : null
};
var fragment_metrics_resize_factor = 0.2;

var fragment_metrics_close_btn = document.getElementById("fragment_metrics_close_btn");
var host_utilization_resize_bar = document.getElementById("fragment_metrics_resize_bar");

function initializeFragmentMetricsChart() {
  fragment_metrics_diagram.style = null;
  fragment_metrics_chart = c3.generate({
    bindto : "#fragment_metrics_diagram",
    data : {
      columns : [[fragment_metrics_timeaxis_name, 0]],
      type : "area",
      order : "asc",
      x : fragment_metrics_timeaxis_name
    }, size : {
      height : getFragmentMetricsHeight(),
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
          format : function (y) { return getReadableSize(y, 1); }
        }
      },
      y2 :
      {
        tick : {
          format : function (y2) { return (y2 == Math.floor(y2) ? y2 : ""); }
        },
        show : true
      }
    }, legend : {
      show : false
    }, tooltip : {
      format : {
        value : function (value, ratio, id, index) {
          if (id.includes("memory usage")){
            return getReadableSize(value, decimals);
          } else {
            return value + " Thread(s)";
          }
        },
        title : function (x, index) {
          return x.toFixed(decimals) + "s";
        }
      }
    }
  });
  fragment_metrics_chart.load({
    unload : true
  });
  var chart_width = diagram_width - margin_chart_end - name_width;
  fragment_metrics_resize_bar.style.marginLeft = `${name_width + chart_width / 4}px`;
  fragment_metrics_resize_bar.style.width = `${chart_width / 2}px`;
  fragment_metrics_resize_bar.style.marginRight = `${chart_width / 4}px`;
}

function dragResizeBar(mousemove_e) {
  if (mousemove_e.target.classList[0] == "c3-event-rect") return;
  var next_height = getFragmentMetricsHeight() + (fragment_metrics_resize_bar.offsetTop -
      mousemove_e.clientY);
  if (next_height >= diagram_min_height &&
      window.innerHeight - next_height - diagram_controls_height >=
      page_additional_height + getUtilizationWrapperHeight() + diagram_min_height) {
    fragment_metrics_resize_factor = next_height / window.innerHeight;
  }
  resizeVerticalAll();
}

function initializeFragmentMetrics(parent_profile, counters, max_samples, timeaxis_name) {
  // user, sys, io and sampled timeticks
  max_samples.available = 0;
  max_samples.period = 0;
  var fragment_metrics_aggregate = new Array(counters.length);
  for (var i = 0; i < counters.length; ++i) {
    fragment_metrics_aggregate[i] = new Array(max_samples.allocated + 3)
        .fill(null);
    fragment_metrics_aggregate[i][1] = 0;
  }
  var sampled_fragment_metrics_timeseries = new Array(max_samples.allocated + 3)
      .fill(null);
  sampled_fragment_metrics_timeseries[0] = timeaxis_name;
  mapTimeseriesCounters(parent_profile.child_profiles[0].time_series_counters, counters);
  return {fragment_metrics_aggregate, sampled_fragment_metrics_timeseries};
}

function getFragmentMetricsHeight() {
  return Math.max(diagram_min_height, window.innerHeight *
      fragment_metrics_resize_factor);
}

export function getFragmentMetricsWrapperHeight() {
  return (fragment_metrics_parse_successful && fragment_id_selections.size > 0)
      * (getFragmentMetricsHeight() + diagram_controls_height);
}

export function toogleFragmentMetricsVisibility() {
  if (fragment_id_selections.size > 0 && fragment_metrics_parse_successful) {
    fragment_metrics_wrapper.style.display = "inline-block";
  } else {
    fragment_metrics_wrapper.style.display = "none";
  }
}

export function collectFragmentMetricsFromProfile() {
  // do not collect metrics, in case a fragment is not selected
  if (fragment_id_selections.size == 0) {
    fragment_metrics_parse_successful = false;
    return;
  }
  // try parsing the metrics from JSON, to seek case of errors in expected format
  // and do not render the chart unless attributes are parsed accurately
  try {
    // "Execution Profile" contains the different fragment's counters and
    // there averaged metrics
    var profile_fragments = profile.child_profiles[2].child_profiles;

    // for all the fragments selected in the execution profile
    profile_fragments.every(function (fragment_profile) {
      // initialize the collection arrays for subsequent collections
      // arrays are overwritten without further re-allocation to reduce memory usage
      if(fragment_id_selections.has(fragment_profile.profile_name)) {
        if (fragment_metrics_chart == null) {
          ({fragment_metrics_aggregate, sampled_fragment_metrics_timeseries} =
              initializeFragmentMetrics(fragment_profile, fragment_metrics_counters,
                  max_samples_fragment_metrics, fragment_metrics_timeaxis_name));
          initializeFragmentMetricsChart();
        }

        // map counters to axes
        var axes_mappings = {};
        for (var i = 0; i < fragment_metrics_counters.length; ++i) {
          fragment_metrics_aggregate[i][0] =
              `${fragment_profile.profile_name} - ${fragment_metrics_counters[i][1]}`;
        }
        axes_mappings[fragment_metrics_aggregate[0][0]] = "y";
        axes_mappings[fragment_metrics_aggregate[1][0]] = "y2";

        // aggregate values from nodes of all fragment's plan nodes
        aggregateProfileTimeseries(fragment_profile, fragment_metrics_aggregate,
            fragment_metrics_counters, max_samples_fragment_metrics);

        // display warnings in case less samples are available without plotting
        if (max_samples_fragment_metrics.available <= 0) {
          var fragment_metrics_samples_message =
              `Warning: Not enough samples for fragment metrics plot.`;
          fragment_metrics_chart = destroyChart(fragment_metrics_chart,
              fragment_metrics_diagram);
          displayWarning(fragment_metrics_diagram, fragment_metrics_samples_message,
              diagram_width, name_width, margin_chart_end);
          fragment_metrics_parse_successful = true;
          return true;
        }

        // generate timestamps for utilization values and decide on last timestamp value
        generateTimesamples(sampled_fragment_metrics_timeseries,
            max_samples_fragment_metrics, true);

        // load fragment metrics to the chart
        fragment_metrics_chart.load({
          columns : [...fragment_metrics_aggregate, sampled_fragment_metrics_timeseries],
          axes : axes_mappings
        });
        // clear utilization value and timestamp samples arrays
        fragment_metrics_aggregate.forEach(function (acc_usage) {
          clearTimeseriesValues(acc_usage, max_samples_fragment_metrics);
        });
      }
      return true;
    });
    fragment_metrics_parse_successful = true;
  } catch (e) {
    fragment_metrics_parse_successful = false;
    fragment_metrics_chart = destroyChart(fragment_metrics_chart,
        fragment_metrics_diagram);
    console.log(e);
  }
}

export async function resizeFragmentMetricsChart() {
  if (fragment_metrics_chart == null) return;
  var chart_width = diagram_width - margin_chart_end - name_width;
  fragment_metrics_resize_bar.style.marginLeft = `${name_width + chart_width / 4}px`;
  fragment_metrics_resize_bar.style.width = `${chart_width / 2}px`;
  fragment_metrics_resize_bar.style.marginRight = `${chart_width / 4}px`;
  fragment_metrics_chart.resize({
    height : getFragmentMetricsHeight(),
    width : diagram_width
  });
}

export function updateFragmentMetricsChartOnClick(click_event) {
  var selected_fragment_id = click_event.target.parentElement.parentElement.id;
  fragment_id_selections.delete(selected_fragment_id);
  if (fragment_metrics_chart != null) {
    var unloaded = fragment_metrics_chart.internal.getTargets().every(function (target) {
      if (target.id.includes(selected_fragment_id)) {
        var unload_ids = new Array(fragment_metrics_counters.length);
        for (var i = 0; i < fragment_metrics_counters.length; i++) {
          unload_ids[i] =
              `${selected_fragment_id} - ${fragment_metrics_counters[i][1]}`;
        }
        fragment_metrics_chart.unload({ids : unload_ids});
        return false;
      }
      return true;
    });
    if (fragment_id_selections.size == 0) {
      fragment_metrics_chart = destroyChart(fragment_metrics_chart,
          fragment_metrics_diagram);
      toogleFragmentMetricsVisibility();
      setTimingDiagramDimensions();
      return;
    }
    if (!unloaded) return;
  }
  if(fragment_id_selections.size == 0) {
    resetUtilizationHeight();
  }
  fragment_id_selections.set(selected_fragment_id, 1);
  collectFragmentMetricsFromProfile();
  resizeUtilizationChart();
  setTimingDiagramDimensions();
  toogleFragmentMetricsVisibility();
}

export function closeFragmentMetricsChart() {
  fragment_id_selections.clear();
  fragment_metrics_chart = destroyChart(fragment_metrics_chart, fragment_metrics_diagram);
  toogleFragmentMetricsVisibility();
  setTimingDiagramDimensions();
}

fragment_metrics_resize_bar.addEventListener('mousedown',
    function dragResizeBarBegin(mousedown_e) {
  fragment_metrics_resize_bar.removeEventListener('mousedown', dragResizeBarBegin);
  document.body.addEventListener('mousemove', dragResizeBar);
  document.body.addEventListener('mouseup', function dragResrizeBarEnd() {
    document.body.removeEventListener('mouseup', dragResrizeBarEnd);
    document.body.removeEventListener('mousemove', dragResizeBar);
    fragment_metrics_resize_bar.addEventListener('mousedown', dragResizeBarBegin);
  });
});

fragment_metrics_close_btn.addEventListener('click', closeFragmentMetricsChart);

fragment_metrics_close_btn.style.height = `${diagram_controls_height}px`;
fragment_metrics_close_btn.style.fontSize = `${diagram_controls_height / 2}px`;

if (typeof process != "undefined" && process.env.NODE_ENV === 'test') {
  exportedForTest = {initializeFragmentMetrics};
}
