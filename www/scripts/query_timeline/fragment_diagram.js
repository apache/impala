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

import {profile, set_maxts, maxts, decimals, set_decimals, diagram_width,
    set_diagram_width, diagram_min_height,
    margin_header_footer, border_stroke_width, margin_chart_end, clearDOMChildren,
    resizeHorizontalAll} from "./global_members.js";
import {host_utilization_chart, getUtilizationWrapperHeight}
    from "./host_utilization_diagram.js";
import {fragment_metrics_chart, getFragmentMetricsWrapperHeight,
    updateFragmentMetricsChartOnClick} from "./fragment_metrics_diagram.js";
import {showTooltip, hideTooltip} from "./chart_commons.js"
import "./global_dom.js";

export var exportedForTest;

export var name_width;
export var page_additional_height;

var stroke_fill_colors = { black : "#000000", dark_grey : "#505050",
    light_grey : "#F0F0F0", transperent : "rgba(0, 0, 0, 0)" };
var rownum;
var row_height = 15;
var integer_part_estimate = 4;
var char_width = 6;
var bucket_size = 5;
var fragment_events_parse_successful = false;
var decimals_multiplier = Math.pow(10, decimals);

// #phases_header
var phases = [
  { color: "#C0C0FF", label: "Prepare" },
  { color: "#E0E0E0", label: "Open" },
  { color: "#FFFFC0", label: "Produce First Batch" },
  { color: "#C0FFFF", label: "Send First Batch" },
  { color: "#C0FFC0", label: "Process Remaining Batches" },
  { color: "#FFC0C0", label: "Close" }
];

// #fragment_diagram
var fragment_colors = ["#A9A9A9", "#FF8C00", "#8A2BE2", "#A52A2A", "#00008B", "#006400",
                       "#228B22", "#4B0082", "#DAA520", "#008B8B", "#000000", "#DC143C"];
var fragments = [];
var fragment_diagram_title = getSvgTitle("");
var all_nodes = [];
var receiver_nodes = [];
var max_namelen = 0;
var frag_name_width;
var chart_width;
var timestamp_gridline;

// #timeticks_footer
export var ntics = 10;

export function set_ntics(val) {
  ntics = val;
}

function removeChildIfExists(parentElement, childElement) {
  try {
    parentElement.removeChild(childElement);
  } catch(e) {
  }
}

function rtd(num) { // round to decimals
  return Math.round(num * decimals_multiplier) / decimals_multiplier;
}

// 'dasharray' and 'stroke_color' are optional parameters
function getSvgRect(fill_color, x, y, width, height, dasharray, stroke_color) {
  var rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  rect.setAttribute("x", rtd(x));
  rect.setAttribute("y", rtd(y));
  rect.setAttribute("width", rtd(width));
  rect.setAttribute("height", rtd(height));
  rect.setAttribute("fill", fill_color);
  rect.setAttribute("stroke-width", 0.5);
  if (stroke_color) {
    rect.setAttribute("stroke", stroke_color);
  }
  if (dasharray) {
    rect.setAttribute("stroke-dasharray", dasharray);
  }
  return rect;
}

// 'max_width' is an optional parameters
function getSvgText(text, fill_color, x, y, height, container_center, max_width) {
  var text_el = document.createElementNS("http://www.w3.org/2000/svg", "text");
  text_el.appendChild(document.createTextNode(text));
  text_el.setAttribute("x", x);
  text_el.setAttribute("y", y);
  text_el.style.fontSize = `${height / 1.5}px`;
  if (container_center) {
    text_el.setAttribute("dominant-baseline", "middle");
    text_el.setAttribute("text-anchor", "middle");
  }
  text_el.setAttribute("fill", fill_color);
  if (max_width) {
    text_el.setAttribute("textLength", max_width);
    text_el.setAttribute("lengthAdjust", "spacingAndGlyphs");
  }
  return text_el;
}

function getSvgLine(stroke_color, x1, y1, x2, y2, dash) {
  var line = document.createElementNS("http://www.w3.org/2000/svg", "line");
  line.setAttribute("x1", rtd(x1));
  line.setAttribute("y1", rtd(y1));
  line.setAttribute("x2", rtd(x2));
  line.setAttribute("y2", rtd(y2));
  line.setAttribute("stroke", stroke_color);
  if (dash) {
    line.setAttribute("stroke-dasharray", "2 2");
  }
  return line;
}

function getSvgTitle(text) {
  var title = document.createElementNS("http://www.w3.org/2000/svg", "title");
  title.textContent = text;
  return title;
}

function getSvgGroup() {
  var group = document.createElementNS("http://www.w3.org/2000/svg", "g");
  return group;
}

function markMinMaxAvg(event) {
  if (event.no_bar !== undefined) return;
  var min = event.ts_list[0];
  var max = min;
  var i = 1;
  for (; i < event.ts_list.length; i++) {
    if (min > event.ts_list[i]) min = event.ts_list[i];
    if (max < event.ts_list[i]) max = event.ts_list[i];
  }
  var event_span_t = max - min;
  event.parts = Array(bucket_size).fill(null).map(
      () => ({count : 0, min : Infinity, max : -Infinity, avg : 0}));
  var k;
  var ts;
  var part;
  for (i = 0; i < event.ts_list.length; i++) {
    ts = event.ts_list[i];
    k = ((ts - min) * bucket_size / event_span_t) | 0;
    if (k >= bucket_size) k = bucket_size - 1;
    part = event.parts[k];
    if (ts < part.min) part.min = ts;
    if (ts > part.max) part.max = ts;
    part.avg += ts;
    part.count += 1;
  }
  for (k = 0; k < bucket_size; k++) {
    event.parts[k].avg /= event.parts[k].count;
  }
}

function getPhaseDasharray(dx, dy, l_dx) {
  return `0 ${rtd(l_dx)} ${rtd(dx - l_dx + dy)} ${rtd(dx)} ${rtd(dy)} 0`;
}

function attachBucketedPhaseTooltip(rect, part) {
  rect.appendChild(getSvgTitle(`# of Inst. : ${part.count}\n`
    + `Min. : ${rtd(part.min / 1e9)}s\n`
    + `Max. : ${rtd(part.max / 1e9)}s\n`
    + `Avg. : ${rtd(part.avg / 1e9)}s`));
  return rect;
}

function DrawBars(svg, rownum, row_height, events, xoffset, px_per_ns) {
  // Structure of each 'events' element -
  // {
  //    no_bar : < Boolean value is set, if event is not to be rendered >
  //    ts_list : < List of this plan node's event's timestamps from all instances >
  //    // A total of 5 buckets represented as 'parts'
  //    parts : [{ // Each object contains statistics of a single timestamps bucket
  //      count : < Number of timestamps in the current bucket >
  //      avg : < Average of timestamps in the current bucket >
  //      min : < Minimum timestamp in the current bucket >
  //      max : < Maximum timestamp in the current bucket >
  //    }, < ... 4 other objects of the same format > ]
  // }
  var bar_height = row_height - 2;
  var last_e_index = events.length - 1;
  var last_ts_index = events[last_e_index].ts_list.length - 1;
  var plan_node = getSvgGroup();
  plan_node.classList.add("plan_node");
  // coordinates start with (0,0) in the top-left corner
  var y = rownum * row_height; // the y-position for the current phase rectangle
  var cp_y = y; // copy of y, used to bring y-position back to the top(initial value)
  var dx, dy; // dimensions of the current phase rectangle
  var bucketed = false;

  if (events[0].ts_list.length > bucket_size) {
    events.map(markMinMaxAvg);
    bucketed = true;
  } else {
    events.map((ev) => {
      ev.ts_list.sort((a, b) => a - b);
    });
  }

  var i;
  var color_idx = last_e_index;
  for (i = 0; i <= last_e_index; ++i) {
    if (events[i].no_bar) --color_idx;
  }

  var j;
  var dasharray; // current stroke-dasharray
  for (var i = last_e_index; i >= 0; --i) {
    y = cp_y; // reuse calculation, returing to the initial y-position
    if (events[i].no_bar === undefined) {
      var l_dx = 0; // previous phase rectangle's width (x-dimension)
      if (bucketed) {
        // Represent the aggregate distribution of instances for each phase
        var part;
        for (j = 0; j < events[i].parts.length; j++) {
          part = events[i].parts[j]; // mantain a reference for reuse
          if (part.count > 0) {
              dy = (part.count / events[i].ts_list.length) * bar_height;
          } else {
            // skip to the next iteration, if the current bucket has no timestamps
            continue;
          }
          dx = part.max * px_per_ns;
          // 'stroke-dasharray' is a string of alternative lengths for
          // dashes and no dashes. So, the length of dasharray of must be even.
          // Width(x-dimension) of previous(upper) phase rectangle is stored to
          // calculate boundaries such that only open boundaries are stroked with borders
          // using the 'getPhaseDasharray' function
          dasharray = getPhaseDasharray(dx, dy, l_dx);
          plan_node.appendChild(attachBucketedPhaseTooltip(getSvgRect(
              phases[color_idx].color, xoffset, y, dx, dy, dasharray,
              stroke_fill_colors.black), part));
          y += dy;
          l_dx = dx;
        }
      } else {
        // Represent phases of each instance seperately, according to the timestamps
        dy = bar_height / events[i].ts_list.length;
        for (j = 0; j < events[i].ts_list.length; j++) {
          dx = events[i].ts_list[j] * px_per_ns;
          // Calculations for the 'stroke-dasharray' have been done same as above
          dasharray = getPhaseDasharray(dx, dy, l_dx);
          plan_node.appendChild(getSvgRect(phases[color_idx].color, xoffset, y, dx, dy,
              dasharray, stroke_fill_colors.black));
          y += dy;
          l_dx = dx;
        }
      }
      --color_idx;
    }
  }

  y = rownum * row_height;
  if (events[last_e_index].no_bar === undefined) {
    // Plan node's top and bottom outlines
    var top_edge_ts, bottom_edge_ts;
    if (bucketed) {
      top_edge_ts = events[last_e_index].parts[0].max;
      bottom_edge_ts = events[last_e_index].parts[bucket_size - 1].max;
    } else {
      top_edge_ts = events[last_e_index].ts_list[0];
      bottom_edge_ts = events[last_e_index].ts_list[last_ts_index];
    }
    plan_node.appendChild(getSvgLine(stroke_fill_colors.black, xoffset, y,
        xoffset + top_edge_ts * px_per_ns, y, false));
    plan_node.appendChild(getSvgLine(stroke_fill_colors.black, xoffset, y + bar_height,
        xoffset +  bottom_edge_ts * px_per_ns, y + bar_height,
        false));
  }

  svg.appendChild(plan_node);
}

async function renderPhases() {
  clearDOMChildren(phases_header);
  var color_idx = 0;
  var width = Math.ceil(chart_width / phases.length);
  phases.forEach(function(p) {
    var x = name_width + Math.ceil(chart_width * color_idx / phases.length);
    x = Math.min(x, diagram_width - width);

    phases_header.appendChild(getSvgRect(stroke_fill_colors.black, x, 0, width,
        row_height));
    phases_header.appendChild(getSvgRect(phases[color_idx++].color, x + 1, 1,
        width - 2, row_height - 2));
    phases_header.appendChild(getSvgText(p.label, stroke_fill_colors.black, x + width
        / 2, (row_height + 4) / 2 , row_height, true,
        Math.min(p.label.length * char_width, width / 1.5)));
  });
}

async function renderFragmentDiagram() {
  clearDOMChildren(fragment_diagram);
  var px_per_ns = chart_width / maxts;
  var rownum_l = 0;
  var pending_children = 0;
  var pending_senders = 0;
  var text_y = row_height - 4;
  fragment_diagram.appendChild(fragment_diagram_title);
  fragments.forEach(function printFragment(fragment) {
    if (!fragment.printed) {
      fragment.printed = true;

      var pending_fragments = [];
      var fevents = fragment.events;

      var frag_name = fragment.name.replace("Coordinator ", "").replace("Fragment ", "");

      fragment_diagram.appendChild(getSvgText(frag_name, fragment.color, 1, text_y,
          row_height, false));

      // Fragment/sender timing row
      var fragment_svg_group = getSvgGroup();
      DrawBars(fragment_svg_group, rownum_l, row_height, fevents, name_width, px_per_ns);

      for (var i = 0; i < fragment.nodes.length; ++i) {
        var node = fragment.nodes[i];

        if (node.events !== undefined) {
          // Plan node timing row
          DrawBars(fragment_svg_group, rownum_l, row_height, node.events, name_width,
              px_per_ns);

          if (node.type === "HASH_JOIN_NODE") {
            fragment_diagram.appendChild(getSvgText("X", stroke_fill_colors.black,
                name_width + Math.min.apply(null, node.events[2].ts_list) * px_per_ns,
                text_y, row_height, false));
            fragment_diagram.appendChild(getSvgText("O", stroke_fill_colors.black,
                name_width + Math.min.apply(null, node.events[2].ts_list) * px_per_ns,
                text_y, row_height, false));
          }
        }

        if (node.is_receiver) {
          pending_senders++;
        } else if (node.is_sender) {
          pending_senders--;
        }
        if (!node.is_sender) {
          pending_children--;
        }

        var label_x = frag_name_width + char_width * pending_children;
        var label_width = Math.min(char_width * node.name.length,
            name_width - label_x - 2);
        fragment_diagram.appendChild(getSvgText(node.name, fragment.color,
            label_x, text_y, row_height, false));

        if (node.parent_node !== undefined) {
          var y = row_height * node.parent_node.rendering.rownum;
          if (node.is_sender) {
            var x = name_width + Math.min.apply(null, fevents[3].ts_list) * px_per_ns;
            // Dotted horizontal connector to received rows
            fragment_diagram.appendChild(getSvgLine(fragment.color, name_width,
                y + row_height / 2 - 1, x, y + row_height / 2 - 1, true));

            // Dotted rectangle for received rows
            var x2 = name_width + Math.max.apply(null, fevents[4].ts_list) * px_per_ns;
            fragment_diagram.appendChild(getSvgRect(stroke_fill_colors.transperent,
                x, y + 4, x2 - x, row_height - 10, "2 2", fragment.color));
          }

          if (node.is_sender && node.parent_node.rendering.rownum !== rownum_l - 1) {
            // DAG edge on right side to distant sender
            var x = name_width - (pending_senders) * char_width - char_width / 2;
            fragment_diagram.appendChild(getSvgLine(fragment.color,
                node.parent_node.rendering.label_end,
                y + row_height / 2, x , y + row_height / 2, false));
            fragment_diagram.appendChild(getSvgLine(fragment.color, x,
                y + row_height / 2, x, text_y - row_height / 2 + 3, false));
            fragment_diagram.appendChild(getSvgLine(fragment.color, x,
                text_y - row_height / 2 + 3, label_x + label_width,
                text_y - row_height / 2 + 3, false));

          } else {
            // DAG edge from parent to immediate child
            var x = frag_name_width + (pending_children + 1) * char_width -
                char_width / 2;
            fragment_diagram.appendChild(getSvgLine(fragment.color, x, y +
                row_height - 3, x, text_y - row_height + 6, false));
          }
        }
        node.rendering = { rownum: rownum_l, label_end: label_x + label_width };
        if (node.num_children) // Scan (leaf) node
          pending_children += (node.num_children - node.is_receiver);
        text_y += row_height;
        rownum_l++;

        if (node.is_receiver) {
          if (plan_order.checked) {
            printFragment(fragments[node.sender_frag_index])
          } else {
            pending_fragments.push(fragments[node.sender_frag_index]);
          }
        }
      }
      fragment_svg_group.id = fragment.name;
      fragment_svg_group.addEventListener('click', updateFragmentMetricsChartOnClick);
      fragment_diagram.appendChild(fragment_svg_group);
      // Visit sender fragments in reverse order to avoid dag edges crossing
      pending_fragments.reverse().forEach(printFragment);
    }
  });
  fragments.forEach(function(fragment) {
    fragment.printed = false;
  });
}

async function renderTimeticks() {
  clearDOMChildren(timeticks_footer);
  var sec_per_tic = maxts / ntics / 1e9;
  var px_per_tic = chart_width / ntics;
  var x = name_width;
  var y = 0;
  var text_y = row_height - 4;
  var timetick_label;
  for (var i = 1; i <= ntics; ++i) {
    timeticks_footer.appendChild(getSvgRect(stroke_fill_colors.black, x, y, px_per_tic,
        row_height));
    timeticks_footer.appendChild(getSvgRect(stroke_fill_colors.light_grey, x + 1,
        y + 1, px_per_tic - 2, row_height - 2));
    timetick_label = (i * sec_per_tic).toFixed(decimals);
    timeticks_footer.appendChild(getSvgText(timetick_label, stroke_fill_colors.black,
        x + px_per_tic - timetick_label.length * char_width + 2, text_y, row_height,
        false));
    x += px_per_tic;
  }
}

export function collectFragmentEventsFromProfile() {
  rownum = 0;
  max_namelen = 0;
  fragments = [];
  all_nodes = [];
  receiver_nodes = [];
  var color_idx = 0;
  try {
    // First pass: compute sizes
    var execution_profile = profile.child_profiles[2];
    var execution_profile_name = execution_profile.profile_name.split(" ").slice(0,2)
        .join(" ");
    console.assert(execution_profile_name === "Execution Profile");
    execution_profile.child_profiles.forEach(function(fp) {

      if (fp.child_profiles !== undefined &&
          fp.child_profiles[0].event_sequences !== undefined) {
        var cp = fp.child_profiles[0];
        var fevents = fp.child_profiles[0].event_sequences[0].events;

        // Build list of timestamps that spans instances for each event
        for (var en = 0; en < fevents.length; ++en) {
          if (fevents[en].label.includes("AsyncCodegen")) {
            fevents[en].no_bar = true;
            continue;
          }
          fevents[en].ts_list = [ fevents[en].timestamp ];
        }
        for (var instance = 1; instance < fp.child_profiles.length; ++instance) {
          if (fp.child_profiles[instance].event_sequences !== undefined) {
            if (fp.child_profiles[instance].event_sequences[0].events.length ==
                fevents.length) {
              for (var en = 0; en < fevents.length; ++en) {
                if (fevents[en].no_bar) continue;
                fevents[en].ts_list.push(
                    fp.child_profiles[instance].event_sequences[0].events[en].timestamp);
              }
            } else {
              var en = 0, i = 0;
              while(i < fevents.length && en < fp.child_profiles[instance]
                  .event_sequences[0].events.length) {
                if (fevents[i].no_bar) {
                  if (fevents[i].label === fp.child_profiles[instance]
                      .event_sequences[0].events[en].label) { en++; }
                } else if (fp.child_profiles[instance].event_sequences[0]
                    .events[en].label === fevents[i].label) {
                  fevents[i].ts_list.push(fp.child_profiles[instance].event_sequences[0]
                      .events[en].timestamp);
                  ++en;
                }
                ++i;
              }
            }
          }
        }

        var fragment = {
          name: fp.profile_name,
          events: fevents,
          nodes: [ ],
          color: fragment_colors[color_idx]
        }
        // Pick a new color for each plan fragment
        color_idx = (color_idx + 1) % fragment_colors.length;
        set_maxts(Math.max(maxts, fevents[fevents.length - 1].timestamp));
        max_namelen = Math.max(max_namelen, fp.profile_name.length);
        var node_path = [];
        var node_stack = [];
        var node_name;
        cp.child_profiles.forEach(function get_plan_nodes(pp, index) {
          if (pp.node_metadata === undefined) return;
          node_path.push(index);
          //  pp.profile_name : "AGGREGATION_NODE (id=52)"
          var name_flds = pp.profile_name.split(/[()]/);
          //  name_flds : ["AGGREGATION_NODE ", "id=52", ""]
          var node_type = name_flds[0].trim();
          //  node_type: "AGGREGATION_NODE"
          var node_id = name_flds.length > 1 ?
              parseInt(name_flds[1].split(/[=]/)[1]) : 0;
          //  node_id: 52
          node_name = pp.profile_name.replace("_NODE", "").replace("_", " ")
              .replace("KrpcDataStreamSender", "SENDER")
              .replace("Hash Join Builder", "JOIN BUILD")
              .replace("join node_", "");
          if (node_type.indexOf("SCAN_NODE") >= 0) {
            var table_name = pp.info_strings.find(({ key }) => key === "Table Name")
                .value.split(/[.]/);
            node_name = node_name.replace("SCAN",
                `SCAN [${table_name[table_name.length - 1]}]`);
          }

          var is_receiver = node_type === "EXCHANGE_NODE" ||
              (node_type === "HASH_JOIN_NODE" && pp.num_children < 3);

          var is_sender = (node_type === "Hash Join Builder" ||
                            node_type === "KrpcDataStreamSender");
          var parent_node;
          if (node_type === "PLAN_ROOT_SINK") {}
          else if (pp.node_metadata.data_sink_id !== undefined) {
            parent_node = receiver_nodes[node_id]; // Exchange sender dst
          } else if (pp.node_metadata.join_build_id !== undefined) {
            parent_node = receiver_nodes[node_id]; // Join sender dst
          } else if (node_stack.length > 0) {
            parent_node = node_stack[node_stack.length - 1];
          } else if (all_nodes.length) {
            parent_node = all_nodes[all_nodes.length - 1];
          }

          max_namelen = Math.max(max_namelen, node_name.length + node_stack.length + 1);

          if (pp.event_sequences !== undefined) {
            var node_events = pp.event_sequences[0].events;

            // Start the instance event list for each event with timestamps
            // from this instance
            for (var en = 0; en < node_events.length; ++en) {
              node_events[en].ts_list = [ node_events[en].timestamp ];
              if ((node_type === "HASH_JOIN_NODE" ||
                  node_type === "NESTED_LOOP_JOIN_NODE") && (en === 1 || en === 2)) {
                node_events[en].no_bar = true;
              }
            }
          }
          var node = {
              name: node_name,
              type: node_type,
              node_id: node_id,
              num_children: 0,
              child_index: 0,
              metadata: pp.node_metadata,
              parent_node: parent_node,
              events: node_events,
              path: node_path.slice(0),
              is_receiver: is_receiver,
              is_sender: is_sender
          }

          if (is_sender) {
            node.parent_node.sender_frag_index = fragments.length;
          }

          if (is_receiver) {
            receiver_nodes[node_id] = node;
          }

          if (parent_node !== undefined) {
            node.child_index = parent_node.num_children++;
          }

          all_nodes.push(node);

          fragment.nodes.push(node);

          if (pp.child_profiles !== undefined) {
            node_stack.push(node);
            pp.child_profiles.forEach(get_plan_nodes);
            node = node_stack.pop();
          }
          rownum++;
          node_path.pop();
        });

        // For each node, retrieve the instance timestamps for the remaining instances
        for (var ni = 0; ni < fragment.nodes.length; ++ni) {
          var node = fragment.nodes[ni];
          for (var cpn = 1; cpn < fp.child_profiles.length; ++cpn) {
            var cp = fp.child_profiles[cpn];

            // Use the saved node path to traverse to the same position in this instance
            for (var pi = 0; pi < node.path.length; ++pi) {
              cp = cp.child_profiles[node.path[pi]];
            }
            console.assert(cp.node_metadata.data_sink_id === undefined ||
                           cp.profile_name.indexOf(`(dst_id=${node.node_id})`));
            console.assert(cp.node_metadata.plan_node_id === undefined ||
                           cp.node_metadata.plan_node_id === node.node_id);
            // Add instance events to this node
            if (cp.event_sequences !== undefined) {
              if (node.events.length === cp.event_sequences[0].events.length) {
                for (var en = 0; en < node.events.length; ++en) {
                  node.events[en].ts_list.push(cp.event_sequences[0].events[en].timestamp);
                }
              } else {
                var i = 0, en = 0;
                while(i < node.events.length && en < cp.event_sequences[0].events.length) {
                  if (node.events[i].label === cp.event_sequences[0].events[en].label) {
                    node.events[i].ts_list.push(cp.event_sequences[0].events[en].timestamp);
                    ++en; ++i;
                  } else {
                    ++i;
                  }
                }
              }
            }
          }
        }
        fragments.push(fragment);
      }
    });
    frag_name_width = (Math.max(2, (fragments.length - 1).toString().length) + 3)
        * char_width;
    name_width = max_namelen * char_width + (frag_name_width + 2);
    fragment_events_parse_successful = true;
  } catch(e) {
    fragment_events_parse_successful = false;
    console.log(e);
  }
}

export function setTimingDiagramDimensions(ignored_arg) {
  // phashes header and timeticks footer with margin and border by considering the offset
  page_additional_height = timing_diagram.offsetTop + (row_height + margin_header_footer
      + border_stroke_width * 4) * 2;
  var fragment_diagram_initial_height = window.innerHeight - page_additional_height;
  var remaining_height = fragment_diagram_initial_height - getUtilizationWrapperHeight()
      - getFragmentMetricsWrapperHeight();
  var display_height = Math.max(diagram_min_height, Math.min(remaining_height,
      rownum * row_height));
  chart_width = diagram_width - name_width - margin_chart_end - border_stroke_width;

  phases_header.style.height = `${row_height}px`;
  fragment_diagram.parentElement.style.height = `${display_height}px`;
  fragment_diagram.style.height = `${Math.max(diagram_min_height,
      rownum * row_height)}px`;
  timeticks_footer.style.height = `${row_height}px`;

  fragment_diagram.parentElement.style.width = `${diagram_width}px`;
  phases_header.parentElement.style.width = `${diagram_width}px`;
  timeticks_footer.parentElement.style.width = `${diagram_width}px`;
  timing_diagram.style.width = `${diagram_width}px`;

  fragment_diagram.style.width = `${diagram_width}px`;
  phases_header.style.width = `${diagram_width}px`;
  timeticks_footer.style.width = `${diagram_width}px`;
}

export function renderTimingDiagram() {
  if (fragment_events_parse_successful) {
    setTimingDiagramDimensions();
    renderPhases();
    renderFragmentDiagram();
    renderTimeticks();
  }
}

fragment_diagram.addEventListener('mouseout', function(e) {
  hideTooltip(host_utilization_chart);
  hideTooltip(fragment_metrics_chart);
  removeChildIfExists(fragment_diagram, timestamp_gridline);
});

fragment_diagram.addEventListener('mousemove', function(e) {
  if (e.clientX + scrollable_screen.scrollLeft >= name_width && e.clientX + scrollable_screen.scrollLeft <= name_width + chart_width){
    removeChildIfExists(fragment_diagram, timestamp_gridline);
    timestamp_gridline = getSvgLine(stroke_fill_colors.black, e.clientX + scrollable_screen.scrollLeft, 0, e.clientX + scrollable_screen.scrollLeft,
        parseInt(fragment_diagram.style.height), false);
    fragment_diagram.appendChild(timestamp_gridline);
    var gridline_time = ((maxts * (e.clientX + scrollable_screen.scrollLeft - name_width) / chart_width) / 1e9);
    showTooltip(host_utilization_chart, gridline_time);
    showTooltip(fragment_metrics_chart, gridline_time);
    fragment_diagram_title.textContent = gridline_time.toFixed(decimals) + " s";
  } else {
    try {
      host_utilization_chart.tooltip.hide();
    } catch (e) {
    }
    removeChildIfExists(fragment_diagram, timestamp_gridline);
    fragment_diagram_title.textContent = "";
  }
});

fragment_diagram.addEventListener('wheel', function(e) {
  if (e.shiftKey) {
    var window_diagram_width = window.innerWidth - border_stroke_width;
    if (e.wheelDelta <= 0 && diagram_width <= window_diagram_width) return;
    var next_diagram_width = diagram_width + Math.round(e.wheelDelta);
    hor_zoomout.disabled = false;
    if (next_diagram_width <= window_diagram_width) {
      next_diagram_width = window_diagram_width;
      hor_zoomout.disabled = true;
    }
    var next_chart_width = next_diagram_width - name_width - margin_chart_end
        - border_stroke_width;
    var next_ntics = (next_diagram_width - diagram_width) * 10 / window.innerWidth;
    next_ntics = ntics + Math.round(next_ntics);
    if (next_ntics < 10) next_ntics = 10;
    var rendering_constraint = char_width * (decimals + integer_part_estimate) >=
        next_chart_width / next_ntics;
    if (rendering_constraint) return;
    ntics = next_ntics;
    set_diagram_width(next_diagram_width);
    resizeHorizontalAll();
  }
});

timeticks_footer.addEventListener('wheel', function(e) {
  if (e.shiftKey) {
    if (e.altKey) {
      if (e.wheelDelta <= 0) {
        if (decimals <= 1) return;
        set_decimals(decimals - 1);
        decimals_multiplier /= 10;
      } else {
        var rendering_constraint = char_width * ((decimals + 1) + integer_part_estimate)
            >= chart_width / ntics;
        if (rendering_constraint) return;
        set_decimals(decimals + 1);
        decimals_multiplier *= 10;
      }
    } else {
      if (e.wheelDelta <= 0 && ntics <= 10) return;
      var next_ntics = ntics + Math.round(e.wheelDelta / 200);
      if (next_ntics <= 10) return;
      var rendering_constraint = char_width * (decimals + integer_part_estimate)
          >= chart_width / next_ntics;
      if (rendering_constraint) return;
      ntics = next_ntics;
    }
    renderTimeticks();
  }
});

plan_order.addEventListener('click', renderFragmentDiagram);

if (typeof process !== "undefined" && process.env.NODE_ENV === 'test') {
  exportedForTest = {getSvgRect, getSvgLine, getSvgText, getSvgTitle, getSvgGroup};
}
