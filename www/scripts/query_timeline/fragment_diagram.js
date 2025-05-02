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
    set_diagram_width, DIAGRAM_MIN_HEIGHT,
    MARGIN_HEADER_FOOTER, BORDER_STROKE_WIDTH, MARGIN_CHART_END, clearDOMChildren,
    resizeHorizontalAll} from "./global_members.js";
import {host_utilization_chart, getUtilizationWrapperHeight}
    from "./host_utilization_diagram.js";
import {fragment_metrics_chart, getFragmentMetricsWrapperHeight,
    updateFragmentMetricsChartOnClick} from "./fragment_metrics_diagram.js";
import {showTooltip, hideTooltip} from "./chart_commons.js"
import "./global_dom.js";

export let exportedForTest;

export let name_width;
export let page_additional_height;

const stroke_fill_colors = { black : "#000000", dark_grey : "#505050",
    light_grey : "#F0F0F0", transperent : "rgba(0, 0, 0, 0)" };
const ROW_HEIGHT = 15;
const INTEGER_PART_ESTIMATE = 4;
const CHAR_WIDTH = 6;
const BUCKET_SIZE = 5;

let rownum;
let fragment_events_parse_successful = false;
let decimals_multiplier = Math.pow(10, 2);

// #phases_header
const phases = [
  { color: "#C0C0FF", label: "Prepare" },
  { color: "#E0E0E0", label: "Open" },
  { color: "#FFFFC0", label: "Produce First Batch" },
  { color: "#C0FFFF", label: "Send First Batch" },
  { color: "#C0FFC0", label: "Process Remaining Batches" },
  { color: "#FFC0C0", label: "Close" }
];

// #fragment_diagram
const fragment_colors = ["#A9A9A9", "#FF8C00", "#8A2BE2", "#A52A2A", "#00008B", "#006400",
                       "#228B22", "#4B0082", "#DAA520", "#008B8B", "#000000", "#DC143C"];
const fragment_diagram_title = getSvgTitle("");

let frag_name_width;
let chart_width;
let timestamp_gridline;
let fragments = [];

// #timeticks_footer
export let ntics = 10;

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
  const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
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
  const text_el = document.createElementNS("http://www.w3.org/2000/svg", "text");
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
  const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
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
  const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
  title.textContent = text;
  return title;
}

function getSvgGroup() {
  const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
  return group;
}

function markMinMaxAvg(event) {
  if (event.no_bar !== undefined) return;
  let min = event.ts_list[0];
  let max = min;
  let i = 1;
  for (; i < event.ts_list.length; i++) {
    if (min > event.ts_list[i]) min = event.ts_list[i];
    if (max < event.ts_list[i]) max = event.ts_list[i];
  }
  const EVENT_SPAN_T = max - min;
  event.parts = Array(BUCKET_SIZE).fill(null).map(
      () => ({count : 0, min : Infinity, max : -Infinity, avg : 0}));
  let k;
  let ts;
  let part;
  for (i = 0; i < event.ts_list.length; i++) {
    ts = event.ts_list[i];
    k = ((ts - min) * BUCKET_SIZE / EVENT_SPAN_T) | 0;
    if (k >= BUCKET_SIZE) k = BUCKET_SIZE - 1;
    part = event.parts[k];
    if (ts < part.min) part.min = ts;
    if (ts > part.max) part.max = ts;
    part.avg += ts;
    part.count += 1;
  }
  for (k = 0; k < BUCKET_SIZE; k++) {
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
  const BAR_HEIGHT = row_height - 2;
  const LAST_E_INDEX = events.length - 1;
  const LAST_TS_INDEX = events[LAST_E_INDEX].ts_list.length - 1;
  let plan_node = getSvgGroup();
  plan_node.classList.add("plan_node");
  // coordinates start with (0,0) in the top-left corner
  let y = rownum * row_height; // the y-position for the current phase rectangle
  const cp_y = y; // copy of y, used to bring y-position back to the top(initial value)
  let dx, dy; // dimensions of the current phase rectangle
  let bucketed = false;

  if (events[0].ts_list.length > BUCKET_SIZE) {
    events.map(markMinMaxAvg);
    bucketed = true;
  } else {
    events.map((ev) => {
      ev.ts_list.sort((a, b) => a - b);
    });
  }

  let i;
  let color_idx = LAST_E_INDEX;
  for (i = 0; i <= LAST_E_INDEX; ++i) {
    if (events[i].no_bar) --color_idx;
  }

  let j;
  let dasharray; // current stroke-dasharray
  for (let i = LAST_E_INDEX; i >= 0; --i) {
    y = cp_y; // reuse calculation, returing to the initial y-position
    if (events[i].no_bar === undefined) {
      let l_dx = 0; // previous phase rectangle's width (x-dimension)
      if (bucketed) {
        // Represent the aggregate distribution of instances for each phase
        let part;
        for (j = 0; j < events[i].parts.length; j++) {
          part = events[i].parts[j]; // mantain a reference for reuse
          if (part.count > 0) {
              dy = (part.count / events[i].ts_list.length) * BAR_HEIGHT;
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
        dy = BAR_HEIGHT / events[i].ts_list.length;
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
  if (events[LAST_E_INDEX].no_bar === undefined) {
    // Plan node's top and bottom outlines
    let top_edge_ts, bottom_edge_ts;
    if (bucketed) {
      top_edge_ts = events[LAST_E_INDEX].parts[0].max;
      bottom_edge_ts = events[LAST_E_INDEX].parts[BUCKET_SIZE - 1].max;
    } else {
      top_edge_ts = events[LAST_E_INDEX].ts_list[0];
      bottom_edge_ts = events[LAST_E_INDEX].ts_list[LAST_TS_INDEX];
    }
    plan_node.appendChild(getSvgLine(stroke_fill_colors.black, xoffset, y,
        xoffset + top_edge_ts * px_per_ns, y, false));
    plan_node.appendChild(getSvgLine(stroke_fill_colors.black, xoffset, y + BAR_HEIGHT,
        xoffset +  bottom_edge_ts * px_per_ns, y + BAR_HEIGHT,
        false));
  }

  svg.appendChild(plan_node);
}

async function renderPhases() {
  clearDOMChildren(phases_header);
  let color_idx = 0;
  const PHASE_WIDTH = Math.ceil(chart_width / phases.length);
  phases.forEach((p) => {
    let x = name_width + Math.ceil(chart_width * color_idx / phases.length);
    x = Math.min(x, diagram_width - PHASE_WIDTH);

    phases_header.appendChild(getSvgRect(stroke_fill_colors.black, x, 0, PHASE_WIDTH,
        ROW_HEIGHT));
    phases_header.appendChild(getSvgRect(phases[color_idx++].color, x + 1, 1,
        PHASE_WIDTH - 2, ROW_HEIGHT - 2));
    phases_header.appendChild(getSvgText(p.label, stroke_fill_colors.black, x + PHASE_WIDTH
        / 2, (ROW_HEIGHT + 4) / 2 , ROW_HEIGHT, true,
        Math.min(p.label.length * CHAR_WIDTH, PHASE_WIDTH / 1.5)));
  });
}

async function renderFragmentDiagram() {
  clearDOMChildren(fragment_diagram);
  const PX_PER_NS = chart_width / maxts;
  let rownum_l = 0;
  let pending_children = 0;
  let pending_senders = 0;
  let text_y = ROW_HEIGHT - 4;
  fragment_diagram.appendChild(fragment_diagram_title);
  fragments.forEach(function printFragment(fragment) {
    if (!fragment.printed) {
      fragment.printed = true;

      let pending_fragments = [];
      let fevents = fragment.events;

      let frag_name = fragment.name.replace("Coordinator ", "").replace("Fragment ", "");

      fragment_diagram.appendChild(getSvgText(frag_name, fragment.color, 1, text_y,
          ROW_HEIGHT, false));

      // Fragment/sender timing row
      let fragment_svg_group = getSvgGroup();
      DrawBars(fragment_svg_group, rownum_l, ROW_HEIGHT, fevents, name_width, PX_PER_NS);

      for (let i = 0; i < fragment.nodes.length; ++i) {
        let node = fragment.nodes[i];

        if (node.events !== undefined) {
          // Plan node timing row
          DrawBars(fragment_svg_group, rownum_l, ROW_HEIGHT, node.events, name_width,
              PX_PER_NS);

          if (node.type === "HASH_JOIN_NODE") {
            fragment_diagram.appendChild(getSvgText("X", stroke_fill_colors.black,
                name_width + Math.min.apply(null, node.events[2].ts_list) * PX_PER_NS,
                text_y, ROW_HEIGHT, false));
            fragment_diagram.appendChild(getSvgText("O", stroke_fill_colors.black,
                name_width + Math.min.apply(null, node.events[2].ts_list) * PX_PER_NS,
                text_y, ROW_HEIGHT, false));
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

        let label_x = frag_name_width + CHAR_WIDTH * pending_children;
        let label_width = Math.min(CHAR_WIDTH * node.name.length,
            name_width - label_x - 2);
        fragment_diagram.appendChild(getSvgText(node.name, fragment.color,
            label_x, text_y, ROW_HEIGHT, false));

        if (node.parent_node !== undefined) {
          let y = ROW_HEIGHT * node.parent_node.rendering.rownum;
          if (node.is_sender) {
            let x = name_width + Math.min.apply(null, fevents[3].ts_list) * PX_PER_NS;
            // Dotted horizontal connector to received rows
            fragment_diagram.appendChild(getSvgLine(fragment.color, name_width,
                y + ROW_HEIGHT / 2 - 1, x, y + ROW_HEIGHT / 2 - 1, true));

            // Dotted rectangle for received rows
            let x2 = name_width + Math.max.apply(null, fevents[4].ts_list) * PX_PER_NS;
            fragment_diagram.appendChild(getSvgRect(stroke_fill_colors.transperent,
                x, y + 4, x2 - x, ROW_HEIGHT - 10, "2 2", fragment.color));
          }

          if (node.is_sender && node.parent_node.rendering.rownum !== rownum_l - 1) {
            // DAG edge on right side to distant sender
            let x = name_width - (pending_senders) * CHAR_WIDTH - CHAR_WIDTH / 2;
            fragment_diagram.appendChild(getSvgLine(fragment.color,
                node.parent_node.rendering.label_end,
                y + ROW_HEIGHT / 2, x , y + ROW_HEIGHT / 2, false));
            fragment_diagram.appendChild(getSvgLine(fragment.color, x,
                y + ROW_HEIGHT / 2, x, text_y - ROW_HEIGHT / 2 + 3, false));
            fragment_diagram.appendChild(getSvgLine(fragment.color, x,
                text_y - ROW_HEIGHT / 2 + 3, label_x + label_width,
                text_y - ROW_HEIGHT / 2 + 3, false));

          } else {
            // DAG edge from parent to immediate child
            let x = frag_name_width + (pending_children + 1) * CHAR_WIDTH -
                CHAR_WIDTH / 2;
            fragment_diagram.appendChild(getSvgLine(fragment.color, x, y +
                ROW_HEIGHT - 3, x, text_y - ROW_HEIGHT + 6, false));
          }
        }
        node.rendering = { rownum: rownum_l, label_end: label_x + label_width };
        if (node.num_children) {
          // Scan (leaf) node
          pending_children += (node.num_children - node.is_receiver);
        }
        text_y += ROW_HEIGHT;
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
  fragments.forEach((fragment) => {
    fragment.printed = false;
  });
}

async function renderTimeticks() {
  clearDOMChildren(timeticks_footer);
  let sec_per_tic = maxts / ntics / 1e9;
  let px_per_tic = chart_width / ntics;
  let x = name_width;
  let y = 0;
  let text_y = ROW_HEIGHT - 4;
  let timetick_label;
  for (let i = 1; i <= ntics; ++i) {
    timeticks_footer.appendChild(getSvgRect(stroke_fill_colors.black, x, y, px_per_tic,
        ROW_HEIGHT));
    timeticks_footer.appendChild(getSvgRect(stroke_fill_colors.light_grey, x + 1,
        y + 1, px_per_tic - 2, ROW_HEIGHT - 2));
    timetick_label = (i * sec_per_tic).toFixed(decimals);
    timeticks_footer.appendChild(getSvgText(timetick_label, stroke_fill_colors.black,
        x + px_per_tic - timetick_label.length * CHAR_WIDTH + 2, text_y, ROW_HEIGHT,
        false));
    x += px_per_tic;
  }
}

export function collectFragmentEventsFromProfile() {
  rownum = 0;
  let max_namelen = 0;
  fragments = [];
  const all_nodes = [];
  const receiver_nodes = [];
  let color_idx = 0;
  try {
    // First pass: compute sizes
    const execution_profile = profile.child_profiles[2];
    const EXECUTION_PROFILE_NAME = execution_profile.profile_name.split(" ").slice(0,2)
        .join(" ");
    console.assert(EXECUTION_PROFILE_NAME === "Execution Profile");
    execution_profile.child_profiles.forEach((fp) => {

      if (fp.child_profiles !== undefined &&
          fp.child_profiles[0].event_sequences !== undefined) {
        let cp = fp.child_profiles[0];
        const fevents = fp.child_profiles[0].event_sequences[0].events;

        // Build list of timestamps that spans instances for each event
        for (let en = 0; en < fevents.length; ++en) {
          if (fevents[en].label.includes("AsyncCodegen")) {
            fevents[en].no_bar = true;
            continue;
          }
          fevents[en].ts_list = [ fevents[en].timestamp ];
        }
        for (let instance = 1; instance < fp.child_profiles.length; ++instance) {
          if (fp.child_profiles[instance].event_sequences !== undefined) {
            if (fp.child_profiles[instance].event_sequences[0].events.length ==
                fevents.length) {
              for (let en = 0; en < fevents.length; ++en) {
                if (fevents[en].no_bar) continue;
                fevents[en].ts_list.push(
                    fp.child_profiles[instance].event_sequences[0].events[en].timestamp);
              }
            } else {
              let en = 0, i = 0;
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

        const fragment = {
          name: fp.profile_name,
          events: fevents,
          nodes: [ ],
          color: fragment_colors[color_idx]
        }
        // Pick a new color for each plan fragment
        color_idx = (color_idx + 1) % fragment_colors.length;
        set_maxts(Math.max(maxts, fevents[fevents.length - 1].timestamp));
        max_namelen = Math.max(max_namelen, fp.profile_name.length);
        const node_path = [];
        const node_stack = [];
        let node_name;
        cp.child_profiles.forEach(function get_plan_nodes(pp, index) {
          if (pp.node_metadata === undefined) return;
          node_path.push(index);
          //  pp.profile_name : "AGGREGATION_NODE (id=52)"
          let name_flds = pp.profile_name.split(/[()]/);
          //  name_flds : ["AGGREGATION_NODE ", "id=52", ""]
          let node_type = name_flds[0].trim();
          //  node_type: "AGGREGATION_NODE"
          let node_id = name_flds.length > 1 ?
              parseInt(name_flds[1].split(/[=]/)[1]) : 0;
          //  node_id: 52
          node_name = pp.profile_name.replace("_NODE", "").replace("_", " ")
              .replace("KrpcDataStreamSender", "SENDER")
              .replace("Hash Join Builder", "JOIN BUILD")
              .replace("join node_", "");
          if (node_type.indexOf("SCAN_NODE") >= 0) {
            let table_name = pp.info_strings.find(({ key }) => key === "Table Name")
                .value.split(/[.]/);
            node_name = node_name.replace("SCAN",
                `SCAN [${table_name[table_name.length - 1]}]`);
          }

          const IS_RECEIVER = node_type === "EXCHANGE_NODE" ||
              (node_type === "HASH_JOIN_NODE" && pp.num_children < 3);

          const IS_SENDER = (node_type === "Hash Join Builder" ||
                            node_type === "KrpcDataStreamSender");
          let parent_node;
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

          let node_events;
          if (pp.event_sequences !== undefined) {
            node_events = pp.event_sequences[0].events;

            // Start the instance event list for each event with timestamps
            // from this instance
            for (let en = 0; en < node_events.length; ++en) {
              node_events[en].ts_list = [ node_events[en].timestamp ];
              if ((node_type === "HASH_JOIN_NODE" ||
                  node_type === "NESTED_LOOP_JOIN_NODE") && (en === 1 || en === 2)) {
                node_events[en].no_bar = true;
              }
            }
          }
          let node = {
              name: node_name,
              type: node_type,
              node_id: node_id,
              num_children: 0,
              child_index: 0,
              metadata: pp.node_metadata,
              parent_node: parent_node,
              events: node_events,
              path: node_path.slice(0),
              is_receiver: IS_RECEIVER,
              is_sender: IS_SENDER
          }

          if (IS_SENDER) {
            node.parent_node.sender_frag_index = fragments.length;
          }

          if (IS_RECEIVER) {
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
        for (let ni = 0; ni < fragment.nodes.length; ++ni) {
          const node = fragment.nodes[ni];
          for (let cpn = 1; cpn < fp.child_profiles.length; ++cpn) {
            cp = fp.child_profiles[cpn];

            // Use the saved node path to traverse to the same position in this instance
            for (let pi = 0; pi < node.path.length; ++pi) {
              cp = cp.child_profiles[node.path[pi]];
            }
            console.assert(cp.node_metadata.data_sink_id === undefined ||
                           cp.profile_name.indexOf(`(dst_id=${node.node_id})`));
            console.assert(cp.node_metadata.plan_node_id === undefined ||
                           cp.node_metadata.plan_node_id === node.node_id);
            // Add instance events to this node
            if (cp.event_sequences !== undefined) {
              if (node.events.length === cp.event_sequences[0].events.length) {
                for (let en = 0; en < node.events.length; ++en) {
                  node.events[en].ts_list.push(cp.event_sequences[0].events[en].timestamp);
                }
              } else {
                let i = 0, en = 0;
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
        * CHAR_WIDTH;
    name_width = max_namelen * CHAR_WIDTH + (frag_name_width + 2);
    fragment_events_parse_successful = true;
  } catch(e) {
    fragment_events_parse_successful = false;
    console.log(e);
  }
}

export function setTimingDiagramDimensions(ignored_arg) {
  // phashes header and timeticks footer with margin and border by considering the offset
  page_additional_height = timing_diagram.offsetTop + (ROW_HEIGHT + MARGIN_HEADER_FOOTER
      + BORDER_STROKE_WIDTH * 4) * 2;
  const FRAGMENT_DIAGRAM_INITIAL_HEIGHT = window.innerHeight - page_additional_height;
  const REMAINING_HEIGHT = FRAGMENT_DIAGRAM_INITIAL_HEIGHT - getUtilizationWrapperHeight()
      - getFragmentMetricsWrapperHeight();
  const DISPLAY_HEIGHT = Math.max(DIAGRAM_MIN_HEIGHT, Math.min(REMAINING_HEIGHT,
      rownum * ROW_HEIGHT));
  chart_width = diagram_width - name_width - MARGIN_CHART_END - BORDER_STROKE_WIDTH;

  phases_header.style.height = `${ROW_HEIGHT}px`;
  fragment_diagram.parentElement.style.height = `${DISPLAY_HEIGHT}px`;
  fragment_diagram.style.height = `${Math.max(DIAGRAM_MIN_HEIGHT,
      rownum * ROW_HEIGHT)}px`;
  timeticks_footer.style.height = `${ROW_HEIGHT}px`;

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

fragment_diagram.addEventListener('mouseout', (e) => {
  hideTooltip(host_utilization_chart);
  hideTooltip(fragment_metrics_chart);
  removeChildIfExists(fragment_diagram, timestamp_gridline);
});

fragment_diagram.addEventListener('mousemove', (e) => {
  if (e.clientX + scrollable_screen.scrollLeft >= name_width && e.clientX + scrollable_screen.scrollLeft <= name_width + chart_width){
    removeChildIfExists(fragment_diagram, timestamp_gridline);
    timestamp_gridline = getSvgLine(stroke_fill_colors.black, e.clientX + scrollable_screen.scrollLeft, 0, e.clientX + scrollable_screen.scrollLeft,
        parseInt(fragment_diagram.style.height), false);
    fragment_diagram.appendChild(timestamp_gridline);
    const GRIDLINE_TIME = ((maxts * (e.clientX + scrollable_screen.scrollLeft - name_width) / chart_width) / 1e9);
    showTooltip(host_utilization_chart, GRIDLINE_TIME);
    showTooltip(fragment_metrics_chart, GRIDLINE_TIME);
    fragment_diagram_title.textContent = GRIDLINE_TIME.toFixed(decimals) + " s";
  } else {
    try {
      host_utilization_chart.tooltip.hide();
    } catch (e) {
    }
    removeChildIfExists(fragment_diagram, timestamp_gridline);
    fragment_diagram_title.textContent = "";
  }
});

fragment_diagram.addEventListener('wheel', (e) => {
  if (e.shiftKey) {
    const WINDOW_DIAGRAM_WIDTH = window.innerWidth - BORDER_STROKE_WIDTH;
    if (e.wheelDelta <= 0 && diagram_width <= WINDOW_DIAGRAM_WIDTH) return;
    const NEXT_DIAGRAM_WIDTH = diagram_width + Math.round(e.wheelDelta);
    hor_zoomout.disabled = false;
    if (NEXT_DIAGRAM_WIDTH <= WINDOW_DIAGRAM_WIDTH) {
      NEXT_DIAGRAM_WIDTH = WINDOW_DIAGRAM_WIDTH;
      hor_zoomout.disabled = true;
    }
    const NEXT_CHART_WIDTH = NEXT_DIAGRAM_WIDTH - name_width - MARGIN_CHART_END
        - BORDER_STROKE_WIDTH;
    const NEXT_NTICS = (NEXT_DIAGRAM_WIDTH - diagram_width) * 10 / window.innerWidth;
    NEXT_NTICS = ntics + Math.round(NEXT_NTICS);
    if (NEXT_NTICS < 10) NEXT_NTICS = 10;
    const RENDERING_CONSTRAINT = CHAR_WIDTH * (decimals + INTEGER_PART_ESTIMATE) >=
        NEXT_CHART_WIDTH / NEXT_NTICS;
    if (RENDERING_CONSTRAINT) return;
    ntics = NEXT_NTICS;
    set_diagram_width(NEXT_DIAGRAM_WIDTH);
    resizeHorizontalAll();
  }
});

timeticks_footer.addEventListener('wheel', (e) => {
  if (e.shiftKey) {
    if (e.altKey) {
      if (e.wheelDelta <= 0) {
        if (decimals <= 1) return;
        set_decimals(decimals - 1);
        decimals_multiplier /= 10;
      } else {
        const RENDERING_CONSTRAINT = CHAR_WIDTH * ((decimals + 1) + INTEGER_PART_ESTIMATE)
            >= chart_width / ntics;
        if (RENDERING_CONSTRAINT) return;
        set_decimals(decimals + 1);
        decimals_multiplier *= 10;
      }
    } else {
      if (e.wheelDelta <= 0 && ntics <= 10) return;
      const NEXT_NTICS = ntics + Math.round(e.wheelDelta / 200);
      if (NEXT_NTICS <= 10) return;
      const RENDERING_CONSTRAINT = CHAR_WIDTH * (decimals + INTEGER_PART_ESTIMATE)
          >= chart_width / NEXT_NTICS;
      if (RENDERING_CONSTRAINT) return;
      ntics = NEXT_NTICS;
    }
    renderTimeticks();
  }
});

plan_order.addEventListener('click', renderFragmentDiagram);

if (typeof process !== "undefined" && process.env.NODE_ENV === 'test') {
  exportedForTest = {getSvgRect, getSvgLine, getSvgText, getSvgTitle, getSvgGroup};
}
