/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.common;

import org.apache.drill.common.Utils;

import java.util.HashMap;
import java.util.Map;


/**
 * This class encapsulates handling of the inter-iteration state. The design
 * goal is to avoid any conversion between backend-native types and UDF.
 * Therefore, the expectation is that
 * all "template" parameters are passed as UDF arguments,
 * whereas non-template arguments are provided in an argument table. Here,
 * "template" arguments are those parameters that cannot be SQL parameters,
 * such as table and column names.

 This class assumes a transition state always has its status indicator
 in the last element. 0 means in progress, 1 means completed, > 1 means
 abnormal. Perhaps a UDF should be added for extracting the status.

 The inter-state iteration table contains three columns:
 - <tt>_grouping_cols</tt> - List of columns that are provided as grouping
 arguments
 - <tt>_iteration INTEGER</tt> - The 0-based iteration number
 - <tt>_state <em>this.stateType</em></tt> - The state (after
 iteration \c _interation)
 */

public class GroupIterationController {
  private static final String TEMP_SCHEMA = "dfs.tmp";
  private static final String TEMP_SCHEMA_PREFIX = TEMP_SCHEMA + ".";
  private boolean temporaryTables;
  private boolean verbose;
  private int     iteration;
  private String  grouping_str;
  private Map<String, Object> args = new HashMap<>();

  public GroupIterationController(
    String rel_args, String rel_state,
    String stateType, Map<String, Object> args) {
    this(rel_args, rel_state, stateType,
      true,
      "dfs.tmp",
      false,
      "NULL",
      "_iteration",
      "_state",
      args);
  }

  public GroupIterationController(
    String rel_args, String rel_state,
    String stateType, boolean temporaryTables,
    String schema_madlib, boolean verbose,
    String grouping_str, String col_grp_iteration,
    String col_grp_state, Map<String, Object> args) {
    this.temporaryTables = temporaryTables;
    this.verbose = verbose;
    this.iteration = -1;
    this.grouping_str = grouping_str;
    this.args = args;
    this.args.put("rel_args", temporaryTables? TEMP_SCHEMA_PREFIX + rel_args : rel_args);
    this.args.put("rel_state", temporaryTables? TEMP_SCHEMA_PREFIX + rel_state: rel_state);
    this.args.put("unqualified_rel_state", rel_state);
    this.args.put("schema_madlib", schema_madlib);
    this.args.put("grouping_str", this.grouping_str);
    this.args.put("col_grp_null", Utils.unique_string());
    this.args.put("col_grp_key", Utils.unique_string());
    this.args.put("col_grp_iteration", col_grp_iteration);
    this.args.put("col_grp_state", col_grp_state);
    this.args.put("stateType", stateType);

  }

}
