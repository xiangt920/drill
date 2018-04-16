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
package org.apache.drill.common;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.server.rest.QueryWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * validator for some arguments.
 *
 */

public class ArgsValidator {

  public static final Set<String> NUMERIC_TYPES = ImmutableSet.<String>builder()
    .add("tinyint")
    .add("smallint")
    .add("integer")
    .add("int")
    .add("bigint")
    .add("float")
    .add("double")
    .add("float4")
    .add("float8")
    .build();

  public static String unquote_ident(String input_str) {
    // TODO: 2018/2/8 what to do?
    return input_str;

  }

  public static String quote_ident(String input_str) {
    // TODO: 2018/2/8 what to do?
    return input_str;
  }

  public static Pair<String, String> split_table(String tbl) {
    String schema = "";
    String table = "";
    tbl = tbl.trim().toLowerCase();
    if (tbl.contains(".")) {
      int lastIdx = tbl.lastIndexOf(".");

      schema = tbl.substring(0, lastIdx);
      table = tbl.substring(lastIdx+1);
    } else {
      table = tbl.toLowerCase();
    }
    return new ImmutablePair<>(schema, table);
  }

  /**
   * Returns True if the table exists in the database.

   If the table name is not schema qualified then current_schemas() is used.
   The table name is searched in information_schema.tables.

   @param tbl Name of the table. Can be schema qualified. If it is not
   qualified then the current schema is used.
    * @return Returns true if the table exists in the database, or returns false
   */
  public static boolean table_exists(String tbl) {
    String sql = String.format("select 1 from %s limit 1", tbl);
    try {
      SqlExecutor.executeSelectSqlNoRecycle(sql);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   *
   * Returns True if the view exists in the database.
   * @param tbl Name of the table. Can be schema qualified. If it is not
   *            qualified then the current schema is used.
   * @return Returns true if the view exists in the database, or returns false
   */
  public static boolean view_exists(String schema, String tbl) {
    if (StringUtils.isBlank(schema)) {
      Pair<String, String> schema_table = split_table(tbl);
      schema = schema_table.getLeft();
      tbl = schema_table.getRight();
    }
    try {
      return SqlExecutor.getTable(schema, tbl) != null;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns True if the input table has no rows
   * @param tbl table name
   * @return returns True if the input table has no rows, or returns false
   */
  public static boolean table_is_empty(String tbl) {
    String sql = String.format("select 1 as _count from %s limit 1", tbl);
    try {
      QueryWrapper.QueryResult result = SqlExecutor.executeSelectSqlNoRecycle(sql);
      return result.rows.isEmpty();
    } catch (Exception e) {
      return true;
    }
  }

  /**
   * Get all column names in a table.
   * If the table is schema qualified then the appropriate schema is searched.
   * If no schema qualification is provided then the current schema is used.
   * @param tbl table name
   * @param schema_madlib schema of madlib table
   * @return return column names of the table
   */
  public static Set<String> get_cols(String tbl, String schema_madlib) {

    String dot = ".";
    if (StringUtils.isBlank(schema_madlib)) {
      schema_madlib = "";
      dot = "";
    }
    if (StringUtils.contains(tbl, '.')) {
      schema_madlib = "";
      dot = "";
    }
    String sql = "select * FROM %s%s%s limit 1";
    sql = String.format(sql, schema_madlib, dot, tbl);
    QueryWrapper.QueryResult result;
    try {
      result = SqlExecutor.executeSelectSqlNoRecycle(sql);
      return Sets.newHashSet(result.columns);
    } catch (Exception e) {
      return Sets.newHashSet();
    }
  }

  /**
   * Get view columns from server by schema and table name
   * @param schema schema name
   * @param tbl table name
   * @return return the columns and their types
   */
  public static Map<String, String> get_view_cols(String schema, String tbl) {
    if (StringUtils.isBlank(schema)) {
      Pair<String, String> schema_table = split_table(tbl);
      schema = schema_table.getLeft();
      tbl = schema_table.getRight();
    }
    try {
      return SqlExecutor.getViewColumns(schema, tbl);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Does each column exist in the table?
   @param tbl Name of source table
   @param cols Iterable list of column names
   @param schema_madlib Schema in which madlib is installed

   @return
   True if all columns in 'cols' exist in source table else False
   */
  public static boolean columns_exist_in_table(String tbl, String[] cols,
                                               String schema_madlib) {

    Set<String> allCols = get_cols(tbl, schema_madlib);
    for (String col : cols) {
      if (!allCols.contains(col)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Validate input table:
   * <ol>
   *   <li>the table name can't be blank or {@code "null"}</li>
   *   <li>the table must exists in server</li>
   *   <li>the table must be a view if {@code isView} is {@code true}</li>
   *   <li>the table can't be empty if {@code check_empty} is {@code true}</li>
   * </ol>
   * @param tbl name of the input table
   * @param module module name
   * @param isView the table is a view or not
   * @param check_empty check the table is empty or not
   */
  public static void input_tbl_valid(String tbl, String module, boolean isView, boolean check_empty) {
    Utils._assert(!(StringUtils.isBlank(tbl) || tbl.equalsIgnoreCase("null")),
      String.format("%s error: NULL/empty input table name!", module));
    Utils._assert(isView? view_exists("", tbl): table_exists(tbl),
      String.format("%s error: Input table '%s' does not exist", module, tbl));
    if (check_empty) {
      Utils._assert(!table_is_empty(tbl),
        String.format("%s error: Input table '%s' is empty!", module, tbl));
    }
  }

  /**
   * Validate input table:
   * <ol>
   *   <li>the table name can't be blank or {@code "null"}</li>
   *   <li>the table must exists in server</li>
   *   <li>the table must be a view if {@code isView} is {@code true}</li>
   *   <li>the table can't be empty</li>
   * </ol>
   * @param tbl table name with schema
   * @param module module name
   * @param isView the table is a view or not
   */
  public static void input_tbl_valid(String tbl, String module, boolean isView) {
    input_tbl_valid(tbl, module, isView, true);
  }

  /**
   * Validate input table:
   * <ol >
   *   <li>the table name can't be blank or {@code "null"}</li>
   *   <li>the table must exists in server</li>
   *   <li>the table must be a view</li>
   *   <li>the table can't be empty</li>
   * </ol>
   * @param tbl table name with schema
   * @param module module name
   */
  public static void input_tbl_valid(String tbl, String module) {
    input_tbl_valid(tbl, module, false, true);
  }

  /**
   * Validate output table, the table can't be blank or {@code "null"} and must not exists in server.
   * @param tbl name of the output table
   * @param module module name
   */
  public static void output_tbl_valid(String tbl, String module) {
    Utils._assert(!(StringUtils.isBlank(tbl) || tbl.equalsIgnoreCase("null")),
      String.format("%s error: NULL/empty output table name!", module));
    Utils._assert(!table_exists(tbl),
      String.format("%s error: Output table '%s' already exists.\n" +
        "            Drop it before calling the function.", module, tbl));

  }

  /**
   * Get which columns are not present in a given table.
   * @param tbl name of source table
   * @param columns List containing column names
   * @return null if all columns in 'cols' exist in source table else the missing columns
   */
  public static List<String> columns_missing_from_table(String tbl, List<String> columns) {
    if (columns == null || columns.isEmpty()) {
      return null;
    }
    Pair<String, String> schema_table = split_table(tbl);
    Set<String> all_columns = get_cols(schema_table.getRight(), schema_table.getLeft());
    Set<String> tmp_columns = new HashSet<>(columns);
    tmp_columns.removeAll(all_columns);
    return new ArrayList<>(tmp_columns);
  }

  /**
   * Validate columns, these columns must exists in source table.
   * @param tbl name of source table
   * @param columns name of columns
   * @param module module name
   */
  public static void cols_in_tbl_valid(String tbl, List<String> columns, String module) {
    for (String column : columns) {
      Utils._assert(StringUtils.isNotBlank(column),
        String.format("%s error: NULL/empty column name!", module));
    }
    List<String> missing_columns = columns_missing_from_table(tbl, columns);
    if (missing_columns != null && !missing_columns.isEmpty()) {
      throw new IllegalStateException(
        String.format("%s error: Column '%s' does not exist in table '%s'!",
          module,
          missing_columns.get(0),
          tbl));
    }


  }
}
