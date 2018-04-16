/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.correlation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.SqlReusableExecutor;
import org.apache.drill.common.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.common.ArgsValidator.NUMERIC_TYPES;
import static org.apache.drill.common.ArgsValidator.cols_in_tbl_valid;
import static org.apache.drill.common.ArgsValidator.get_cols;
import static org.apache.drill.common.ArgsValidator.get_view_cols;
import static org.apache.drill.common.ArgsValidator.input_tbl_valid;
import static org.apache.drill.common.ArgsValidator.output_tbl_valid;
import static org.apache.drill.common.ArgsValidator.split_table;
import static org.apache.drill.common.SqlReusableExecutor.execute_ddl_sql;
import static org.apache.drill.common.Utils._assert;
import static org.apache.drill.common.Utils._format_string_by_map;
import static org.apache.drill.common.Utils.add_postfix;
import static org.apache.drill.common.Utils.list_to_sql_string;
import static org.apache.drill.common.Utils.unique_string;

/**
 *
 */

public class Correlation {

  private interface FormatFunction<T> {
    String format(T c);
  }

  private static String join_collection(
    Collection<String> clt,
    FormatFunction<String> eleFunc) {
    StringBuilder sb = new StringBuilder();

    for (String e : clt) {
      sb.append(eleFunc.format(e));
      sb.append(",");
    }

    return sb.substring(0, sb.length()-1);
  }

  /**
   * Given a help string, provide usage information
   * @param msg the help string
   * @param cov is covariance or not
   * @return return the usage information
   */
  public static String help(String msg, boolean cov) {
    if (msg == null) {
      msg = "";
    } else {
      msg = msg.trim().toLowerCase();
    }
    Map<String, Object> helpParams = new HashMap<>();
    String func = cov? "covariance": "correlation";
    helpParams.put("func", func);
    String result = "";
    if (StringUtils.isBlank(msg)) {

      if (cov) {
        result = "Covariance is a measure of how much two random variables change together. If the\n" +
          "greater values of one variable mainly correspond with the greater values of the\n" +
          "other variable, and the same holds for the smaller values, i.e., the variables\n" +
          "tend to show similar behavior, the covariance is positive. In the opposite\n" +
          "case, when the greater values of one variable mainly correspond to the smaller\n" +
          "values of the other, i.e., the variables tend to show opposite behavior, the\n" +
          "covariance is negative. The sign of the covariance therefore shows the tendency\n" +
          "-------\n" +
          "For an overview on usage, run:\n" +
          "    SELECT covariance('usage');\n" +
          "-------\n" +
          "For examples:\n" +
          "    SELECT covariance('example');\n" +
          "            ";
      } else {
        result = "A correlation function is the degree and direction of association of\n" +
          "two variables; how well can one random variable be predicted\n" +
          "from the other. The coefficient of correlation varies from -1 to 1:\n" +
          "1 implies perfect correlation, 0 means no correlation, and -1 means\n" +
          "perfectly anti-correlated.\n" +
          "-------\n" +
          "For an overview on usage, run:\n" +
          "    SELECT correlation('usage');\n" +
          "-------\n" +
          "For examples:\n" +
          "    SELECT correlation('example');\n" +
          "            ";
      }
    } else if(msg.equals("usage") || msg.equals("help") || msg.equals("?")) {
      result = "Usage:\n" +
        "-----------------------------------------------------------------------\n" +
        "SELECT $func\n" +
        "(\n" +
        "    source_table TEXT,   -- Source table name (Required)\n" +
        "    schema       TEXT,   -- Schema name of output table\n" +
        "    output_table TEXT,   -- Output table name (Required)\n" +
        "    target_cols  TEXT,   -- Comma separated columns for which summary is desired\n" +
        "                         --   (Default: '*' - produces result for all columns)\n" +
        ")\n" +
        "-----------------------------------------------------------------------\n" +
        "Output will be a table with N+2 columns and N rows, where N is the number\n" +
        "of numeric columns in 'target_cols'.\n" +
        "The columns of the table are described as follows:\n" +
        "\n" +
        "    - column_position   : Position of the variable in the 'source_table'.\n" +
        "    - variable          : Provides the row-header for each variable\n" +
        "    - Rest of the table is the NxN {func} matrix for all numeric columns\n" +
        "    in 'source_table'.\n" +
        "\n" +
        "The output table is arranged as a lower-traingular matrix with the upper\n" +
        "triangle set to NULL. To obtain the result from the output_table in this matrix\n" +
        "format ensure to order the elements using the 'column_position' column.\n" +
        "\n";

    } else if(msg.equals("example") || msg.equals("examples")) {
      result = "\n" +
        "DROP TABLE IF EXISTS example_data;\n" +
        "CREATE TABLE example_data(\n" +
        "    id SERIAL,\n" +
        "    outlook text,\n" +
        "    temperature float8,\n" +
        "    humidity float8,\n" +
        "    windy text,\n" +
        "    class text) ;\n" +
        "\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('sunny', 85, 85, 'false', E'Dont Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('sunny', 80, 90, 'true', E'Dont Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('overcast', 83, 78, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('rain', 70, 96, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('rain', 68, 80, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('rain', 65, 70, 'true', E'Dont Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('overcast', 64, 65, 'true', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('sunny', 72, 95, 'false', E'Dont Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('sunny', 69, 70, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('rain', 75, 80, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('sunny', 75, 70, 'true', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('overcast', 72, 90, 'true', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('overcast', 81, 75, 'false', 'Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES('rain', 71, 80, 'true', E'Dont Play');\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES(NULL, 100, 100, 'true', NULL);\n" +
        "INSERT INTO example_data(outlook, temperature, humidity, windy, class)\n" +
        "VALUES(NULL, 110, 100, 'true', NULL);\n" +
        "\n" +
        "SELECT $func('example_data', 'example_data_output');\n" +
        "SELECT $func('example_data', 'example_data_output', '*');\n" +
        "SELECT $func('example_data', 'example_data_output', 'temperature, humidity');\n" +
        "\n" +
        "-- To get the $func matrix from output table:\n" +
        "SELECT * from example_data_output order by column_position;\n" +
        "         ";
    } else {
      result = "No such option. Use $func()";
    }
    return Utils._format_string_by_map(result, helpParams);
  }

  /**
   * Creates a relation with the appropriate number of columns given a list of
   * column names and populates with the correlation coefficients. If the table
   * already exists, then it is dropped before creating.
   * @param schema schema of output table
   * @param source_table name of source table
   * @param output_table name of output table
   * @param col_names Name of all columns to place in output table
   * @param get_cov If False return the correlation matrix else
   *                return covariance matrix
   * @return return time for computation
   */
  public static long _populate_output_table(
    String schema, String source_table, String output_table,
    List<String> col_names, boolean get_cov) {
    long start = System.currentTimeMillis();
    int col_len = col_names.size();
    String col_names_as_text_array = list_to_sql_string(col_names, true);
    String temp_table= unique_string();
    String function_name = "Correlation";
    String agg_str = "correlation_agg(x, mean)";
    if (get_cov) {
      function_name = "Covariance";
      agg_str = "\n" +
        "                (CASE WHEN count(*) > 0\n" +
        "                      THEN array_scalar_mult(covariance_agg(x, mean),\n" +
        "                                                 1.0 / count(*))\n" +
        "                      ELSE NULL\n" +
        "                END) ";
    }
    final String src_alias = "src";
    final String sub1_alias = "sub1";
    String cols = join_collection(col_names, new FormatFunction<String>() {
      @Override
      public String format(String c) {
        return String.format("coalesce(%s.%s, %s.%s)", src_alias, c, sub1_alias, add_postfix(c, "_avg"));
      }
    });
    String avgs = join_collection(col_names, new FormatFunction<String>() {
      @Override
      public String format(String c) {
        return String.format("avg(%s) as %s", c, add_postfix(c, "_avg"));
      }
    });
    String avg_array = join_collection(col_names, new FormatFunction<String>() {
      @Override
      public String format(String c) {
        return String.format("%s.%s",sub1_alias, add_postfix(c, "_avg"));
      }
    });
    Map<String, Object> params = new HashMap<>();
    params.put("temp_table", temp_table);
    params.put("source_table", source_table);
    params.put("schema", schema);
    params.put("output_table", output_table);
    params.put("agg_str", agg_str);
    params.put("cols", cols);
    params.put("avg_array", avg_array);
    params.put("avgs", avgs);
    params.put("src", src_alias);
    params.put("sub1", sub1_alias);
    params.put("col_names_as_text_array", col_names_as_text_array);
    params.put("function_name", function_name);

    String sql = "\n" +
      "CREATE TEMPORARY TABLE $temp_table AS\n" +
      "with \n" +
      "sub1 as \n" +
      "(SELECT $avgs\n" +
      "        FROM $source_table),\n" +
      "sub2 as\n" +
      "(SELECT ARRAY[ $cols ] AS x,\n" +
      "            ARRAY [ $avg_array ] AS mean\n" +
      "    FROM $source_table $src ,$sub1),\n" +
      "sub3 as\n" +
      "(SELECT\n" +
      "    count(*) AS tot_cnt,\n" +
      "    $agg_str as cor_mat\n" +
      "FROM sub2),\n" +
      "sub4 as\n" +
      "(select mean from sub2 limit 1)\n" +
      "select sub3.tot_cnt, sub4.mean, sub3.cor_mat from sub4,sub3";
    sql = _format_string_by_map(sql, params);
    if(execute_ddl_sql(sql)) {
      String summary_table = add_postfix(output_table, "_summary");
      params.put("summary_table", summary_table);
      sql = "CREATE TABLE $schema.$summary_table AS\n" +
        "            SELECT\n" +
        "                '$function_name'  AS `method`,\n" +
        "                '$source_table'   AS source,\n" +
        "                '$output_table'   AS output_table,\n" +
        "                $col_names_as_text_array   AS column_names,\n" +
        "                mean                        AS mean_vector,\n" +
        "                tot_cnt                     AS total_rows_processed\n" +
        "            FROM $temp_table\n" +
        "            ";
      sql = _format_string_by_map(sql, params);
      if(execute_ddl_sql(sql)) {
        String col_names_table = unique_string();
        params.put("col_names_table", col_names_table);
        sql = "create temporary table $col_names_table as " +
          "select $col_names_as_text_array as variable from (values(1))";
        sql = _format_string_by_map(sql, params);
        execute_ddl_sql(sql);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < col_names.size(); i++) {
          sb.append(", $v2.$flat_cor.val[");
          sb.append(i);
          sb.append("] as ");
          sb.append(col_names.get(i));
          sb.append(" \n");
        }
        params.put("v1", "v1");
        params.put("v2", "v2");
        params.put("flat_cor", "flat_cor");
        params.put("flat_var", "flat_var");
        params.put("col_names_alias", _format_string_by_map(sb.toString(), params));
        String col_list = join_collection(col_names, new FormatFunction<String>() {
          @Override
          public String format(String c) {
            return String.format("matrix_subq.%s", c);
          }
        });
        params.put("col_list", ","+col_list);

        sql = "CREATE TABLE $schema.$output_table AS\n" +
          "SELECT\n" +
          "    variable_subq.column_position,\n" +
          "    variable_subq.variable \n" +
          "    $col_list \n" +
          "FROM\n" +
          "(\n" +
          "    SELECT \n" +
          "        $v1.$flat_var.pos as column_position, \n" +
          "        $v1.$flat_var.val as variable \n" +
          "    FROM (\n" +
          "        SELECT \n" +
          "            flatten(__array_position(variable)) $flat_var \n" +
          "        FROM $col_names_table\n" +
          "    ) $v1\n" +
          ") variable_subq\n" +
          "JOIN\n" +
          "(\n" +
          "    SELECT \n" +
          "        $v2.$flat_cor.pos as column_position\n" +
          "        $col_names_alias \n" +
          "    FROM (SELECT flatten(__array_position(cor_mat)) $flat_cor from $temp_table) $v2\n" +
          ") matrix_subq\n" +
          "USING (column_position)";
        sql = _format_string_by_map(sql, params);
        if(!execute_ddl_sql(sql)) {
          throw new RuntimeException("Can't create output table");
        }

      } else {
        throw new RuntimeException("Can't create summary table");
      }
    } else {
      throw new RuntimeException("Can't create temporary table");
    }

    long end = System.currentTimeMillis();
    return end - start;
  }

  /**
   * Populates an output table with the coefficients of correlation between
   * the columns in a source table.
   * @param source_table Name of input table
   * @param schema  MADlib schema namespace
   * @param output_table Name of output table
   * @param target_cols Name of specific columns targetted for correlation
   * @param get_cov If False return the correlation matrix else
   *                return the covariance matrix
   * @return Tuple (output table name, number of columns, time for computation)
   */
  public static String correlation(
    String source_table, String schema,
    String output_table, String target_cols,
    boolean get_cov) {
    List<String> outputTextList = Lists.newArrayList("");

    try {
      _validate_corr_arg(source_table, schema + "." + output_table);
      Pair<Set<String>, Set<String>> columns = _get_numeric_columns(source_table);
      Set<String> _numeric_column_names = columns.getLeft();
      Set<String> _non_numeric_column_names = columns.getRight();
      List<String> _target_columns = _analyze_target_cols(source_table, target_cols);
      List<String> _existing_target_cols = new ArrayList<>();
      List<String> _nonexisting_target_cols = new ArrayList<>();
      List<String> _nonnumeric_target_cols = new ArrayList<>();
      if (CollectionUtils.isEmpty(_numeric_column_names)) {
        // Unfortunately, unlike PostgreSQL or other relational databases,
        // Drill is a schema-free engine and we may don't know the data type of columns,
        // thus we add all target columns.
        _existing_target_cols.addAll(_target_columns);
      } else {
        for (String column : _target_columns) {
          if (_numeric_column_names.contains(column)) {
            _existing_target_cols.add(column);
          } else if (_non_numeric_column_names.contains(column)) {
            _nonnumeric_target_cols.add(column);
          } else {
            _nonexisting_target_cols.add(column);
          }
        }
      }
      _assert(CollectionUtils.isNotEmpty(_existing_target_cols),
        "Correlation error: No numeric column found in the target list.");
      _assert(_existing_target_cols.size() > 1,
        "Correlation error: Only one numeric column found in the target list.");

      long run_time = _populate_output_table(schema, source_table, output_table, _existing_target_cols, get_cov);

      outputTextList.add("Summary for 'correlation' function");
      outputTextList.add("Output table = " + output_table);

      if (CollectionUtils.isNotEmpty(_nonnumeric_target_cols)) {
        outputTextList.add(String.format(
          "Non-numeric columns ignored: %s",
          _nonnumeric_target_cols.toString()));
      }

      if (CollectionUtils.isNotEmpty(_nonnumeric_target_cols)) {
        outputTextList.add(String.format(
          "Columns that don't exist in '%s' ignored: %s",
          source_table, _nonexisting_target_cols.toString()
        ));
      }
      outputTextList.add(String.format(
        "Producing correlation for columns: %s",
        _existing_target_cols.toString()
      ));
      outputTextList.add("Total run time = " + run_time);
    } catch (Exception e) {
      outputTextList.add(e.getMessage());
    } finally {
      SqlReusableExecutor.close_executor();
    }
    return StringUtils.join(outputTextList, "\n");
  }

  /**
   * Validates all arguments and raises an error if there is an invalid argument.
   * @param source_table Name of input table (string)
   * @param output_table Name of output table (string)
   */
  private static void _validate_corr_arg(String source_table, String output_table) {
    input_tbl_valid(source_table, "Correlation");
    output_tbl_valid(output_table, "Correlation");
    output_tbl_valid(add_postfix(output_table, "_summary"), "Correlation");
  }

  /**
   * Returns all column names for numeric type columns in a table
   * @param source_table table name with schema
   * @return List of column names in table
   */
  private static Pair<Set<String>, Set<String>> _get_numeric_columns(String source_table) {

    final Set<String> numeric_cols = new HashSet<>();
    final Set<String> non_numeric_cols = new HashSet<>();
    Map<String, String> columns = get_view_cols("", source_table);
    if (columns != null) {
      for (Map.Entry<String, String> column : columns.entrySet()) {
        String col = column.getKey();
        String type = column.getValue();
        if (!col.equals("*") && !type.equalsIgnoreCase("any")) {
          if (NUMERIC_TYPES.contains(type.toLowerCase())) {
            numeric_cols.add(col);
          } else {
            non_numeric_cols.add(col);
          }
        }
      }
    }

    return new ImmutablePair<>(numeric_cols, non_numeric_cols);
  }

  private static List<String> _analyze_target_cols(String source_table, String target_cols) {
    if (StringUtils.isBlank(target_cols) || target_cols.trim().equals("*")) {
      Pair<String, String> schema_table = split_table(source_table);

      return new ArrayList<>(get_cols(schema_table.getRight(), schema_table.getLeft()));
    } else {
      List<String> columns = new ArrayList<>(Sets.newHashSet(target_cols.split(",")));
      cols_in_tbl_valid(source_table, columns, "Correlation");
      return columns;
    }
  }

}
