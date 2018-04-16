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
package org.apache.drill.madlib.linregr.udf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.ArgsValidator;
import org.apache.drill.common.SqlReusableExecutor;
import org.apache.drill.common.Utils;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Drill UDFs for linear regression.
 *
 */
@SuppressWarnings({"WeakerAccess", "unused", "ConstantConditions"})
public class LinearRegressionFunctions {

  private final Map<String, Object> params = Maps.newHashMap();
  private static final String SRC_TBL_ALIAS = "s";
  private static final String TMP_TBL_ALIAS = "tmp";
  private static final String TMP_H_TBL_ALIAS = "htmp";
  private static final String VIEW_ALIAS = "v";
  private String source_table;
  private String out_table;
  private String dependent_varname;
  private String independent_varname;
  private String grouping_cols;
  private Boolean heteroskedasticity_option;
  private String schema_madlib;

  private void validateArgs(
    String schema_madlib,
    String source_table,
    String out_table,
    String dependent_name,
    String independent_name,
    String grouping_cols,
    Boolean heteroskedasticity_option) {

    Utils._assert(!(StringUtils.isBlank(source_table) ||
      source_table.equalsIgnoreCase("null")),
      "Linregr error: Invalid data table name!");
    Utils._assert(ArgsValidator.table_exists(source_table),
      "Linregr error: Data table does not exist!");
    Utils._assert(!ArgsValidator.table_is_empty(source_table),
      "Linregr error: Data table is empty!");

    Utils._assert(!(StringUtils.isBlank(out_table) ||
        out_table.equalsIgnoreCase("null")),
      "Linregr error: Invalid output table name!");

    Utils._assert(!ArgsValidator.table_exists(out_table),
      "Output table name already exists. Drop the table before calling the function.");

    Utils._assert(!(StringUtils.isBlank(dependent_name) ||
        dependent_name.equalsIgnoreCase("null")),
      "Linregr error: Invalid dependent column name!");
    Utils._assert(!(StringUtils.isBlank(independent_name) ||
        independent_name.equalsIgnoreCase("null")),
      "Linregr error: Invalid independent column name!");

    if (!StringUtils.isBlank(grouping_cols)) {
      String[] cols = Utils._string_to_array(grouping_cols);
      Utils._assert(ArgsValidator.columns_exist_in_table(source_table, cols, schema_madlib),
        "Linregr error: Grouping column does not exist!");
      Set<String> grouping_set = Sets.newHashSet(cols);
      Set<String> predefined = Sets.newHashSet(
        "coef", "r2", "std_err", "t_stats",
        "p_values", "condition_no",
        "num_processed", "num_missing_rows_skipped",
        "variance_covariance");
      if (heteroskedasticity_option) {
        predefined.add("bp_stats");
        predefined.add("bp_p_value");
      }
      predefined.retainAll(grouping_set);
      Utils._assert(predefined.isEmpty(), String.format("Linregr error: " +
          "Conflicting " +
        "grouping column name.%nPredefined name(s) %s are not allowed!",
        predefined.toString()));
    }

  }

  /**
   * Setup for training.
   * @param schema_madlib name of schema for output table
   * @param source_table name of input data table
   * @param out_table name of output table
   * @param dependent_varname dependent column name
   * @param independent_varname independent column names
   * @param grouping_cols grouping column names
   * @param heteroskedasticity_option if execute perform heteroskedasticity test or not
   */
  private void trainSetup(
    String schema_madlib,
    String source_table,
    String out_table,
    String dependent_varname,
    String independent_varname,
    String grouping_cols,
    Boolean heteroskedasticity_option) {

    params.clear();
    this.schema_madlib = schema_madlib;
    this.source_table = source_table;
    this.out_table = out_table;
    this.dependent_varname = dependent_varname;
    this.independent_varname = independent_varname;
    this.grouping_cols = grouping_cols;
    this.heteroskedasticity_option = heteroskedasticity_option;

    params.put("source_table", source_table);
    params.put("out_table", out_table);
    params.put("dependent_varname", Utils.add_prefix_in_columns(dependent_varname, SRC_TBL_ALIAS+"."));
    params.put("independent_varname", Utils.add_prefix_in_array(independent_varname, SRC_TBL_ALIAS+"."));
    params.put("grouping_cols", Utils.add_prefix_in_columns(grouping_cols, SRC_TBL_ALIAS+"."));
    params.put("heteroskedasticity_option", heteroskedasticity_option);
    params.put("schema_madlib", schema_madlib);
    params.put("schema_tmp", Utils.SCHEMA_TMP);

    params.put("src_tbl_alias", SRC_TBL_ALIAS);
    params.put("tmp_tbl_alias", TMP_TBL_ALIAS);
    params.put("tmp_h_tbl_alias", TMP_H_TBL_ALIAS);
    params.put("view_alias", VIEW_ALIAS);
  }

  private String train() {
    try {
      LinearRegressionFunctions.this.validateArgs(schema_madlib, source_table, out_table, dependent_varname, independent_varname, grouping_cols, heteroskedasticity_option);


      boolean empty_grouping = StringUtils.isBlank((String)params.get("grouping_cols"));
      String group_str = empty_grouping ? "" : String
        .format(" group by %s ", params.get("grouping_cols"));
      String group_str_sel = empty_grouping ? "" : String
        .format("%s , ", params.get("grouping_cols"));
      String group_str_sel_view = empty_grouping ? "" : String
        .format("%s, ", Utils.add_prefix_in_columns(grouping_cols, VIEW_ALIAS+"."));
      String join_str = empty_grouping ? "," : " JOIN ";
      String using_str = empty_grouping ? "" : String.format("USING (%s)", grouping_cols);
      String temp_lin_rst = Utils.unique_string();
      params.put("temp_lin_rst", temp_lin_rst);
      params.put("group_str", group_str);
      params.put("group_str_sel", group_str_sel);
      params.put("join_str", join_str);
      params.put("using_str", using_str);
      params.put("group_str_sel_view", group_str_sel_view);


      String sql =
        "            CREATE temporary TABLE $schema_tmp.$temp_lin_rst AS\n" +
          "            select $group_str_sel_view " +
          "            $view_alias.r.coef as coef," +
          "            $view_alias.r.r2 as r2," +
          "            $view_alias.r.std_err as std_err, " +
          "            $view_alias.r.t_stats as t_stats, " +
          "            $view_alias.r.p_values as p_values," +
          "            $view_alias.r.condition_no as condition_no, " +
          "            $view_alias.r.num_processed as num_processed, " +
          "            $view_alias.r.vcov as vcov, " +
          "            $view_alias.num_rows as num_rows " +
          "            from(" +
          "            SELECT\n" +
          "                $group_str_sel\n" +
          "                (convert_from(linregr(\n" +
          "                    $dependent_varname,\n" +
          "                    $independent_varname), 'JSON')) r,\n" +
          "                count(*) AS num_rows\n" +
          "            FROM\n" +
          "                $source_table as $src_tbl_alias \n" +
          "            $group_str) $view_alias";

      sql = Utils._format_string_by_map(sql, params);
      if (!SqlReusableExecutor.execute_ddl_sql(sql)) {
        return String.format("Failed to create temp table(%s.%s)", schema_madlib, temp_lin_rst);
      }

      String temp_hsk_rst = "";
      if (heteroskedasticity_option) {
        temp_hsk_rst = Utils.unique_string();
        params.put("temp_hsk_rst", temp_hsk_rst);

        sql = "CREATE temporary TABLE $schema_tmp.$temp_hsk_rst AS\n" +
          "                SELECT\n" +
          "                    $group_str_sel\n" +
          "                    convert_from (heteroskedasticity_test_linregr(\n" +
          "                        $src_tbl_alias.$dependent_varname,\n" +
          "                        $independent_varname,\n" +
          "                        $tmp_tbl_alias.coef), 'JSON') AS hsk_rst\n" +
          "                FROM\n" +
          "                    $source_table $src_tbl_alias $join_str $temp_lin_rst $tmp_tbl_alias $using_str\n" +
          "                $group_str";
        sql = Utils._format_string_by_map(sql, params);
        if (!SqlReusableExecutor.execute_ddl_sql(sql)) {
          return String.format("Failed to create temp table(%s.%s)", schema_madlib, temp_hsk_rst);
        }
      }
      join_str = "";
      using_str = "";
      String bp_stats = "";
      String bp_p_value = "";
      if (heteroskedasticity_option) {
        if (empty_grouping) {
          join_str = String.format(", %s AS %s", temp_hsk_rst, TMP_H_TBL_ALIAS);
        } else {
          join_str = String.format("JOIN %s AS %s", temp_hsk_rst, TMP_H_TBL_ALIAS);
          using_str = String.format("USING (%s)", grouping_cols);
        }
        bp_stats = TMP_H_TBL_ALIAS+".hsk_rst.bp_stats as bp_stats,";
        bp_p_value = TMP_H_TBL_ALIAS+".hsk_rst.bp_p_value as bp_p_value,";
      }
      params.put("join_str", join_str);
      params.put("using_str", using_str);
      params.put("bp_stats", bp_stats);
      params.put("bp_p_value", bp_p_value);
      sql = "CREATE TABLE $schema_madlib.$out_table AS\n" +
        "            SELECT\n" +
        "                $group_str_sel\n" +
        "                $src_tbl_alias.coef,\n" +
        "                $src_tbl_alias.r2,\n" +
        "                $src_tbl_alias.std_err,\n" +
        "                $src_tbl_alias.t_stats,\n" +
        "                $src_tbl_alias.p_values,\n" +
        "                $src_tbl_alias.condition_no,\n" +
        "                $bp_stats\n" +
        "                $bp_p_value\n" +
        "                CASE WHEN $src_tbl_alias.num_processed IS NULL\n" +
        "                    THEN 0\n" +
        "                    ELSE $src_tbl_alias.num_processed\n" +
        "                END AS num_rows_processed,\n" +
        "                CASE WHEN $src_tbl_alias.num_processed IS NULL\n" +
        "                    THEN $src_tbl_alias.num_rows\n" +
        "                    ELSE $src_tbl_alias.num_rows - $src_tbl_alias.num_processed\n" +
        "                END AS num_missing_rows_skipped,\n" +
        "                $src_tbl_alias.vcov as variance_covariance\n" +
        "            FROM\n" +
        "                $temp_lin_rst AS $src_tbl_alias $join_str $using_str";
      sql = Utils._format_string_by_map(sql, params);
      if (!SqlReusableExecutor.execute_ddl_sql(sql)) {
        return String.format("Failed to create output table(%s.%s)", schema_madlib, out_table);
      }

      sql = "select\n" +
        "                sum(num_rows_processed) as num_rows_processed,\n" +
        "                sum(num_missing_rows_skipped) as num_rows_skipped\n" +
        "            from $schema_madlib.$out_table";
      sql = Utils._format_string_by_map(sql, params);
      List<Map<String, String>> numRowsList = SqlReusableExecutor.execute_select_sql(sql);
      if (numRowsList == null || numRowsList.isEmpty()) {
        return Utils._format_string_by_map("Failed to get num_rows from $schema_madlib.$out_table", params);
      }
      Map<String, String> numRows = numRowsList.get(0);
      if (StringUtils.isBlank(numRows.get("num_rows_processed"))) {
        numRows.put("num_rows_processed", "0");
        numRows.put("num_rows_skipped", "0");
      }
      params.putAll(numRows);
      String out_table_summary = Utils.add_postfix(out_table, "_summary");
      params.put("out_table_summary", out_table_summary);
      if (empty_grouping) {
        params.put("grouping_col", "");
      } else {
        params.put("grouping_col", grouping_cols);
      }
      sql = "create table $schema_madlib.$out_table_summary as\n" +
        "                select\n" +
        "                      'linregr'                  as method_\n" +
        "                    , '$source_table'            as source_table\n" +
        "                    , '$out_table'               as out_table\n" +
        "                    , '$dependent_varname'       as dependent_varname\n" +
        "                    , '$independent_varname'     as independent_varname\n" +
        "                    , $num_rows_processed        as num_rows_processed\n" +
        "                    , $num_rows_skipped          as num_missing_rows_skipped\n" +
        "                    , '$grouping_col'              as grouping_col" +
        "                    from (values(1))";
      sql = Utils._format_string_by_map(sql, params);
      if (!SqlReusableExecutor.execute_ddl_sql(sql)) {
        return Utils._format_string_by_map("Failed to create summary table($schema_madlib.$out_table_summary)", params);
      }
      return "success";
    } finally {
      SqlReusableExecutor.close_executor();
    }
  }

  public static String train(
    String schema_madlib,
    String source_table,
    String out_table,
    String dependent_varname,
    String independent_varname,
    String grouping_cols,
    Boolean heteroskedasticity_option) {
    LinearRegressionFunctions linear = new LinearRegressionFunctions();


    linear.trainSetup(schema_madlib, source_table,
      out_table, dependent_varname, independent_varname, grouping_cols,
      heteroskedasticity_option);

    try {
      return linear.train();
    } catch (Exception e) {
      return e.getMessage();
    }
  }

  public static String help(String msg) {
    if (msg == null) {
      msg = "";
    }
    msg = msg.trim().toLowerCase();
    String help;
    if (StringUtils.isBlank(msg)) {
      help = "-----------------------------------------------------------------------\n" +
        "                            SUMMARY\n" +
        "-----------------------------------------------------------------------\n" +
        "Ordinary Least Squares Regression, also called Linear Regression, is a\n" +
        "statistical model used to fit linear models.\n" + "\n" +
        "It models a linear relationship of a scalar dependent variable y to one\n" +
        "or more explanatory independent variables x to build a\n" +
        "model of coefficients.\n" + "\n" +
        "For more details on function usage:\n" +
        "    SELECT linregr_train('usage') from (values(1))\n" + "\n" +
        "For an example on using the function:\n" +
        "    SELECT linregr_train('example') from (values(1))";
    } else if(msg.equals("usage") || msg.equals("help") || msg.equals("?")) {
      help = "        -----------------------------------------------------------------------\n" +
        "                                        USAGE\n" +
        "        -----------------------------------------------------------------------\n" +
        "         SELECT linregr_train(\n" +
        "            source_table,                -- name of input table\n" +
        "            schema,                      -- name of schema of output table\n" +
        "            out_table,                   -- name of output table\n" +
        "            dependent_varname,           -- name of dependent variable\n" +
        "            independent_varname,         -- name of independent variables\n" +
        "            grouping_cols,               -- names of columns to group-by\n" +
        "            heteroskedasticity_option    -- perform heteroskedasticity test?\n" +
        "         );\n" + "\n" +
        "        -----------------------------------------------------------------------\n" +
        "                                        OUTPUT\n" +
        "        -----------------------------------------------------------------------\n" +
        "        The output table ('out_table' above) has the following columns:\n" +
        "             <...>,                                          -- Grouping columns used during training\n" +
        "             'coef'                     DOUBLE PRECISION[],  -- Vector of coefficients\n" +
        "             'r2'                       DOUBLE PRECISION,    -- R-squared coefficient\n" +
        "             'std_err'                  DOUBLE PRECISION[],  -- Standard errors of coefficients\n" +
        "             't_stats'                  DOUBLE PRECISION[],  -- t-stats of the coefficients\n" +
        "             'p_values'                 DOUBLE PRECISION[],  -- p-values of the coefficients\n" +
        "             'condition_no'             INTEGER,             -- The condition number of the covariance matrix.\n" +
        "             'bp_stats'                 DOUBLE PRECISION,    -- The Breush-Pagan statistic of heteroskedacity.\n" +
        "                                                            (if heteroskedasticity_option=TRUE)\n" +
        "             'bp_p_value'               DOUBLE PRECISION,    -- The Breush-Pagan calculated p-value.\n" +
        "                                                            (if heteroskedasticity_option=TRUE)\n" +
        "             'num_rows_processed'       INTEGER,            -- Number of rows that are actually used in each group\n" +
        "             'num_missing_rows_skipped' INTEGER             -- Number of rows that have NULL and are skipped in each group\n" + "\n" +
        "        A summary table named <out_table>_summary is also created at the same time, which has:\n" +
        "            'source_table'              VARCHAR,    -- the data source table name\n" +
        "            'out_table'                 VARCHAR,    -- the output table name\n" +
        "            'dependent_varname'         VARCHAR,    -- the dependent variable\n" +
        "            'independent_varname'       VARCHAR,    -- the independent variable\n" +
        "            'num_rows_processed'        INTEGER,    -- total number of rows that are used\n" +
        "            'num_missing_rows_skipped'  INTEGER     -- total number of rows that are skipped because of NULL values";
    } else if(msg.equals("example") || msg.equals("examples")) {
      help = "        CREATE TABLE houses (id INT, tax INT,\n" +
        "                             bedroom INT, bath FLOAT,\n" +
        "                             price INT, size INT, lot INT);\n" +
        "        insert follow values into the table(houses):\n" +
        "          1 |  590 |       2 |    1 |  50000 |  770 | 22100\n" +
        "          2 | 1050 |       3 |    2 |  85000 | 1410 | 12000\n" +
        "          3 |   20 |       3 |    1 |  22500 | 1060 |  3500\n" +
        "          4 |  870 |       2 |    2 |  90000 | 1300 | 17500\n" +
        "          5 | 1320 |       3 |    2 | 133000 | 1500 | 30000\n" +
        "          6 | 1350 |       2 |    1 |  90500 |  820 | 25700\n" +
        "          7 | 2790 |       3 |  2.5 | 260000 | 2130 | 25000\n" +
        "          8 |  680 |       2 |    1 | 142500 | 1170 | 22000\n" +
        "          9 | 1840 |       3 |    2 | 160000 | 1500 | 19000\n" +
        "         10 | 3680 |       4 |    2 | 240000 | 2790 | 20000\n" +
        "         11 | 1660 |       3 |    1 |  87000 | 1030 | 17500\n" +
        "         12 | 1620 |       3 |    2 | 118600 | 1250 | 20000\n" +
        "         13 | 3100 |       3 |    2 | 140000 | 1760 | 38000\n" +
        "         14 | 2070 |       2 |    3 | 148000 | 1550 | 14000\n" +
        "         15 |  650 |       3 |  1.5 |  65000 | 1450 | 12000\n" +
        "        \\.\n" +
        "\n" +
        "        --  Train a regression model. First, single regression for all data.\n" +
        "        SELECT linregr_train( 'houses',\n" +
        "                         '{schema}', \n" +
        "                         'houses_linregr',\n" +
        "                         'price',\n" +
        "                         'ARRAY[1, tax, bath, size]'\n" +
        "                       );\n" +
        "        -- Generate three output models, one for each value of \"bedroom\".\n" +
        "        SELECT linregr_train('houses',\n" +
        "                                    '{schema}',\n" +
        "                                    'houses_linregr_bedroom',\n" +
        "                                    'price',\n" +
        "                                    'ARRAY[1, tax, bath, size]',\n" +
        "                                    'bedroom'\n" +
        "                                    );\n" +
        "        -- Examine the resulting models.\n" +
        "        SELECT * FROM houses_linregr;\n" +
        "        SELECT * FROM houses_linregr_bedroom;";
    } else {
      help = "No such option. Use linregr_train()";
    }
    return help;
  }

  public static String predict_help(String msg) {
    if (msg == null) {
      msg = "";
    }
    msg = msg.trim().toLowerCase();
    String help = "";
    if (StringUtils.isBlank(msg)) {
      help = "-----------------------------------------------------------------------\n" +
        "                            SUMMARY\n" +
        "-----------------------------------------------------------------------\n" +
        "Prediction Function for Ordinary Least Squares Regression (Linear\n" +
        "Regression), is simple wrapper for dot product function\n" +
        "array_dot.\n" +
        "\n" +
        "For more details on function usage:\n" +
        "    SELECT linregr_predict('usage')\n" +
        "\n" +
        "For an example on using the function:\n" +
        "    SELECT linregr_predict('example')";
    } else if(msg.equals("usage") || msg.equals("help") || msg.equals("?")) {
      help = "-----------------------------------------------------------------------\n" +
        "                                USAGE\n" +
        "-----------------------------------------------------------------------\n" +
        " linregr_predict(\n" +
        "    coef,        -- DOUBLE PRECISION[], Coefficient of logistic regression model\n" +
        "    col_ind_var  -- DOUBLE PRECISION[], Values for the independent variables\n" +
        " )\n" +
        "The lengths of 'coef' array and 'col_ind_var' array above should be equal. For\n" +
        "a small example on using the function:\n" +
        "    SELECT linregr_predict('example')\n" +
        "-----------------------------------------------------------------------\n" +
        "                                OUTPUT\n" +
        "-----------------------------------------------------------------------\n" +
        "The output of the function is a DOUBLE PRECISION value representing the\n" +
        "predicted dependent variable value.";
    } else if (msg.equals("example") || msg.equals("examples")) {
      help = "-- The tables below are obtained from the example in 'linregr_train'.\n" +
        "-- Details can be found by running \"SELECT linregr_train('examples');\"\n" +
        "\n" +
        "SELECT id,\n" +
        "       linregr_predict(coef, ARRAY[1, tax, bath, size]) as pred_value\n" +
        "FROM houses h, houses_linregr m\n" +
        "ORDER BY id;";
    }
    return help;
  }

  @FunctionTemplate(name = "linregr_train",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegTrainAll implements DrillAggFunc {

    @Param VarCharHolder  srcTable;
    @Param VarCharHolder  schemaMadlib;
    @Param VarCharHolder  outTable;
    @Param VarCharHolder  dependentVar;
    @Param VarCharHolder  independentVar;
    @Param VarCharHolder  groupingCols;
    @Param BitHolder      heteroskedasticityOption;

    @Workspace VarCharHolder  w_srcTable;
    @Workspace VarCharHolder  w_schemaMadlib;
    @Workspace VarCharHolder  w_outTable;
    @Workspace VarCharHolder  w_dependentVar;
    @Workspace VarCharHolder  w_independentVar;
    @Workspace VarCharHolder  w_groupingCols;
    @Workspace BitHolder      w_heteroskedasticityOption;

    @Inject DrillBuf  buf_srcTable;
    @Inject DrillBuf  buf_schemaMadlib;
    @Inject DrillBuf  buf_outTable;
    @Inject DrillBuf  buf_dependentVar;
    @Inject DrillBuf  buf_independentVar;
    @Inject DrillBuf  buf_groupingCols;

    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;

    @Override
    public void setup() {
    }

    @Override
    public void add() {

      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(srcTable, w_srcTable, buf_srcTable);
      buf_srcTable = w_srcTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(schemaMadlib, w_schemaMadlib, buf_schemaMadlib);
      buf_schemaMadlib = w_schemaMadlib.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(outTable, w_outTable, buf_outTable);
      buf_outTable = w_outTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(dependentVar, w_dependentVar, buf_dependentVar);
      buf_dependentVar = w_dependentVar.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(independentVar, w_independentVar, buf_independentVar);
      buf_independentVar = w_independentVar.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(groupingCols, w_groupingCols, buf_groupingCols);
      buf_groupingCols = w_groupingCols.buffer;
      w_heteroskedasticityOption.value = heteroskedasticityOption.value;
    }

    @Override
    public void output() {
      // janino require external static method must be "public",
      // then, it requires calling static method with its class name and
      // package name.
      // Are you kidding me?
      String source_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_srcTable);
      String out_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_outTable);
      String dependent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_dependentVar);
      String independent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_independentVar);
      String grouping_cols = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_groupingCols);
      Boolean heteroskedasticity_option = org.apache.drill.madlib.common.DataHolderUtils.extractBoolean
        (w_heteroskedasticityOption);
      String schema_madlib = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_schemaMadlib);

      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.train(
        schema_madlib, source_table,
        out_table, dependent_varname, independent_varname, grouping_cols,
        heteroskedasticity_option);

      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);

      buf = out.buffer;
    }

    @Override
    public void reset() {

    }

  }

  @FunctionTemplate(name = "linregr_train",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegTrain6Args implements DrillAggFunc {

    @Param VarCharHolder  srcTable;
    @Param VarCharHolder  schemaMadlib;
    @Param VarCharHolder  outTable;
    @Param VarCharHolder  dependentVar;
    @Param VarCharHolder  independentVar;
    @Param VarCharHolder  groupingCols;

    @Workspace VarCharHolder  w_srcTable;
    @Workspace VarCharHolder  w_schemaMadlib;
    @Workspace VarCharHolder  w_outTable;
    @Workspace VarCharHolder  w_dependentVar;
    @Workspace VarCharHolder  w_independentVar;
    @Workspace VarCharHolder  w_groupingCols;

    @Inject DrillBuf  buf_srcTable;
    @Inject DrillBuf  buf_schemaMadlib;
    @Inject DrillBuf  buf_outTable;
    @Inject DrillBuf  buf_dependentVar;
    @Inject DrillBuf  buf_independentVar;
    @Inject DrillBuf  buf_groupingCols;

    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(srcTable, w_srcTable, buf_srcTable);
      buf_srcTable = w_srcTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(schemaMadlib, w_schemaMadlib, buf_schemaMadlib);
      buf_schemaMadlib = w_schemaMadlib.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(outTable, w_outTable, buf_outTable);
      buf_outTable = w_outTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(dependentVar, w_dependentVar, buf_dependentVar);
      buf_dependentVar = w_dependentVar.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(independentVar, w_independentVar, buf_independentVar);
      buf_independentVar = w_independentVar.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(groupingCols, w_groupingCols, buf_groupingCols);
      buf_groupingCols = w_groupingCols.buffer;
    }

    @Override
    public void output() {
      // janino require external static method must be "public",
      // then, it requires calling static method with its class name and
      // package name.
      // Are you kidding me?
      String source_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_srcTable);
      String out_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_outTable);
      String dependent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_dependentVar);
      String independent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_independentVar);
      String grouping_cols = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_groupingCols);
      String schema_madlib = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_schemaMadlib);

      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.train(
        schema_madlib, source_table,
        out_table, dependent_varname, independent_varname, grouping_cols,
        false);

      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);

      buf = out.buffer;
    }

    @Override
    public void reset() {

    }
  }

  @FunctionTemplate(name = "linregr_train",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegTrain5Args implements DrillAggFunc {

    @Param VarCharHolder  srcTable;
    @Param VarCharHolder  schemaMadlib;
    @Param VarCharHolder  outTable;
    @Param VarCharHolder  dependentVar;
    @Param VarCharHolder  independentVar;

    @Workspace VarCharHolder  w_srcTable;
    @Workspace VarCharHolder  w_schemaMadlib;
    @Workspace VarCharHolder  w_outTable;
    @Workspace VarCharHolder  w_dependentVar;
    @Workspace VarCharHolder  w_independentVar;

    @Inject DrillBuf  buf_srcTable;
    @Inject DrillBuf  buf_schemaMadlib;
    @Inject DrillBuf  buf_outTable;
    @Inject DrillBuf  buf_dependentVar;
    @Inject DrillBuf  buf_independentVar;



    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;

    @Override
    public void setup() {

    }

    @Override
    public void add() {
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(srcTable, w_srcTable, buf_srcTable);
      buf_srcTable = w_srcTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(schemaMadlib, w_schemaMadlib, buf_schemaMadlib);
      buf_schemaMadlib = w_schemaMadlib.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(outTable, w_outTable, buf_outTable);
      buf_outTable = w_outTable.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(dependentVar, w_dependentVar, buf_dependentVar);
      buf_dependentVar = w_dependentVar.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(independentVar, w_independentVar, buf_independentVar);
      buf_independentVar = w_independentVar.buffer;
    }

    @Override
    public void output() {
      // janino require external static method must be "public",
      // then, it requires calling static method with its class name and
      // package name.
      // Are you kidding me?
      String source_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_srcTable);
      String out_table = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_outTable);
      String dependent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_dependentVar);
      String independent_varname = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_independentVar);
      String schema_madlib = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_schemaMadlib);

      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.train(
        schema_madlib, source_table,
        out_table, dependent_varname, independent_varname, null,
        false);

      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);

      buf = out.buffer;

    }

    @Override
    public void reset() {

    }

  }

  @FunctionTemplate(name = "linregr_train",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegTrainMessage implements DrillAggFunc {

    @Param VarCharHolder  msgVar;

    @Workspace VarCharHolder w_out;
    @Inject DrillBuf buf_out;

    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;

    @Override
    public void setup() {

    }

    @Override
    public void add() {
      String msg = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(msgVar);

      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.help(msg);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf_out, w_out, result);
      buf_out = w_out.buffer;
    }

    @Override
    public void output() {
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(w_out, out, buf);
      buf = out.buffer;
    }

    @Override
    public void reset() {

    }

  }

  @FunctionTemplate(name = "linregr_train",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegTrainEmpty implements DrillSimpleFunc {

    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;
    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.help("");
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);
      buf = out.buffer;
    }
  }


  @FunctionTemplate(name = "linregr_predict",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegPredictMessage implements DrillSimpleFunc {

    @Param VarCharHolder  msgVar;

    @Output VarCharHolder out;
    @Inject DrillBuf buf;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {

      String msg = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(msgVar);

      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.predict_help(msg);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);
      buf = out.buffer;
    }

  }

  @FunctionTemplate(name = "linregr_predict",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class LinearRegPredictEmpty implements DrillSimpleFunc {

    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;
    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String result = org.apache.drill.madlib.linregr.udf.LinearRegressionFunctions.predict_help("");
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);
      buf = out.buffer;
    }
  }

}
