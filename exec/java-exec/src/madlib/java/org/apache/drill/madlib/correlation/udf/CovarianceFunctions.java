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
package org.apache.drill.madlib.correlation.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.correlation.Correlation;

import javax.inject.Inject;

/**
 * UDFs for covariance
 */

@SuppressWarnings("unused")
public class CovarianceFunctions {
  /*
    Compute a covariance matrix for a table with optional target columns specified.
    @param source_table Name of source relation containing the data
    @param output_table Name of output table name to store the covariance
    @param target_cols  String with comma separated list of columns for which cross-covariance is desired
   */
  @FunctionTemplate(name = "covariance", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Covariance3 implements DrillAggFunc {

    @Param
    VarCharHolder source_table_holder;
    @Param
    VarCharHolder madlib_schema_holder;
    @Param
    VarCharHolder output_table_holder;
    @Param
    VarCharHolder target_cols_holder;

    @Workspace
    VarCharHolder w_source_table;
    @Workspace
    VarCharHolder w_madlib_schema;
    @Workspace
    VarCharHolder w_output_table;
    @Workspace
    VarCharHolder w_target_cols;

    @Inject
    DrillBuf buf_source_table;
    @Inject
    DrillBuf buf_madlib_schema;
    @Inject
    DrillBuf buf_output_table;
    @Inject
    DrillBuf buf_target_cols;


    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;
    @Override
    public void setup() {

    }

    @Override
    public void add() {
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(source_table_holder, w_source_table, buf_source_table);
      buf_source_table = w_source_table.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(madlib_schema_holder, w_madlib_schema, buf_madlib_schema);
      buf_madlib_schema = w_madlib_schema.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(output_table_holder, w_output_table, buf_output_table);
      buf_output_table = w_output_table.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(target_cols_holder, w_target_cols, buf_target_cols);
      buf_target_cols = w_target_cols.buffer;
    }

    @Override
    public void output() {
      String sourceTable = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_source_table);
      String madlibSchema = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_madlib_schema);
      String outputTable = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_output_table);
      String targetCols = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_target_cols);
      String result = org.apache.drill.madlib.correlation.Correlation.correlation(
        sourceTable, madlibSchema, outputTable,
        targetCols, true);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);
    }

    @Override
    public void reset() {

    }
  }

  @FunctionTemplate(name = "covariance", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Covariance2 implements DrillAggFunc {

    @Param
    VarCharHolder source_table_holder;
    @Param
    VarCharHolder madlib_schema_holder;
    @Param
    VarCharHolder output_table_holder;

    @Workspace
    VarCharHolder w_source_table;
    @Workspace
    VarCharHolder w_madlib_schema;
    @Workspace
    VarCharHolder w_output_table;

    @Inject
    DrillBuf buf_source_table;
    @Inject
    DrillBuf buf_madlib_schema;
    @Inject
    DrillBuf buf_output_table;


    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;
    @Override
    public void setup() {

    }

    @Override
    public void add() {
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(source_table_holder, w_source_table, buf_source_table);
      buf_source_table = w_source_table.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(madlib_schema_holder, w_madlib_schema, buf_madlib_schema);
      buf_madlib_schema = w_madlib_schema.buffer;
      org.apache.drill.madlib.common.DataHolderUtils.copyVarchar(output_table_holder, w_output_table, buf_output_table);
      buf_output_table = w_output_table.buffer;
    }

    @Override
    public void output() {
      String sourceTable = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_source_table);
      String madlibSchema = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_madlib_schema);
      String outputTable = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(w_output_table);
      String result = org.apache.drill.madlib.correlation.Correlation.correlation(
        sourceTable, madlibSchema, outputTable,
        "*", true);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);

    }

    @Override
    public void reset() {

    }
  }

  @FunctionTemplate(name = "covariance", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class CovarianceMessage implements DrillSimpleFunc {

    @Param
    VarCharHolder message_holder;

    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String msg = org.apache.drill.madlib.common.DataHolderUtils.extractVarchar(message_holder);
      String help = org.apache.drill.madlib.correlation.Correlation.help(msg, true);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, help);
      buf = out.buffer;
    }
  }

  @FunctionTemplate(name = "covariance", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class CovarianceEmpty implements DrillSimpleFunc {


    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;
    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String help = org.apache.drill.madlib.correlation.Correlation.help("", true);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, help);
      buf = out.buffer;
    }
  }
}
