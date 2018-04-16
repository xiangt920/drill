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

import org.apache.drill.exec.server.InternalProxy;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Internal SQL executor
 *
 */

public class SqlExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SqlExecutor.class);

  private static final SqlExecutor EXECUTOR = new SqlExecutor();

  private QueryWrapper.QueryResult execute(QueryWrapper query) throws Exception {

    LOG.info("executing sql: {}", query.getQuery());
    return InternalProxy.execute(query);
  }

  private QueryWrapper.QueryResult execute(String sql) throws Exception {
    QueryWrapper query = new QueryWrapper(sql, "SQL");
    return execute(query);
  }

  private String getTableInternal(String schema, String table) throws Exception {
    return InternalProxy.getTable(schema, table);
  }

  private Map<String, String> getColumnsInternal(String schema, String table) throws Exception {
    return InternalProxy.getColumns(schema, table);
  }

  private void recycle() throws IOException {
    InternalProxy.recycle();
  }

  public static boolean executeDdlSql(String sql) throws Exception {

    try {
      EXECUTOR.execute(sql);
      return true;
    } finally {
      EXECUTOR.recycle();
    }
  }

  public static QueryWrapper.QueryResult executeSelectSql(String sql) throws
    Exception {
    try {
      QueryWrapper.QueryResult result = EXECUTOR.execute(sql);
      return result;
    } finally {
      EXECUTOR.recycle();
    }
  }

  public static boolean executeDdlSqlNoRecycle(String sql) throws Exception {
    EXECUTOR.execute(sql);
    return true;
  }

  public static QueryWrapper.QueryResult executeSelectSqlNoRecycle(String sql) throws Exception {
    return EXECUTOR.execute(sql);
  }

  /**
   * Get table from server by schema and table name. The table must be a view.
   * @param schema name of schema
   * @param table name of table
   * @return return the real table name in server
   */
  public static String getTable(String schema, String table) throws Exception {
    return EXECUTOR.getTableInternal(schema, table);
  }

  /**
   * Get view columns from server by schema and table name.
   * @param schema name of schema
   * @param table name of table
   * @return return the columns and their types.
   */
  public static Map<String, String> getViewColumns(String schema, String table) throws Exception {
    return EXECUTOR.getColumnsInternal(schema, table);
  }

  public static void recycleExecutor() {
    try {

      EXECUTOR.recycle();
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

}
