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
package org.apache.drill.exec.server;

import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.rest.InternalSessionResources;
import org.apache.drill.exec.server.rest.InternalUserConnection;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.apache.drill.exec.server.rest.WebSessionResources;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.work.WorkManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 *
 */

@SuppressWarnings({"RedundantThrows", "Convert2Lambda", "unused"})
public class InternalProxy {

  private final static InternalProxy INSTANCE = new InternalProxy();

  private EventExecutor executor;
  private WorkManager manager;
  private DrillUserPrincipal userPrincipal;
  private final ThreadLocal<UserSession> localSession = new ThreadLocal<>();

  private void mayInit() throws IOException {
    if (localSession.get() == null) {
      localSession.set(createNewSession());
    }
  }

  private UserSession getSession() throws IOException {
    mayInit();
    return localSession.get();
  }

  public QueryWrapper.QueryResult executeQuery(QueryWrapper query) throws Exception {
    WebUserConnection connection = getConnection();
    QueryWrapper.QueryResult result = query.run(manager, connection);
    connection.cleanupSession();
    return result;
  }

  private List<String> getTables(String schema, String table) throws Exception{
    UserSession session = getSession();
    String catalog = "DRILL";

    final UserProtos.GetTablesReq.Builder reqBuilder = UserProtos.GetTablesReq.newBuilder();
    if (StringUtils.isNotBlank(catalog)) {
      reqBuilder.setCatalogNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(catalog)
        .setEscape("\\")
        .build());
    }

    if (StringUtils.isNotBlank(schema)) {
      reqBuilder.setSchemaNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(schema)
        .setEscape("\\")
        .build());
    }

    if (StringUtils.isNotBlank(table)) {
      reqBuilder.setTableNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(table)
        .setEscape("\\")
        .build());
    }
    final CountDownLatch latch = new CountDownLatch(1);

    final List<String> tables = new ArrayList<>();
    manager.getUserWorker().submitTablesMetadataWork(session, reqBuilder.build(), new ResponseSender() {
      @Override
      public void send(Response r) {
        UserProtos.GetTablesResp resp = (UserProtos.GetTablesResp) r.pBody;
        for (UserProtos.TableMetadata metadata : resp.getTablesList()) {
          tables.add(metadata.getTableName());
        }
        latch.countDown();
      }
    });

    latch.await();
    return tables;
  }

  private Map<String, String> getAllColumns(String schema, String table) throws Exception{
    UserSession session = getSession();
    String catalog = "DRILL";

    final UserProtos.GetColumnsReq.Builder reqBuilder = UserProtos.GetColumnsReq.newBuilder();
    if (StringUtils.isNotBlank(catalog)) {
      reqBuilder.setCatalogNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(catalog)
        .setEscape("\\")
        .build());
    }

    if (StringUtils.isNotBlank(schema)) {
      reqBuilder.setSchemaNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(schema)
        .setEscape("\\")
        .build());
    }

    if (StringUtils.isNotBlank(table)) {
      reqBuilder.setTableNameFilter(UserProtos.LikeFilter.newBuilder()
        .setPattern(table)
        .setEscape("\\")
        .build());
    }
    final CountDownLatch latch = new CountDownLatch(1);
    final Map<String, String> columns = new HashMap<>();
    manager.getUserWorker().submitColumnsMetadataWork(session, reqBuilder.build(), new ResponseSender() {
      @Override
      public void send(Response r) {
        UserProtos.GetColumnsResp resp = (UserProtos.GetColumnsResp) r.pBody;
        resp.getColumnsList().forEach(meta->columns.put(meta.getColumnName(), meta.getDataType()));
        latch.countDown();
      }
    });

    latch.await();
    return columns;
  }

  private void releaseConnection() throws IOException {
    if (localSession.get() != null) {
      localSession.get().close();
      localSession.remove();
    }
  }

  public static void init(Drillbit bit, WorkManager manager) {
    INSTANCE.manager = manager;
    INSTANCE.userPrincipal =
      new DrillUserPrincipal("admin", true);
    INSTANCE.executor = manager.getContext().getBitLoopGroup().next();
  }

  private WebUserConnection getConnection() throws IOException {
    // Create an allocator here for each request
    final BufferAllocator sessionAllocator = manager.getContext().getAllocator()
      .newChildAllocator("IntervalProxy:UserSession",
        manager.getContext().getConfig().getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
        manager.getContext().getConfig().getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));

    // Create new UserSession
    final UserSession drillUserSession = getSession();

    // Try to get the remote Address but set it to null in case of failure.
    SocketAddress remoteAddress = null;
    try {
      // This can be slow as the underlying library will try to resolve the address
      remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 0);
    } catch (Exception ex) {
      // no-op
    }

    // Create a dummy close future which is needed by Foreman only. Foreman uses this future to add a close
    // listener to known about channel close event from underlying layer.
    //
    // The invocation of this close future is no-op as it will be triggered after query completion in unsecure case.
    // But we need this close future as it's expected by Foreman.
    final ChannelPromise closeFuture = new DefaultChannelPromise(null, executor);

    final WebSessionResources webSessionResources = new InternalSessionResources(sessionAllocator, remoteAddress,
      drillUserSession, closeFuture);
    return new InternalUserConnection(webSessionResources, true);
  }

  private UserSession createNewSession() {
    return UserSession.Builder.newBuilder()
      .withCredentials(UserBitShared.UserCredentials.newBuilder()
        .setUserName(userPrincipal.getName())
        .build())
      .withOptionManager(manager.getContext().getOptionManager())
      .setSupportComplexTypes(manager.getContext().getConfig().getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES))
      .build();
  }

  public static QueryWrapper.QueryResult execute(QueryWrapper query) throws Exception {
    return INSTANCE.executeQuery(query);
  }

  public static String getTable(String schema, String table) throws Exception {
    List<String> tables = INSTANCE.getTables(schema, table);
    if (tables == null || tables.isEmpty()) {
      return null;
    } else {
      return tables.get(0);
    }
  }

  public static Map<String, String> getColumns(String schema, String table) throws Exception {
    return INSTANCE.getAllColumns(schema, table);
  }

  public static void recycle() throws IOException {
    INSTANCE.releaseConnection();
  }
}
