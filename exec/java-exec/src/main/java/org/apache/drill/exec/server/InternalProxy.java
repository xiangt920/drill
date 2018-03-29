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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
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
import java.security.Principal;

/**
 *
 */

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

  public QueryWrapper.QueryResult executeQuery(QueryWrapper query) throws Exception {
    WebUserConnection connection = null;
    mayInit();
    connection = getConnection();
    QueryWrapper.QueryResult result = query.run(manager, connection);
    connection.cleanupSession();
    return result;
  }

  public void releaseConnection() throws IOException {
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

  private WebUserConnection getConnection() {
    // Create an allocator here for each request
    final BufferAllocator sessionAllocator = manager.getContext().getAllocator()
      .newChildAllocator("IntervalProxy:UserSession",
        manager.getContext().getConfig().getLong(ExecConstants.HTTP_SESSION_MEMORY_RESERVATION),
        manager.getContext().getConfig().getLong(ExecConstants.HTTP_SESSION_MEMORY_MAXIMUM));

    final Principal sessionUserPrincipal = userPrincipal;

    // Create new UserSession
    final UserSession drillUserSession = localSession.get();

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

  public static void recycle() throws IOException {
    INSTANCE.releaseConnection();
  }
}
