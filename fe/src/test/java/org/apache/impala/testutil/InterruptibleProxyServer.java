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

package org.apache.impala.testutil;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class InterruptibleProxyServer implements AutoCloseable {
  private final String targetHost;
  private final int targetPort;
  private ServerSocket serverSocket;
  private final Set<Socket> clientSockets = Collections.synchronizedSet(new HashSet<>());
  private volatile boolean running = false;
  private Thread acceptThread;

  public InterruptibleProxyServer(String host, int port) {
    this.targetHost = host;
    this.targetPort = port;
  }

  public void start() throws IOException {
    serverSocket = new ServerSocket();
    serverSocket.bind(null);
    running = true;
    acceptThread = new Thread(this::acceptLoop, "InterruptibleProxyServer-Accept");
    acceptThread.start();
  }

  public int getLocalPort() {
    return serverSocket.getLocalPort();
  }

  private void acceptLoop() {
    while (running) {
      try {
        Socket client = serverSocket.accept();
        clientSockets.add(client);
        Thread proxyThread =
            new Thread(() -> handleClient(client), "InterruptibleProxyServer-Proxy");
        proxyThread.start();
      } catch (IOException e) {
        if (running)
          e.printStackTrace();
      }
    }
  }

  private void handleClient(Socket client) {
    try (Socket target = new Socket(targetHost, targetPort)) {
      clientSockets.add(target);
      Thread t1 = new Thread(() -> forward(client, target));
      Thread t2 = new Thread(() -> forward(target, client));
      t1.start();
      t2.start();
      t1.join();
      t2.join();
    } catch (Exception e) {
      // Ignore
    } finally {
      clientSockets.remove(client);
      try {
        client.close();
      } catch (IOException ignored) {
      }
    }
  }

  private void forward(Socket in, Socket out) {
    try {
      byte[] buf = new byte[4096];
      int len;
      while ((len = in.getInputStream().read(buf)) != -1) {
        out.getOutputStream().write(buf, 0, len);
        out.getOutputStream().flush();
      }
    } catch (IOException ignored) {
    }
  }

  public void closeConnections() {
    synchronized (clientSockets) {
      for (Socket s : clientSockets) {
        try {
          s.close();
        } catch (IOException ignored) {
        }
      }
      clientSockets.clear();
    }
  }

  public void close() throws IOException {
    running = false;
    if (serverSocket != null)
      serverSocket.close();
    if (acceptThread != null)
      acceptThread.interrupt();
    closeConnections();
  }
}