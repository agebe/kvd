/*
 * Copyright 2020 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.server;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.net.ServerSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSocketServer {

  private static final Logger log = LoggerFactory.getLogger(SimpleSocketServer.class);

  private Thread t;

  private AtomicBoolean run = new AtomicBoolean(true);

  private ServerSocket serverSocket;

  private int port;

  private Consumer<Socket> connectHandler;

  public SimpleSocketServer(int port, Consumer<Socket> connectHandler) {
    super();
    this.port = port;
    this.connectHandler = connectHandler;
  }

  private void openServerSocket() {
    try {
      serverSocket = ServerSocketFactory.getDefault().createServerSocket(port);
    } catch(Exception e) {
      throw new RuntimeException("open server socket failed", e);
    }
  }

  private void closeServerSocket() {
    try {
      serverSocket.close();
    } catch(Exception e) {
      throw new RuntimeException("close server socket failed", e);
    }
  }

  public synchronized void start() {
    if(t == null) {
      openServerSocket();
      Thread t = new Thread(() -> {
        while(run.get()) {
          try {
            // only block accept call for limited time to check on run flag
            serverSocket.setSoTimeout(500);
          } catch (SocketException e1) {
            throw new RuntimeException("failed to setup socket SoTimeout", e1);
          }
          try {
            connectHandler.accept(serverSocket.accept());
          } catch(SocketTimeoutException e) {
            // ignore
          } catch(Exception e) {
            log.error("socket exception", e);
          }
        }
        closeServerSocket();
        log.debug("exit");
      }, "simple-socket-server");
      t.start();
    } else {
      log.warn("already started");
    }
  }

  public synchronized void stop() {
    run.set(false);
  }

  public int getLocalPort() {
    return serverSocket.getLocalPort();
  }

}
