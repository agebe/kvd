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

import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.server.storage.StorageBackend;
import kvd.server.util.HumanReadable;

public class SocketConnectHandler implements Consumer<Socket> {

  private static final Logger log = LoggerFactory.getLogger(SocketConnectHandler.class);

  private AtomicLong clientIdCounter = new AtomicLong();

  private Set<ClientHandler> clients = new HashSet<>();

  private int maxClients;

  private int socketSoTimeoutMs;

  private int clientTimeoutSeconds;

  private StorageBackend storage;

  public SocketConnectHandler(int maxClients,
      int socketSoTimeoutMs,
      int clientTimeoutSeconds,
      StorageBackend storage) {
    super();
    if(maxClients <= 0) {
      throw new KvdException("invalid max clients " + maxClients);
    }
    this.maxClients = maxClients;
    this.socketSoTimeoutMs = socketSoTimeoutMs;
    this.clientTimeoutSeconds = clientTimeoutSeconds;
    this.storage = storage;
    log.info("client timeout '{}', socket so timeout '{}'",
        HumanReadable.formatDuration(clientTimeoutSeconds, TimeUnit.SECONDS),
        HumanReadable.formatDuration(socketSoTimeoutMs, TimeUnit.MILLISECONDS));
  }

  @Override
  public synchronized void accept(Socket socket) {
    try {
      if(clients.size() < maxClients) {
        long clientId = clientIdCounter.getAndIncrement();
        final ClientHandler client = new ClientHandler(clientId,
            socketSoTimeoutMs,
            clientTimeoutSeconds,
            socket,
            storage);
        clients.add(client);
        Thread t = new Thread(() -> {
          try {
            client.run();
            log.debug("client handler '{}' run exit", clientId);
          } finally {
            try {
              client.close();
            } catch(Exception e) {
              log.warn("client cleanup failed", e);
            }
            removeClient(client);
            Utils.closeSocketQuietly(socket);
          }
        }, "client-" + clientId);
        t.start();
      } else {
        log.warn("not accepting now connections (max clients reached '{}')", maxClients);
        socket.close();
      }
    } catch(Exception e) {
      log.error("failed to accept new client connections, close", e);
      Utils.closeSocketQuietly(socket);
    }
  }

  private synchronized void removeClient(ClientHandler client) {
    clients.remove(client);
    log.debug("removed client '{}', # connected clients '{}'", client.getClientId(), clients.size());
  }

}
