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
package kvd.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.HostAndPort;
import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.PacketType;

/**
 * {@code KvdClient} is the public API that clients should use to interact with the server.
 *
 * <p>Example usage:<pre>
 *  try(KvdClient client = new KvdClient("kvd.example.com:3030")) {
 *    try(DataOutputStream out = new DataOutputStream(client.put("test"))) {
 *      out.writeLong(42);
 *    }
 *  }
 * </pre>
 *
 * <p>Note: {@link KvdClient#KvdClient(java.lang.String)} establishes a single socket connection to the server
 * that it keeps alive until {@link KvdClient#close} is called.
 *
 * <p>Note: {@code KvdClient} is thread-safe.
 */
public class KvdClient implements KvdOperations, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(KvdClient.class);

  private static final int NO_TX = 0;

  private ClientBackend backend;

  private Set<Abortable> abortables = new HashSet<>();

  private AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Establishes the connection to the server.
   * @param serverAddress in the form
   *        <a href="https://guava.dev/releases/30.1-jre/api/docs/com/google/common/net/HostAndPort.html">{@code host:port}</a>.
   *        Port can be omitted and 3030 is used in this case.
   */
  public KvdClient(String serverAddress) {
    try {
      HostAndPort hp = HostAndPort.fromString(serverAddress).withDefaultPort(3030);
      log.trace("connecting to '{}'", hp);
      Socket socket = new Socket(InetAddress.getByName(hp.getHost()), hp.getPort());
      socket.setSoTimeout(1000);
      backend = new ClientBackend(socket, () -> {
        try {
          log.debug("client backend close notification");
          close();
        } catch(Exception e) {
          // ignore
        }
      });
      backend.start();
      backend.sendAsync(Packets.hello());
      backend.waitForHelloReceived();
    } catch(Exception e) {
      throw new KvdException(String.format("failed to connect to '%s'", serverAddress), e);
    }
  }

  private synchronized void removeAbortable(Abortable a) {
    this.abortables.remove(a);
  }

  private void checkClosed() {
    if(isClosed()) {
      throw new KvdException("closed");
    }
  }


  @Override
  public synchronized Future<OutputStream> putAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdPut put = new KvdPut(backend, NO_TX, key, this::removeAbortable);
    abortables.add(put);
    put.start();
    return put.getFuture();
  }

  @Override
  public synchronized Future<InputStream> getAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdGet get = new KvdGet(backend, NO_TX, key, this::removeAbortable);
    abortables.add(get);
    get.start();
    return get.getFuture();
  }

  @Override
  public synchronized Future<Boolean> containsAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdContains contains = new KvdContains(backend, NO_TX, key, this::removeAbortable);
    abortables.add(contains);
    contains.start();
    return contains.getFuture();
  }

  @Override
  public synchronized Future<Boolean> removeAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdRemove remove = new KvdRemove(backend, NO_TX, key, this::removeAbortable);
    abortables.add(remove);
    remove.start();
    return remove.getFuture();
  }

  /**
   * Waits for pending requests to finish and closes the connection to the server. Once closed this instance
   * can't be reused and must be discarded.
   */
  @Override
  public synchronized void close() {
    if(!closed.getAndSet(true)) {
      new HashSet<>(abortables).forEach(a -> {
        try {
          log.warn("aborting '{}'", a);
          a.abort();
        } catch(Exception e) {
          // ignore
        }
      });
      abortables.clear();
      try {
        backend.sendAsync(Packets.packet(PacketType.BYE));
      } catch(Exception e) {
        // ignore
      }
      backend.warnOnOpenChannels();
      backend.closeGracefully();
    }
  }

  /**
   * Check if the KvdClient can still be used.
   * @return {@code true} if the instance is closed, {@code false} otherwise.
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * begin a new transaction with the specified timeout.
   * @param timeoutMs the timeout in milliseconds or 0 for no timeout
   * @return {@code Future} that evaluates to a {@link KvdTransaction} when the server has created the transaction
   */
  public synchronized Future<KvdTransaction> beginTransactionAsync(long timeoutMs) {
    checkClosed();
    KvdBeginTransaction txBegin = new KvdBeginTransaction(backend, this::removeAbortable, timeoutMs);
    abortables.add(txBegin);
    txBegin.start();
    return txBegin.getFuture();
  }

  public KvdTransaction beginTransaction(long timeoutMs) {
    try {
      return beginTransactionAsync(timeoutMs).get();
    } catch (Exception e) {
      throw new KvdException("begin transaction failed", e);
    }
  }

  public KvdTransaction beginTransaction() {
    return beginTransaction(0);
  }

}
