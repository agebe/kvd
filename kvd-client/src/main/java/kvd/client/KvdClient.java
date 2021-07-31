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
import java.util.stream.Stream;

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
 * All {@link KvdOperations} methods implemented in this class execute a single operation transaction on the server that
 * is automatically committed when the operation completes. They partake in resource locking the same way as
 * {@link KvdTransaction} operations. The main difference between {@link KvdTransaction} operations is that operations
 * in {@code KvdClient} are auto committed. For manual commit/rollback start a new Transaction with {@link #beginTransaction()}
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

  private long transactionDefaultTimeoutMs;

  private ThreadLocal<KvdTransaction> transactions = new ThreadLocal<>();

  /**
   * Create a new {@code KvdClient} instance connecting to the server. Use {@link KvdClientBuilder} to create a
   * {@code KvdClient} with non standard options.
   * @param serverAddress in the form
   *        <a href="https://guava.dev/releases/30.1-jre/api/docs/com/google/common/net/HostAndPort.html">{@code host:port}</a>.
   *        Port can be omitted and 3030 is used in this case.
   */
  public KvdClient(String serverAddress) {
    this(new KvdClientBuilder(serverAddress));
  }

  KvdClient(KvdClientBuilder builder) {
    try {
      this.transactionDefaultTimeoutMs = builder.getTransactionDefaultTimeoutMs();
      HostAndPort hp = HostAndPort.fromString(builder.getServerAddress()).withDefaultPort(3030);
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
      throw new KvdException(String.format("failed to connect to '%s'", builder.getServerAddress()), e);
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
  public synchronized Future<OutputStream> putAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdPut put = new KvdPut(backend, NO_TX, key, this::removeAbortable);
    abortables.add(put);
    put.start();
    return put.getFuture();
  }

  @Override
  public synchronized Future<InputStream> getAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdGet get = new KvdGet(backend, NO_TX, key, this::removeAbortable);
    abortables.add(get);
    get.start();
    return get.getFuture();
  }

  @Override
  public synchronized Future<Boolean> containsAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdContains contains = new KvdContains(backend, NO_TX, key, this::removeAbortable);
    abortables.add(contains);
    contains.start();
    return contains.getFuture();
  }

  @Override
  public synchronized Future<Boolean> removeAsync(byte[] key) {
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
   * Begin a new transaction with the specified timeout. Also see {{@link #beginTransaction(long)}
   * @param timeoutMs The transaction timeout in milliseconds or 0 for no timeout. If the timeout is exceeded
   * the transaction is aborted (rollback).
   * @return {@code Future} that evaluates to a {@link KvdTransaction} when the server has created the transaction
   */
  public synchronized Future<KvdTransaction> beginTransactionAsync(long timeoutMs) {
    checkClosed();
    KvdBeginTransaction txBegin = new KvdBeginTransaction(backend, this::removeAbortable, timeoutMs);
    abortables.add(txBegin);
    txBegin.start();
    return txBegin.getFuture();
  }

  /**
   * Begin a new transaction with the specified timeout and waits until the transaction has been created on the server.
   * Normally this method should be used in a try-with-resource block to make
   * sure the transaction is closed. Note that you have to commit the transaction manually to make changes permanent
   * before the try-with-resource block closes the transaction. Also consider using {@link #withTransaction(KvdWork)}
   * which automatically commits transactions and also allows outer/inner units of work to share the same transaction
   * to improve code reusability
   * @param timeoutMs The transaction timeout in milliseconds or 0 for no timeout. If the timeout is exceeded
   * the transaction is aborted (rollback).
   * @return {@link KvdTransaction}
   */
  public KvdTransaction beginTransaction(long timeoutMs) {
    try {
      return beginTransactionAsync(timeoutMs).get();
    } catch (Exception e) {
      throw new KvdException("begin transaction failed", e);
    }
  }

  /**
   * See {@link #beginTransaction(long)} except this method uses the default transaction timeout
   * @return {@link KvdTransaction}
   */
  public KvdTransaction beginTransaction() {
    return beginTransaction(transactionDefaultTimeoutMs);
  }

  private <T> T withNewTransaction(KvdWork<T> work) {
    // try-with-resource closes KvdTransaction (rollback), no need to handle exceptions here
    try(KvdTransaction tx = beginTransaction()) {
      transactions.set(tx);
      T result = work.execute(tx);
      tx.commit();
      return result;
    } finally {
      transactions.remove();
    }
  }

  /**
   * Execute a new or join an existing {@link KvdTransaction} that has been started either with
   * {@link #withTransaction(KvdWork)} or {@link #withTransactionVoid(KvdVoidWork)}. A new transaction is started 
   * with the default timeout. The transaction is automatically committed when the most outer {@link KvdWork} finishes
   * or aborted (rollback) when the {@link KvdWork} throws an exceptions.
   * @param <T> Result type of the {@link KvdWork}
   * @param work the unit of work to be executed within the transaction
   * @return The result of the unit of work
   */
  public <T> T withTransaction(KvdWork<T> work) {
    KvdTransaction tx = transactions.get();
    if(tx != null) {
      return work.execute(tx);
    } else {
      return withNewTransaction(work);
    }
  }

  /**
   * Same as {@link #withTransaction(KvdWork)} except this does not return a result
   * @param work the unit of work to be executed within the transaction
   */
  public void withTransactionVoid(KvdVoidWork work) {
    withTransaction(tx -> {
      work.execute(tx);
      return null;
    });
  }

  /**
   * Retrieve the default transaction timeout in milliseconds, 0 means no timeout. This value can only be set when
   * the {@code KvdClient} is constructed through the {@link KvdClientBuilder}
   * @return The default transaction timeout in milliseconds
   */
  public long getTransactionDefaultTimeoutMs() {
    return transactionDefaultTimeoutMs;
  }

  /**
   * Removes all keys and values from the database.
   * @return {@code Future} which evaluates to {@code true} if when all key/values have been removed from the server,
   *         {@code false} otherwise.
   */
  public Future<Boolean> removeAllAsync() {
    checkClosed();
    KvdRemoveAll removeAll = new KvdRemoveAll(backend, this::removeAbortable);
    abortables.add(removeAll);
    removeAll.start();
    return removeAll.getFuture();
  }

  /**
   * Remove all keys and values from the database.
   */
  public Boolean removeAll() {
    try {
      return removeAllAsync().get();
    } catch(Exception e) {
      throw new KvdException("failed to remove all key/values", e);
    }
  }

}
