/*
 * Copyright 2021 Andre Gebers
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.TransactionClosedException;
import kvd.common.Utils;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;

/**
 * Transaction support, Unit of Work pattern
 */
public class KvdTransaction implements KvdOperations, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(KvdTransaction.class);

  private ClientBackend backend;

  private int txId;

  private int channel;

  private AtomicBoolean closed = new AtomicBoolean();

  private Set<Abortable> abortables = new HashSet<>();

  private CompletableFuture<Boolean> txClosed = new CompletableFuture<>();

  KvdTransaction(ClientBackend backend, int txId, int channel) {
    super();
    this.backend = backend;
    this.txId = txId;
    this.channel = channel;
    txClosed.whenComplete((b,t) -> {
      backend.closeChannel(channel);
    });
  }

  CompletableFuture<Boolean> closedFuture() {
    return txClosed;
  }

  void abortNow() {
    txClosed.complete(true);
  }

  /**
   * Rollback and close transaction.
   */
  @Override
  public void close() {
    rollback();
  }

  private boolean isClosed() {
    return closed.get();
  }

  private void checkClosed() {
    if(isClosed()) {
      throw new TransactionClosedException();
    }
  }

  private synchronized void removeAbortable(Abortable a) {
    this.abortables.remove(a);
  }

  private synchronized void abortAll() {
    abortables.forEach(a -> {
      try {
        log.warn("transaction '{}', aborting '{}'", txId, a);
        a.abort();
      } catch(Exception e) {
        // ignore
      }
    });
    abortables.clear();
  }

  synchronized void receive(Packet packet) {
    if(PacketType.TX_ABORT.equals(packet.getType())) {
      try {
        closeInternal(null);
      } finally {
        txClosed.completeExceptionally(new KvdException("transaction aborted"));
      }
    } else if(PacketType.TX_CLOSED.equals(packet.getType())) {
      txClosed.complete(Boolean.TRUE);
    }
  }

  /**
   * Commit and close the transaction. No further operations are allowed on the transaction after commit and the
   * transaction object can be discarded.
   * @return {@code Future} future that resolves to true or completes exceptionally.
   */
  public synchronized Future<Boolean> commitAsync() {
    closeInternal(Packets.packet(PacketType.TX_COMMIT, channel, txId));
    return txClosed;
  }

  /**
   * Commit and close the transaction. No further operations are allowed on the transaction after commit and the
   * transaction object can be discarded.
   */
  public void commit() {
    try {
      commitAsync().get();
    } catch(Exception e) {
      throw new KvdException("failed on commit", e);
    }
  }

  /**
   * Rollback and close the transaction. No further operations are allowed on the transaction after commit and the
   * transaction object can be discarded.
   * @return {@code Future} that resolves to true or completes exceptionally.
   */
  public synchronized Future<Boolean> rollbackAsync() {
    closeInternal(Packets.packet(PacketType.TX_ROLLBACK, channel, txId));
    return txClosed;
  }

  /**
   * Rollback and close the transaction. No further operations are allowed on the transaction after commit and the
   * transaction object can be discarded.
   */
  public void rollback() {
    try {
      rollbackAsync().get();
    } catch(Exception e) {
      // ignore, might have already been aborted (timeout)
    }
  }

  private synchronized void closeInternal(Packet packet) {
    if(!closed.getAndSet(true)) {
      abortAll();
      if(packet != null) {
        try {
          backend.sendAsync(packet);
        } catch(InterruptedException e) {
          log.warn("interrupted", e);
        }
      }
    }
  }

  @Override
  public synchronized Future<OutputStream> putAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdPut put = new KvdPut(backend, txId, key, this::removeAbortable);
    abortables.add(put);
    put.start();
    return put.getFuture();
  }

  @Override
  public synchronized Future<InputStream> getAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdGet get = new KvdGet(backend, txId, key, this::removeAbortable);
    abortables.add(get);
    get.start();
    return get.getFuture();
  }

  @Override
  public synchronized Future<Boolean> containsAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdContains contains = new KvdContains(backend, txId, key, this::removeAbortable);
    abortables.add(contains);
    contains.start();
    return contains.getFuture();
  }

  @Override
  public synchronized Future<Boolean> removeAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdRemove remove = new KvdRemove(backend, txId, key, this::removeAbortable);
    abortables.add(remove);
    remove.start();
    return remove.getFuture();
  }

  /**
   * Obtain write lock on the key in the same way a put or remove operation would do.
   * It does not matter whether the key exists in the database or not.
   * @param key the key to write lock
   * @return {@code Future} that evaluates to true if concurrency mode is different from NONE or false for concurrency mode NONE
   * or completes exceptionally if the key can't be write locked because either another transaction has a write lock on
   * the key already in optimistic concurrency mode or a deadlock is detected in pessimistic concurrency mode
   */
  public synchronized Future<Boolean> lockAsync(byte[] key) {
    checkClosed();
    Utils.checkKey(key);
    KvdLock lock = new KvdLock(backend, txId, key, this::removeAbortable);
    abortables.add(lock);
    lock.start();
    return lock.getFuture();
  }

  /**
   * Obtain write lock on the key in the same way a put or remove operation would do.
   * It does not matter whether the key exists in the database or not.
   * @param key the key to write lock
   * @return {@code Future} that evaluates to true if concurrency mode is different from NONE or false for concurrency mode NONE
   * or completes exceptionally if the key can't be write locked because either another transaction has a write lock on
   * the key already in optimistic concurrency mode or a deadlock is detected in pessimistic concurrency mode
   */
  public synchronized Future<Boolean> lockAsync(String key) {
    return lockAsync(key.getBytes());
  }

  /**
   * @param key the key to write lock
   * @return see {@link #lockAsync(String)}
   */
  public boolean lock(String key) {
    try {
      return lockAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("write lock failed", e);
    }
  }

  @Override
  public String toString() {
    return "KvdTransaction [txId=" + txId + "]";
  }

}
