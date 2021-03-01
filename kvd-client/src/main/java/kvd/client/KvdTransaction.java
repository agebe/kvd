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
import kvd.common.packet.Packet;
import kvd.common.packet.PacketType;
import kvd.common.packet.TxCommitPacket;
import kvd.common.packet.TxRollbackPacket;

public class KvdTransaction implements KvdOperations, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(KvdTransaction.class);

  private ClientBackend backend;

  private int txId;

  private int channel;

  private AtomicBoolean closed = new AtomicBoolean();

  private Set<Abortable> abortables = new HashSet<>();

  private CompletableFuture<Boolean> txClosed = new CompletableFuture<>();

  public KvdTransaction(ClientBackend backend, int txId, int channel) {
    super();
    this.backend = backend;
    this.txId = txId;
    this.channel = channel;
    txClosed.whenComplete((b,t) -> {
      backend.closeChannel(channel);
    });
  }

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

  public synchronized Future<Boolean> commitAsync() {
    closeInternal(new TxCommitPacket(channel, txId));
    return txClosed;
  }

  public void commit() {
    try {
      commitAsync().get();
    } catch(Exception e) {
      throw new KvdException("failed on commit", e);
    }
  }

  public synchronized Future<Boolean> rollbackAsync() {
    closeInternal(new TxRollbackPacket(channel, txId));
    return txClosed;
  }

  public void rollback() {
    try {
      rollbackAsync().get();
    } catch(Exception e) {
      throw new KvdException("failed on rollback", e);
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
  public synchronized OutputStream put(String key) {
    checkClosed();
    Utils.checkKey(key);
    try {
      KvdPutOutputStream out = new KvdPutOutputStream(backend, txId, key, this::removeAbortable);
      abortables.add(out);
      return out;
    } catch(Exception e) {
      throw new KvdException("put failed", e);
    }
  }

  @Override
  public synchronized Future<InputStream> getAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdGet get = new KvdGet(backend, txId, key, this::removeAbortable);
    abortables.add(get);
    get.start();
    return get.getFuture();
  }

  @Override
  public synchronized Future<Boolean> containsAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdContains contains = new KvdContains(backend, txId, key, this::removeAbortable);
    abortables.add(contains);
    contains.start();
    return contains.getFuture();
  }

  @Override
  public synchronized Future<Boolean> removeAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    KvdRemove remove = new KvdRemove(backend, txId, key, this::removeAbortable);
    abortables.add(remove);
    remove.start();
    return remove.getFuture();
  }

  @Override
  public String toString() {
    return "KvdTransaction [txId=" + txId + "]";
  }

}
