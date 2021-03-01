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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.packet.Packet;
import kvd.common.packet.PacketType;
import kvd.common.packet.TxBeginPacket;

public class KvdBeginTransaction implements Abortable {

  private static final Logger log = LoggerFactory.getLogger(KvdBeginTransaction.class);

  private ClientBackend backend;

  private CompletableFuture<KvdTransaction> future = new CompletableFuture<>();

  private int channelId;

  private Consumer<Abortable> closeListener;

  private AtomicBoolean closed = new AtomicBoolean();

  private long timeoutMs;

  private KvdTransaction tx;

  public KvdBeginTransaction(ClientBackend backend, Consumer<Abortable> closeListener, long timeoutMs) {
    this.backend = backend;
    this.closeListener = closeListener;
    this.timeoutMs = timeoutMs;
    future.whenComplete((t,e) -> close());
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(new TxBeginPacket(channelId, timeoutMs));
    } catch(Exception e) {
      log.warn("tx begin failed", e);
      try {
        close();
      } catch(Exception e2) {
        // ignore
      }
      throw new KvdException("get failed", e);
    }
  }

  @Override
  public void abort() {
    future.completeExceptionally(new KvdException("aborted"));
    close();
    if(tx != null) {
      tx.rollback();
    }
  }

  private void close() {
    if(!closed.getAndSet(true)) {
      this.closeListener.accept(this);
      future.complete(null);
      // do not close the channel here as it is passed on into the transaction
      //backend.closeChannel(channelId);
    }
  }

  public void receive(Packet packet) {
    if(tx != null) {
      // pass packets on to transaction
      tx.receive(packet);
    } else {
      if(PacketType.TX_BEGIN.equals(packet.getType())) {
        TxBeginPacket p = (TxBeginPacket)packet;
        log.debug("begin tx '{}'", p.getTxId());
        tx = new KvdTransaction(backend, p.getTxId(), p.getChannel());
        future.complete(tx);
      } else if(PacketType.TX_ABORT.equals(packet.getType())) {
        abort();
      } else {
        log.error("received unexpected packet " + packet.getType());
        future.completeExceptionally(new KvdException("received unexpected packet " + packet.getType()));
        abort();
      }
    }
  }

  public CompletableFuture<KvdTransaction> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "TX BEGIN";
  }

}
