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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;

class KvdLock implements Abortable {

  private static final Logger log = LoggerFactory.getLogger(KvdLock.class);

  private ClientBackend backend;

  private String key;

  private CompletableFuture<Boolean> future = new CompletableFuture<>();

  private Consumer<Abortable> closeListener;

  private int channelId;

  private int txId;

  public KvdLock(ClientBackend backend, int txId, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.txId = txId;
    this.key = key;
    this.closeListener = closeListener;
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(Packets.packet(PacketType.LOCK, channelId, txId, key));
    } catch(Exception e) {
      try {
        close();
      } catch(Exception e2) {
        // ignore
      }
      throw new KvdException("lock failed", e);
    }
  }

  @Override
  public void abort() {
    future.completeExceptionally(new KvdException("aborted"));
    close();
  }

  private void close() {
    backend.closeChannel(channelId);
    this.closeListener.accept(this);
  }


  private void receive(Packet packet) {
    try {
      close();
    } catch(Exception e) {
      log.error("close failed", e);
    }
    if(PacketType.LOCK.equals(packet.getType())) {
      byte[] buf = packet.getByteBody().toByteArray();
      if((buf != null) && (buf.length >= 1)) {
        future.complete((buf[0] == 1));
      } else {
        log.error("invalid response");
        future.completeExceptionally(new KvdException("invalid response"));
      }
    } else if(PacketType.ABORT.equals(packet.getType())) {
      future.completeExceptionally(new KvdException("server abort"));
    } else {
      log.error("received unexpected packet " + packet.getType());
      future.completeExceptionally(new KvdException("received unexpected packet " + packet.getType()));
    }
  }

  public CompletableFuture<Boolean> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "LOCK " + key;
  }

}
