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

import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import kvd.common.KvdException;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.common.packet.proto.PutInitBody;

class KvdPut implements Abortable {

  private ClientBackend backend;

  private String key;

  private CompletableFuture<OutputStream> future = new CompletableFuture<>();

  private int channelId;

  private Consumer<Abortable> closeListener;

  private KvdPutOutputStream stream;

  private AtomicBoolean closed = new AtomicBoolean();

  private int txId;

  public KvdPut(ClientBackend backend, int txId, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.txId = txId;
    this.key = key;
    this.closeListener = closeListener;
  }

  public void start() {
    channelId = backend.createChannel(this::receive);
    try {
      backend.sendAsync(Packets.builder(PacketType.PUT_INIT, channelId, txId)
          .setPutInit(PutInitBody.newBuilder()
              .setKey(key)
              .build())
          .build());
    } catch(Exception e) {
      throw new KvdException("kvd put failed", e);
    }
  }

  @Override
  public void abort() {
    future.completeExceptionally(new KvdException("aborted"));
    if(stream != null) {
      stream.abort();
    }
    close();
  }

  private void close() {
    if(!closed.getAndSet(true)) {
      if(stream != null) {
        stream.close();
      }
      this.closeListener.accept(this);
      future.complete(null);
      backend.closeChannel(channelId);
    }
  }

  public void receive(Packet packet) {
    if(stream != null) {
      stream.channelReceiver(packet);
    } else if(PacketType.PUT_INIT.equals(packet.getType())) {
      stream = new KvdPutOutputStream(backend, channelId, s -> close());
      future.complete(stream);
    } else if(PacketType.PUT_ABORT.equals(packet.getType())) {
      future.completeExceptionally(new KvdException("aborted"));
      close();
    } else {
      throw new KvdException(String.format("invalid server response '%s'", packet.getType()));
    }
  }

  public CompletableFuture<OutputStream> getFuture() {
    return future;
  }

  @Override
  public String toString() {
    return "PUT " + key;
  }
}
