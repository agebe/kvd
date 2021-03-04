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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import kvd.common.ByteRingBuffer;
import kvd.common.IOStreamUtils;
import kvd.common.KvdException;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.common.packet.proto.PutInitBody;

public class KvdPutOutputStream extends OutputStream implements Abortable {

  private ClientBackend backend;

  private int channelId;

  private ByteRingBuffer ring = new ByteRingBuffer(16*1024);

  private Consumer<Abortable> closeListener;

  private AtomicBoolean closed = new AtomicBoolean();

  private AtomicBoolean aborted = new AtomicBoolean();

  private CompletableFuture<Boolean> completed = new CompletableFuture<Boolean>();

  private String key;

  public KvdPutOutputStream(ClientBackend backend, int txId, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.closeListener = closeListener;
    this.key = key;
    channelId = backend.createChannel(this::channelReceiver);
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
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf);
  }

  private void channelReceiver(Packet packet) {
    if(PacketType.PUT_ABORT.equals(packet.getType())) {
      aborted.set(true);
      completed.completeExceptionally(new KvdException("server aborted"));
    } else if(PacketType.PUT_COMPLETE.equals(packet.getType())) {
      completed.complete(true);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if(closed.get()) {
      throw new KvdException("stream closed");
    }
    if(aborted.get()) {
      abort();
      throw new KvdException("stream aborted");
    }
    IOStreamUtils.checkFromIndexSize(b, off, len);
    int written = 0;
    for(;;) {
      written += ring.write(b, off+written, len-written);
      if(written < len) {
        flush();
      } else {
        break;
      }
    }
  }

  @Override
  public void flush() throws IOException {
    int used = ring.getUsed();
    if(used > 0) {
      byte[] buf = new byte[used];
      int read = ring.read(buf);
      if(used != read) {
        throw new KvdException(String.format("internal error, read (%s) != used (%s)", read, used));
      }
      try {
        backend.sendAsync(Packets.packet(PacketType.PUT_DATA, channelId, buf));
      } catch(Exception e) {
        throw new KvdException("flush failed", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
      backend.sendAsync(Packets.packet(PacketType.PUT_FINISH, channelId));
      completed.get();
    } catch(Exception e) {
      throw new KvdException("close failed", e);
    } finally {
      closeInternal();
    }
  }

  @Override
  public void abort() {
    try {
      backend.sendAsync(Packets.packet(PacketType.PUT_ABORT, channelId));
    } catch(Exception e) {
      throw new KvdException("abort failed", e);
    } finally {
      closeInternal();
      completed.completeExceptionally(new KvdException("aborted"));
    }
  }

  private void closeInternal() {
    closed.set(true);
    backend.closeChannel(channelId);
    this.closeListener.accept(this);
  }

  @Override
  public String toString() {
    return "PUT " + key;
  }

}
