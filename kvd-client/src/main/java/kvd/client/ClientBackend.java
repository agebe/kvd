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

import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;

class ClientBackend {

  private static final Logger log = LoggerFactory.getLogger(ClientBackend.class);

  private String clientId = UUID.randomUUID().toString();

  private Socket socket;

  private int serverTimeoutSeconds;

  private BlockingQueue<Packet> sendQueue = new ArrayBlockingQueue<>(100);

  private Map<Integer, Consumer<Packet>> channelReceivers = new HashMap<>();

  private AtomicBoolean closed = new AtomicBoolean(false);

  private AtomicInteger channelIds = new AtomicInteger(1);

  private Thread sendThread;

  private Thread receiveThread;

  private Thread pingThread;

  private Runnable onClose;

  private CompletableFuture<Boolean> helloReceivedFuture = new CompletableFuture<>();

  public ClientBackend(Socket socket, int serverTimeoutSeconds, Runnable onClose) {
    this.socket = socket;
    this.serverTimeoutSeconds = serverTimeoutSeconds;
    this.onClose = onClose;
  }

  public synchronized void start() {
    if(sendThread == null) {
      sendThread = new Thread(this::sendLoop, "kvd-send-" + clientId);
      sendThread.start();
      receiveThread = new Thread(this::receiveLoop, "kvd-receive-" + clientId);
      receiveThread.start();
      try {
        sendAsync(Packets.hello());
        waitForHelloReceived();
      } catch(InterruptedException e) {
        throw new KvdException("hello interrupted", e);
      }
      // start ping loop after hello packets
      pingThread = new Thread(this::pingLoop, "kvd-ping-" + clientId);
      pingThread.start();
    } else {
      log.warn("already started");
    }
  }

  private void pingLoop() {
    log.trace("starting ping loop");
    try {
      while(!isClosed()) {
        for(int i=0;i<10;i++) {
          if(isClosed()) {
            break;
          }
          Thread.sleep(100);
        }
        try {
          sendAsync(Packet.newBuilder().setType(PacketType.PING).build());
        } catch(Exception e) {
          if(!isClosed()) {
            log.warn("send ping failed", e);
          }
        }
      }
    } catch(Exception e) {
      log.warn("send ping failure", e);
    } finally {
      log.trace("ping loop exit");
      onClose.run();
    }
  }

  private void sendLoop() {
    log.trace("starting send loop");
    try(OutputStream out = socket.getOutputStream()) {
      for(;;) {
        Packet packet = sendQueue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          packet.writeDelimitedTo(out);
        } else if(isClosed()) {
          break;
        }
      }
    } catch(Exception e) {
      log.warn("send loop failure", e);
    } finally {
      log.trace("send loop exit");
      onClose.run();
    }
  }

  private void receiveLoop() {
    log.trace("starting receive loop");
    try(InputStream in = socket.getInputStream()) {
      Packets.receiveHello(in);
      helloReceivedFuture.complete(true);
      log.trace("received hello packet");
      long lastReceiveNs = System.nanoTime();
      for(;;) {
        try {
          Packet packet = Packet.parseDelimitedFrom(in);
          if(packet != null) {
            lastReceiveNs = System.nanoTime();
            if(PacketType.PONG.equals(packet.getType())) {
              log.trace("received pong");
            } else if(PacketType.BYE.equals(packet.getType())) {
              log.trace("received bye");
              break;
            } else {
              int channelId = packet.getChannel();
              Consumer<Packet> channel = getChannel(channelId);
              if(channel != null) {
                channel.accept(packet);
              } else {
                log.debug("ignore packet '{}', channel '{}' does not exist", packet.getType(), channelId);
              }
            }
          }
          if(Utils.isTimeout(lastReceiveNs, serverTimeoutSeconds)) {
            throw new KvdException("receive timeout");
          }
        } catch(SocketTimeoutException e) {
          // ignore
        }
      }
    } catch(EOFException e) {
      log.trace("receive loop EOF");
    } catch(Exception e) {
      log.warn("receive loop failure", e);
    } finally {
      log.trace("receive loop exit");
      onClose.run();
    }
  }

  public void sendAsync(Packet packet) throws InterruptedException {
    // TODO maybe we need a fair lock here? sending ping packets is required to receive pong packets otherwise
    // the client disconnects on not receiving any packets which might happen on large value uploads.
    while(true) {
      if(!closed.get()) {
        if(sendQueue.offer(packet, 1, TimeUnit.SECONDS)) {
          break;
        }
      } else {
        throw new KvdException("already closed");
      }
    }
  }

  public void closeGracefully() {
    closed.set(true);
  }

  public synchronized int createChannel(Consumer<Packet> channelReceiver) {
    channelIds.compareAndSet(Integer.MAX_VALUE, 1);
    int channelId = channelIds.getAndIncrement();
    channelReceivers.put(channelId, channelReceiver);
    return channelId;
  }

  public synchronized void closeChannel(int channelId) {
    try {
      sendAsync(Packets.packet(PacketType.CLOSE_CHANNEL, channelId));
    } catch(InterruptedException e) {
      // ignore
    }
    channelReceivers.remove(channelId);
    this.notifyAll();
  }

  private synchronized Consumer<Packet> getChannel(int channelId) {
    return channelReceivers.get(channelId);
  }

  public synchronized void warnOnOpenChannels() {
    if(channelReceivers.size() > 0) {
      log.warn("client still has '{}' active channel(s)", channelReceivers.size());
    }
  }

  public String getClientId() {
    return clientId;
  }

  public boolean isClosed() {
    return closed.get();
  }

  public void waitForHelloReceived() {
    try {
      helloReceivedFuture.get(serverTimeoutSeconds, TimeUnit.SECONDS);
    } catch(Exception e) {
      throw new KvdException("wait for server hello failed", e);
    }
  }
}
