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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.Socket;
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
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class ClientBackend {

  private static final Logger log = LoggerFactory.getLogger(ClientBackend.class);

  private String clientId = UUID.randomUUID().toString();

  private Socket socket;

  private BlockingQueue<Packet> sendQueue = new ArrayBlockingQueue<>(100);

  private Map<Integer, Consumer<Packet>> channelReceivers = new HashMap<>();

  private AtomicBoolean closed = new AtomicBoolean(false);

  private AtomicInteger channelIds = new AtomicInteger(1);

  private Thread sendThread;

  private Thread receiveThread;

  private Thread pingThread;

  private Runnable onClose;

  private CompletableFuture<Boolean> helloReceivedFuture = new CompletableFuture<>();

  public ClientBackend(Socket socket, Runnable onClose) {
    this.socket = socket;
    this.onClose = onClose;
  }

  public synchronized void start() {
    if(sendThread == null) {
      sendThread = new Thread(this::sendLoop, "kvd-send-" + clientId);
      sendThread.start();
      receiveThread = new Thread(this::receiveLoop, "kvd-receive-" + clientId);
      receiveThread.start();
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
        // do the sleep first before the ping so the ping is sent after the initial hello packet
        for(int i=0;i<10;i++) {
          if(isClosed()) {
            break;
          }
          Thread.sleep(100);
        }
        try {
          sendAsync(new Packet(PacketType.PING));
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
    try(DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
      while(true) {
        Packet packet = sendQueue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          packet.write(out);
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
    try(DataInputStream in = new DataInputStream(socket.getInputStream())) {
      Utils.receiveHello(in);
      helloReceivedFuture.complete(true);
      log.trace("received hello packet");
      long lastReceiveNs = System.nanoTime();
      while(true) {
        Packet packet = Packet.readNext(in);
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
              log.debug("ignore packet, channel '{}' does not exist", channel);
            }
          }
        }
        if(Utils.isTimeout(lastReceiveNs, 15)) {
          throw new KvdException("receive timeout");
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
    int channelId = channelIds.getAndIncrement();
    channelReceivers.put(channelId, channelReceiver);
    return channelId;
  }

  public synchronized void closeChannel(int channelId) {
    try {
      sendAsync(new Packet(PacketType.CLOSE_CHANNEL, channelId));
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
      helloReceivedFuture.get(1, TimeUnit.MINUTES);
    } catch(Exception e) {
      throw new KvdException("wait for server hello failed", e);
    }
  }
}
