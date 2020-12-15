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
package kvd.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.HelloPacket;
import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class ClientHandler implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

  private long clientId;

  private Socket socket;

  private DataInputStream in;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Map<Integer, ChannelConsumer> channels = new HashMap<>();

  private Storage storage;

  private ClientResponseHandler client;

  public ClientHandler(long clientId, Socket socket, Storage storage) {
    this.clientId = clientId;
    this.socket = socket;
    this.storage = storage;
  }

  private synchronized void setupResponseHandler(DataOutputStream out) {
    if(client == null) {
      client = new ClientResponseHandler(out);
      Thread t = new Thread(client, "client-resp-" + clientId);
      t.start();
    } else {
      log.warn("client response handler already setup");
    }
  }

  public void run() {
    try {
      // block on socket read for 1 sec only
      socket.setSoTimeout(1000);
      log.info("client connect, id '{}'", clientId);
      in = new DataInputStream(socket.getInputStream());
      setupResponseHandler(new DataOutputStream(socket.getOutputStream()));
      Utils.receiveHello(in);
      client.sendAsync(new HelloPacket());
      long lastReceiveNs = System.nanoTime();
      while(!closed.get()) {
        Packet packet = Packet.readNext(in);
        if(packet != null) {
          lastReceiveNs = System.nanoTime();
          log.trace("received packet " + packet.getType());
          handlePacket(packet);
        } else {
          if(Utils.isTimeout(lastReceiveNs, 10)) {
            log.info("client '{}' timeout", clientId);
            break;
          }
        }
      }
    } catch(Exception e) {
      log.error("client connection failed", e);
    } finally {
      Utils.closeQuietly(this);
      log.info("client id '{}' disconnect", clientId);
    }
  }

  @SuppressWarnings("resource")
  private void handlePacket(Packet packet) {
    if(PacketType.PING.equals(packet.getType())) {
      client.sendAsync(new Packet(PacketType.PONG));
    } else if(PacketType.BYE.equals(packet.getType())) {
      log.debug("client '{}' close received", clientId);
      closed.set(true);
    } else if(PacketType.PUT_INIT.equals(packet.getType())) {
      createChannel(packet, new PutConsumer(storage, client));
    } else if(PacketType.PUT_DATA.equals(packet.getType()) ||
        PacketType.PUT_FINISH.equals(packet.getType())) {
      int channel = packet.getChannel();
      ChannelConsumer c = channels.get(channel);
      if(c != null) {
        c.accept(packet);
      } else {
        throw new KvdException("channel does not exist " + channel);
      }
    } else if(PacketType.GET_INIT.equals(packet.getType())) {
      createChannel(packet, new GetConsumer(clientId, packet.getChannel(), storage, client));
    } else if(PacketType.CLOSE_CHANNEL.equals(packet.getType())) {
      closeChannel(packet);
    } else if(PacketType.CONTAINS_REQUEST.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      boolean contains = storage.contains(key);
      client.sendAsync(new Packet(PacketType.CONTAINS_RESPONSE,
          packet.getChannel(), new byte[] {(contains?(byte)1:(byte)0)}));
    } else if(PacketType.REMOVE_REQUEST.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      boolean removed = storage.remove(key);
      client.sendAsync(new Packet(PacketType.REMOVE_RESPONSE,
          packet.getChannel(), new byte[] {(removed?(byte)1:(byte)0)}));
    } else {
      log.error("can't handle packet type '{}' (not implemented)", packet.getType());
      throw new KvdException("server error, can't handle packet type " + packet.getType());
    }
  }

  private void createChannel(Packet packet, ChannelConsumer c) {
    int channel = packet.getChannel();
    if(!channels.containsKey(channel)) {
      log.trace("channel opened '{}'", channel);
      channels.put(channel, c);
      c.accept(packet);
    } else {
      throw new KvdException("client error, channel already exists " + channel);
    }
  }

  private void closeChannel(Packet packet) {
    if(channels.remove(packet.getChannel()) == null) {
      log.trace("closed channel (did not exist) '{}'", packet.getChannel());
    } else {
      log.trace("closed channel '{}'", packet.getChannel());
    }
  }

  public long getClientId() {
    return clientId;
  }

  @Override
  public void close() throws Exception {
    channels.values().forEach(c -> {
      Utils.closeQuietly(c);
    });
    channels.clear();
    Utils.closeQuietly(in);
    Utils.closeQuietly(client);
  }

}
