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

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.ByePacket;
import kvd.common.packet.GenericOpPacket;
import kvd.common.packet.HelloPacket;
import kvd.common.packet.OpPacket;
import kvd.common.packet.Packet;
import kvd.common.packet.PacketType;
import kvd.common.packet.PongPacket;
import kvd.server.storage.StorageBackend;

public class ClientHandler implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

  private long clientId;

  private Socket socket;

  private DataInputStream in;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Map<Integer, ChannelConsumer> channels = new HashMap<>();

  private StorageBackend storage;

  private ClientResponseHandler client;

  private Thread clientThread;

  public ClientHandler(long clientId, Socket socket, StorageBackend storage) {
    this.clientId = clientId;
    this.socket = socket;
    this.storage = storage;
  }

  private synchronized void setupResponseHandler(DataOutputStream out) {
    if(client == null) {
      client = new ClientResponseHandler(out);
      clientThread = new Thread(client, "client-resp-" + clientId);
      clientThread.start();
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
        Packet packet = Packet.readNextPacket(in);
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
      try {
        client.close();
        clientThread.join();
        log.trace("client thread joined");
      } catch(Exception clientCloseException) {
        log.warn("close client sender failed", clientCloseException);
      }
      Utils.closeQuietly(this);
      log.info("client id '{}' disconnect", clientId);
    }
  }

  @SuppressWarnings("resource")
  private void handlePacket(Packet packet) {
    if(PacketType.PING.equals(packet.getType())) {
      client.sendAsync(new PongPacket());
    } else if(PacketType.BYE.equals(packet.getType())) {
      log.debug("client '{}' close received", clientId);
      client.sendAsync(new ByePacket());
      closed.set(true);
    } else if(PacketType.PUT_INIT.equals(packet.getType())) {
      createChannel(packet, new PutConsumer(storage, client));
    } else if(
        PacketType.PUT_DATA.equals(packet.getType()) ||
        PacketType.PUT_FINISH.equals(packet.getType()) ||
        PacketType.PUT_ABORT.equals(packet.getType())) {
      int channel = getChannel(packet);
      ChannelConsumer c = channels.get(channel);
      if(c != null) {
        c.accept(packet);
      } else {
        throw new KvdException("channel does not exist " + channel);
      }
    } else if(PacketType.GET_INIT.equals(packet.getType())) {
      createChannel(packet, new GetConsumer(clientId, getChannel(packet), storage, client));
    } else if(PacketType.CLOSE_CHANNEL.equals(packet.getType())) {
      closeChannel(getChannel(packet));
    } else if(PacketType.CONTAINS_REQUEST.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      if(Keys.isInternalKey(key)) {
        client.sendAsync(new GenericOpPacket(PacketType.CONTAINS_ABORT, getChannel(packet)));
      } else {
        storage.withTransactionVoid(tx -> {
          boolean contains = storage.contains(tx, key);
          client.sendAsync(new GenericOpPacket(PacketType.CONTAINS_RESPONSE,
              getChannel(packet), new byte[] {(contains?(byte)1:(byte)0)}));
        });
      }
    } else if(PacketType.REMOVE_REQUEST.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      if(Keys.isInternalKey(key)) {
        client.sendAsync(new GenericOpPacket(PacketType.REMOVE_ABORT, getChannel(packet)));
      } else {
        storage.withTransactionVoid(tx -> {
          boolean removed = storage.remove(tx, key);
          client.sendAsync(new GenericOpPacket(PacketType.REMOVE_RESPONSE,
              getChannel(packet), new byte[] {(removed?(byte)1:(byte)0)}));
        });
      }
    } else {
      log.error("can't handle packet type '{}' (not implemented)", packet.getType());
      throw new KvdException("server error, can't handle packet type " + packet.getType());
    }
  }

  private void createChannel(Packet packet, ChannelConsumer c) {
    int channel = getChannel(packet);
    if(!channels.containsKey(channel)) {
      log.trace("channel opened '{}'", channel);
      channels.put(channel, c);
      c.accept(packet);
    } else {
      throw new KvdException("client error, channel already exists " + channel);
    }
  }

  private void closeChannel(int channel) {
    ChannelConsumer c = channels.remove(channel);
    if(c != null) {
      try {
        c.close();
      } catch(Exception e) {
        log.debug("failed to close channel", e);
      }
    }
  }

  private int getChannel(Packet p) {
    return ((OpPacket)p).getChannel();
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
