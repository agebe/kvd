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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class ClientHandler implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

  private long clientId;

  private Socket socket;

  private InputStream in;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Map<Integer, ChannelConsumer> channels = new HashMap<>();

  private Map<Integer, Transaction> transactions = new HashMap<>();

  private StorageBackend storage;

  private ClientResponseHandler client;

  private Thread clientThread;

  public ClientHandler(long clientId, Socket socket, StorageBackend storage) {
    this.clientId = clientId;
    this.socket = socket;
    this.storage = storage;
  }

  private synchronized void setupResponseHandler(OutputStream out) {
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
      in = socket.getInputStream();
      setupResponseHandler(socket.getOutputStream());
      Packets.receiveHello(in);
      client.sendAsync(Packets.hello());
      long lastReceiveNs = System.nanoTime();
      while(!closed.get()) {
        Packet packet = Packet.parseDelimitedFrom(in);
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
      client.sendAsync(Packets.packet(PacketType.PONG));
    } else if(PacketType.BYE.equals(packet.getType())) {
      log.debug("client '{}' close received", clientId);
      closeAllChannels();
      rollbackAllTransactions();
      client.sendAsync(Packets.packet(PacketType.BYE));
      closed.set(true);
    } else if(PacketType.PUT_INIT.equals(packet.getType())) {
      int txId = packet.getTx();
      Transaction tx = transactions.get(txId);
      log.debug("put init, txId '{}', tx '{}'", txId, tx);
      if((txId!=0) && (tx==null)) {
        log.warn("received put init for tx '{}' but transaction does not exit", txId);
        client.sendAsync(Packets.packet(PacketType.PUT_ABORT, packet.getChannel()));
      } else {
        createChannel(packet, new PutConsumer(storage, client, tx));
      }
    } else if(
        PacketType.PUT_DATA.equals(packet.getType()) ||
        PacketType.PUT_FINISH.equals(packet.getType()) ||
        PacketType.PUT_ABORT.equals(packet.getType())) {
      int channel = packet.getChannel();
      ChannelConsumer c = channels.get(channel);
      if(c != null) {
        c.accept(packet);
      } else {
        throw new KvdException("channel does not exist " + channel);
      }
    } else if(PacketType.GET_INIT.equals(packet.getType())) {
      int txId = packet.getTx();
      Transaction tx = transactions.get(txId);
      log.debug("get req, txId '{}', tx '{}'", txId, tx);
      if((txId!=0) && (tx==null)) {
        log.warn("received get init for tx '{}' but transaction does not exit", txId);
        client.sendAsync(Packets.packet(PacketType.GET_ABORT, packet.getChannel()));
      } else {
        createChannel(packet, new GetConsumer(clientId, packet.getChannel(), storage, client, tx));
      }
    } else if(PacketType.CLOSE_CHANNEL.equals(packet.getType())) {
      closeChannel(packet.getChannel());
    } else if(PacketType.CONTAINS_REQUEST.equals(packet.getType())) {
      String key = packet.getStringBody().getStr();
      if(Keys.isInternalKey(key)) {
        client.sendAsync(Packets.packet(PacketType.CONTAINS_ABORT, packet.getChannel()));
      } else {
        int txId = packet.getTx();
        Transaction tx = transactions.get(txId);
        log.debug("contains req, txId '{}', tx '{}'", txId, tx);
        if((txId!=0) && (tx==null)) {
          log.warn("received contains request for tx '{}' but transaction does not exit", txId);
          client.sendAsync(Packets.packet(PacketType.CONTAINS_ABORT, packet.getChannel()));
        } else if(tx == null) {
          // TODO change CONTAINS_RESPONSE body to "true" or "false" (StringBody)
          storage.withTransactionVoid(newTx -> {
            boolean contains = storage.contains(newTx, key);
            client.sendAsync(Packets.packet(PacketType.CONTAINS_RESPONSE,
                packet.getChannel(), new byte[] {(contains?(byte)1:(byte)0)}));
          });
        } else {
          boolean contains = storage.contains(tx, key);
          client.sendAsync(Packets.packet(PacketType.CONTAINS_RESPONSE,
              packet.getChannel(), new byte[] {(contains?(byte)1:(byte)0)}));
        }
      }
    } else if(PacketType.REMOVE_REQUEST.equals(packet.getType())) {
      String key = packet.getStringBody().getStr();
      if(Keys.isInternalKey(key)) {
        client.sendAsync(Packets.packet(PacketType.REMOVE_ABORT, packet.getChannel()));
      } else {
        int txId = packet.getTx();
        Transaction tx = transactions.get(txId);
        log.debug("contains req, txId '{}', tx '{}'", txId, tx);
        if((txId!=0) && (tx==null)) {
          log.warn("received remove request for tx '{}' but transaction does not exit", txId);
          client.sendAsync(Packets.packet(PacketType.REMOVE_ABORT, packet.getChannel()));
        } else if(tx == null) {
          storage.withTransactionVoid(Newtx -> {
            boolean removed = storage.remove(Newtx, key);
            client.sendAsync(Packets.packet(PacketType.REMOVE_RESPONSE,
                packet.getChannel(), new byte[] {(removed?(byte)1:(byte)0)}));
          });
        } else {
          boolean removed = storage.remove(tx, key);
          client.sendAsync(Packets.packet(PacketType.REMOVE_RESPONSE,
              packet.getChannel(), new byte[] {(removed?(byte)1:(byte)0)}));
        }
      }
    } else if(PacketType.TX_BEGIN.equals(packet.getType())) {
      Transaction tx = storage.begin();
      int txId = tx.handle();
      if(txId >= 1) {
        transactions.put(txId, tx);
        client.sendAsync(Packets.packet(PacketType.TX_BEGIN, packet.getChannel(), txId));
      } else {
        log.error("wrong txId '{}', must be >= 1", txId);
        client.sendAsync(Packets.packet(PacketType.TX_ABORT, packet.getChannel(), txId));
      }
    } else if(PacketType.TX_COMMIT.equals(packet.getType())) {
      int txId = packet.getTx();
      Transaction tx = transactions.get(txId);
      log.debug("tx commit, txId '{}', tx '{}'", txId, tx);
      if((txId!=0) && (tx==null)) {
        log.warn("received tx commit for txId '{}' but transaction does not exit", txId);
      } else if(txId == 0) {
        log.warn("received tx commit for txId 0 (NO_TX), ignore");
      } else {
        try {
          tx.commit();
        } finally {
          try {
            client.sendAsync(Packets.packet(PacketType.TX_CLOSED, packet.getChannel(), txId));
          } finally {
            transactions.remove(txId);
          }
        }
      }
    } else if(PacketType.TX_ROLLBACK.equals(packet.getType())) {
      int txId = packet.getTx();
      Transaction tx = transactions.get(txId);
      log.debug("tx rollback, txId '{}', tx '{}'", txId, tx);
      if((txId!=0) && (tx==null)) {
        log.warn("received tx rollback for txId '{}' but transaction does not exit", txId);
      } else if(txId == 0) {
        log.warn("received tx rollback for txId 0 (NO_TX), ignore");
      } else {
        try {
          tx.rollback();
        } finally {
          try {
            client.sendAsync(Packets.packet(PacketType.TX_CLOSED, packet.getChannel(), txId));
          } finally {
            transactions.remove(txId);
          }
        }
      }
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

  public long getClientId() {
    return clientId;
  }

  private void closeAllChannels() {
    channels.values().forEach(c -> {
      Utils.closeQuietly(c);
    });
    channels.clear();
  }

  private void rollbackAllTransactions() {
    transactions.values().forEach(tx -> {
      try {
        log.debug("rollback unfinished transaction on close, tdIx '{}'", tx.handle());
        tx.rollback();
      } catch(Exception e) {
        log.warn("tx rollback on close failed", e);
      }
    });
    transactions.clear();
  }

  @Override
  public void close() throws Exception {
    closeAllChannels();
    rollbackAllTransactions();
    Utils.closeQuietly(in);
    Utils.closeQuietly(client);
  }

}
