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
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;
import kvd.server.storage.concurrent.AcquireLockException;

public class ClientHandler implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

  private long clientId;

  private Kvd.KvdOptions options;

  private int socketSoTimeoutMs;

  private int clientTimeoutSeconds;

  private Socket socket;

  private InputStream in;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Map<Integer, ChannelConsumer> channels = new HashMap<>();

  private Map<Integer, Tx> transactions = new HashMap<>();

  private StorageBackend storage;

  private ClientResponseHandler client;

  private Thread clientThread;

  private ExecutorService pool = Executors.newCachedThreadPool();

  // how to react to client packets
  private Map<PacketType, Consumer<Packet>> packetConsumers = ImmutableMap.<PacketType, Consumer<Packet>>builder()
      .put(PacketType.PING, this::ping)
      .put(PacketType.BYE, this::bye)
      .put(PacketType.PUT_INIT, this::putInit)
      .put(PacketType.PUT_DATA, this::put)
      .put(PacketType.PUT_FINISH, this::put)
      .put(PacketType.PUT_ABORT, this::put)
      .put(PacketType.GET_INIT, this::getInit)
      .put(PacketType.CLOSE_CHANNEL, this::closeChannel)
      .put(PacketType.CONTAINS_REQUEST, this::containsRequest)
      .put(PacketType.REMOVE_REQUEST, this::removeRequest)
      .put(PacketType.TX_BEGIN, this::txBegin)
      .put(PacketType.TX_COMMIT, this::txCommit)
      .put(PacketType.TX_ROLLBACK, this::txRollback)
      .put(PacketType.LOCK, this::lockRequest)
      .put(PacketType.REMOVEALL_REQUEST, this::removeAllRequest)
      .build();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private Map<Integer, ScheduledFuture<?>> txTimeouts = new HashMap<>();

  public ClientHandler(long clientId,
      Kvd.KvdOptions options,
      int socketSoTimeoutMs,
      int clientTimeoutSeconds,
      Socket socket,
      StorageBackend storage) {
    this.options = options;
    this.socketSoTimeoutMs = socketSoTimeoutMs;
    this.clientTimeoutSeconds = clientTimeoutSeconds;
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
      socket.setSoTimeout(socketSoTimeoutMs);
      log.info("client connect, id '{}'", clientId);
      in = socket.getInputStream();
      setupResponseHandler(socket.getOutputStream());
      Packets.receiveHello(in);
      client.sendAsync(Packets.hello());
      long lastReceiveNs = System.nanoTime();
      while(!closed.get()) {
        try {
          Packet packet = Packet.parseDelimitedFrom(in);
          if(packet != null) {
            lastReceiveNs = System.nanoTime();
            log.trace("received packet " + packet.getType());
            handlePacket(packet);
          } else {
            if(Utils.isTimeout(lastReceiveNs, clientTimeoutSeconds)) {
              log.info("client '{}' timeout", clientId);
              break;
            }
          }
        } catch(SocketTimeoutException e) {
          // ignore
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

  private void ping(Packet packet) {
    client.sendAsync(Packets.packet(PacketType.PONG));
  }

  private void bye(Packet packet) {
    log.debug("client '{}' close received", clientId);
    closeAllChannels();
    rollbackAllTransactions();
    client.sendAsync(Packets.packet(PacketType.BYE));
    closed.set(true);
  }

  private void putInit(Packet packet) {
    int txId = packet.getTx();
    Tx tx = transactions.get(txId);
    log.debug("put init, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received put init for tx '{}' but transaction does not exit", txId);
      client.sendAsync(Packets.packet(PacketType.PUT_ABORT, packet.getChannel()));
    } else {
      final PutConsumer c = new PutConsumer(
          storage,
          client,
          (tx!=null?tx.getTransaction():null),
          options.logAccess);
      createChannel(packet, c);
      // execute async as this might block
      pool.execute(() -> c.accept(packet));
    }
  }

  private void put(Packet packet) {
    int channel = packet.getChannel();
    ChannelConsumer c = channels.get(channel);
    if(c != null) {
      c.accept(packet);
    } else {
      throw new KvdException("channel does not exist " + channel);
    }
  }

  private void getInit(Packet packet) {
    int txId = packet.getTx();
    Tx tx = transactions.get(txId);
    log.debug("get req, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received get init for tx '{}' but transaction does not exit", txId);
      client.sendAsync(Packets.packet(PacketType.GET_ABORT, packet.getChannel()));
    } else {
      final GetConsumer c = new GetConsumer(
          packet.getChannel(),
          storage,
          client,
          (tx!=null?tx.getTransaction():null),
          options.logAccess);
      createChannel(packet, c);
      pool.execute(() -> c.accept(packet));
    }
  }

  private void closeChannel(Packet packet) {
    ChannelConsumer c = channels.remove(packet.getChannel());
    if(c != null) {
      try {
        c.close();
      } catch(Exception e) {
        log.debug("failed to close channel", e);
      }
    }
  }

  private void containsRequest(Packet packet) {
    Key key = new Key(packet.getByteBody().toByteArray());
    int txId = packet.getTx();
    Tx tx = transactions.get(txId);
    log.debug("contains req, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received contains request for tx '{}' but transaction does not exit", txId);
      client.sendAsync(Packets.packet(PacketType.CONTAINS_ABORT, packet.getChannel()));
    } else {
      pool.execute(() -> containsRequest(packet, tx!=null?tx.getTransaction():null, key));
    }
  }

  private void containsRequest(Packet packet, Transaction tx, Key key) {
    try {
      log.trace("execute contains, tx '{}', key '{}'", tx, key);
      boolean contains = contains(tx, key);
      // race: if there was no outer transaction the step transaction must be committed before sending out the response
      client.sendAsync(Packets.packet(PacketType.CONTAINS_RESPONSE,
          packet.getChannel(), new byte[] {(contains?(byte)1:(byte)0)}));
      log.trace("done execute contains, tx '{}', key '{}', contains '{}'", tx, key, contains);
    } catch(Exception e) {
      if(e instanceof AcquireLockException) {
        log.trace("contains failed", e);
      } else {
        log.warn("contains failed", e);
      }
      client.sendAsync(Packets.packet(PacketType.CONTAINS_ABORT, packet.getChannel()));
    }
  }

  private void logAccess(String type, Key key, Transaction tx) {
    if(options.logAccess) {
      log.info("{} '{}' / tx '{}'", type, key, tx.handle());
    }
  }

  private boolean contains(Transaction tx, Key key) {
    if(tx!=null) {
      logAccess("contains", key, tx);
      return tx.contains(key);
    } else {
      return storage.withTransaction(newTx -> {
        logAccess("contains", key, newTx);
        return newTx.contains(key);
      });
    }
  }

  private void removeRequest(Packet packet) {
    Key key = new Key(packet.getByteBody().toByteArray());
    int txId = packet.getTx();
    Tx tx = transactions.get(txId);
    log.debug("remove req, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received remove request for tx '{}' but transaction does not exit", txId);
      client.sendAsync(Packets.packet(PacketType.REMOVE_ABORT, packet.getChannel()));
    } else {
      pool.execute(() -> removeRequest(packet, tx!=null?tx.getTransaction():null, key));
    }
  }

  private void removeRequest(Packet packet, Transaction tx, Key key) {
    try {
      boolean removed = remove(tx, key);
      // race: if there was no outer transaction the step transaction must be committed before sending out the response
      client.sendAsync(Packets.packet(PacketType.REMOVE_RESPONSE,
          packet.getChannel(), new byte[] {(removed?(byte)1:(byte)0)}));
    } catch(Exception e) {
      if(e instanceof AcquireLockException) {
        log.trace("remove failed", e);
      } else {
        log.warn("remove failed", e);
      }
      client.sendAsync(Packets.packet(PacketType.REMOVE_ABORT, packet.getChannel()));
    }
  }

  private boolean remove(Transaction tx, Key key) {
    if(tx!=null) {
      logAccess("remove", key, tx);
      return tx.remove(key);
    } else {
      return storage.withTransaction(newTx -> {
        logAccess("remove", key, newTx);
        return newTx.remove(key);
      });
    }
  }

  private void lockRequest(Packet packet) {
    Key key = new Key(packet.getByteBody().toByteArray());
    int txId = packet.getTx();
    Tx tx = transactions.get(txId);
    log.debug("lock req, txId '{}', tx '{}'", txId, tx);
    if(tx==null) {
      // can only lock key within transaction
      client.sendAsync(Packets.packet(PacketType.ABORT, packet.getChannel()));
    } else {
      pool.execute(() -> lockRequest(packet, tx!=null?tx.getTransaction():null, key));
    }
  }

  private void lockRequest(Packet packet, Transaction tx, Key key) {
    try {
      boolean locked = tx.lock(key);
      client.sendAsync(Packets.packet(PacketType.LOCK,
          packet.getChannel(), new byte[] {(locked?(byte)1:(byte)0)}));
    } catch(Exception e) {
      if(e instanceof AcquireLockException) {
        log.trace("remove failed", e);
      } else {
        log.warn("remove failed", e);
      }
      client.sendAsync(Packets.packet(PacketType.ABORT, packet.getChannel()));
    }
  }

  private void removeAllRequest(Packet packet) {
    log.debug("remove all req");
    pool.execute(() -> removeAll(packet));
  }

  private void removeAll(Packet packet) {
    log.debug("remove all");
    // TODO abort all transactions
    try {
      storage.withTransactionVoid(tx -> {
        tx.removeAll();
        boolean removed = true;
        client.sendAsync(Packets.packet(PacketType.REMOVE_RESPONSE,
            packet.getChannel(), new byte[] {(removed?(byte)1:(byte)0)}));
      });
    } catch(Exception e) {
      client.sendAsync(Packets.packet(PacketType.REMOVE_ABORT, packet.getChannel()));
    }
  }

  private synchronized void txBegin(Packet packet) {
    Transaction tx = storage.begin();
    int txId = tx.handle();
    if(txId >= 1) {
      transactions.put(txId, new Tx(txId, packet.getChannel(), tx));
      long timeoutMs = packet.getTxBegin().getTimeoutMs();
      if(timeoutMs > 0) {
        ScheduledFuture<?> f = scheduler.schedule(() -> {
          txAbort(txId);
        }, timeoutMs, TimeUnit.MILLISECONDS);
        txTimeouts.put(txId, f);
      }
      client.sendAsync(Packets.packet(PacketType.TX_BEGIN, packet.getChannel(), txId));
    } else {
      log.error("wrong txId '{}', must be >= 1", txId);
      client.sendAsync(Packets.packet(PacketType.TX_ABORT, packet.getChannel(), txId));
    }
  }

  private synchronized void txAbort(Integer txId) {
    Tx tx = transactions.get(txId);
    if(tx == null) {
      // already gone, ignore
      return;
    }
    log.debug("aborting transaction '{}'", txId);
    client.sendAsync(Packets.packet(PacketType.TX_ABORT, tx.getChannel(), txId));
    txRollback(txId);
  }

  private void txCommit(Packet packet) {
    txCommit(packet.getTx());
  }

  private synchronized void txCommit(int txId) {
    Tx tx = transactions.get(txId);
    log.debug("tx commit, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received tx commit for txId '{}' but transaction does not exit", txId);
    } else if(txId == 0) {
      log.warn("received tx commit for txId 0 (NO_TX), ignore");
    } else {
      try {
        tx.getTransaction().commit();
      } finally {
        try {
          client.sendAsync(Packets.packet(PacketType.TX_CLOSED, tx.getChannel(), txId));
        } finally {
          transactions.remove(txId);
          Future<?> f = txTimeouts.get(txId);
          if(f != null) {
            txTimeouts.remove(txId);
            f.cancel(false);
          }
        }
      }
    }
  }

  private void txRollback(Packet packet) {
    txRollback(packet.getTx());
  }

  private synchronized void txRollback(int txId) {
    Tx tx = transactions.get(txId);
    log.debug("tx rollback, txId '{}', tx '{}'", txId, tx);
    if((txId!=0) && (tx==null)) {
      log.warn("received tx rollback for txId '{}' but transaction does not exit", txId);
    } else if(txId == 0) {
      log.warn("received tx rollback for txId 0 (NO_TX), ignore");
    } else {
      try {
        tx.getTransaction().rollback();
      } finally {
        try {
          client.sendAsync(Packets.packet(PacketType.TX_CLOSED, tx.getChannel(), txId));
        } finally {
          transactions.remove(txId);
          Future<?> f = txTimeouts.get(txId);
          if(f != null) {
            txTimeouts.remove(txId);
            f.cancel(false);
          }
        }
      }
    }
  }

  private void handlePacket(Packet packet) {
    Consumer<Packet> c = packetConsumers.get(packet.getType());
    if(c != null) {
      c.accept(packet);
    } else {
      log.error("can't handle packet type '{}' (not implemented)", packet.getType());
      client.sendAsync(Packets.packet(PacketType.INVALID_REQUEST, packet.getChannel()));
      throw new KvdException("server error, can't handle packet type " + packet.getType());
    }
  }

  private void createChannel(Packet packet, ChannelConsumer c) {
    int channel = packet.getChannel();
    if(!channels.containsKey(channel)) {
      log.trace("channel opened '{}'", channel);
      channels.put(channel, c);
    } else {
      throw new KvdException("client error, channel already exists " + channel);
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
        log.debug("rollback unfinished transaction on close, tdIx '{}'", tx.getTxId());
        tx.getTransaction().rollback();
      } catch(Exception e) {
        log.warn("tx rollback on close failed", e);
      }
    });
    transactions.clear();
  }

  private void shutdownTxTimeouts() {
    try {
      try {
        txTimeouts.values().forEach(f -> f.cancel(true));
      } catch(Exception e) {
        log.warn("failed to cancel tx timeout", e);
      }
      scheduler.shutdown();
    } catch(Exception e) {
      log.warn("failed to shutdown tx timeouts", e);
    }
  }

  @Override
  public void close() throws Exception {
    closeAllChannels();
    rollbackAllTransactions();
    shutdownTxTimeouts();
    Utils.closeQuietly(in);
    Utils.closeQuietly(client);
    pool.shutdown();
  }

}
