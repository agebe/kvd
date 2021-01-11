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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.HelloPacket;
import kvd.common.HostAndPort;
import kvd.common.JoiningOutputStream;
import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

/**
 * {@code KvdClient} is the public API that clients should use to interact with the server.
 *
 * <p>Example usage:<pre>
 *  try(KvdClient client = new KvdClient("kvd.example.com:3030")) {
 *    try(DataOutputStream out = new DataOutputStream(client.put("test"))) {
 *      out.writeLong(42);
 *    }
 *  }
 * </pre>
 *
 * <p>Note: {@link KvdClient#KvdClient(java.lang.String)} establishes a single socket connection to the server
 * that it keeps alive until {@link KvdClient#close} is called.
 *
 * <p>Note: {@code KvdClient} is thread-safe.
 */
public class KvdClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(KvdClient.class);

  private ClientBackend backend;

  private Set<AutoCloseable> closeables = new HashSet<>();

  private AtomicBoolean closed = new AtomicBoolean(false);

  private AtomicBoolean run = new AtomicBoolean(true);

  /**
   * Establishes the connection to the server.
   * @param serverAddress in the form
   *        <a href="https://guava.dev/releases/30.1-jre/api/docs/com/google/common/net/HostAndPort.html">{@code host:port}</a>.
   *        Port can be omitted and 3030 is used in this case.
   */
  public KvdClient(String serverAddress) {
    try {
      HostAndPort hp = HostAndPort.fromString(serverAddress).withDefaultPort(3030);
      log.trace("connecting to '{}'", hp);
      Socket socket = new Socket(InetAddress.getByName(hp.getHost()), hp.getPort());
      socket.setSoTimeout(1000);
      backend = new ClientBackend(socket, () -> {
        // close the client when the server connection closes
        ForkJoinPool.commonPool().execute(() -> {
          try {
            log.trace("close client");
            close();
          } catch(Exception e) {
            // ignore
          }
        });
      });
      backend.start();
      backend.sendAsync(new HelloPacket());
    } catch(Exception e) {
      throw new KvdException(String.format("failed to connect to '%s'", serverAddress), e);
    }
  }

  private synchronized void addCloseable(AutoCloseable c) {
    closeables.add(c);
  }

  private synchronized void removeCloseable(AutoCloseable c) {
    closeables.remove(c);
  }

  private void checkClosed() {
    if(isClosed()) {
      throw new KvdException("closed");
    }
  }

  /**
   * Put a new value or replace an existing.
   * @param key key with which the specified value is to be associated
   * @return {@code OutputStream} to be used to stream the value in.
   *         Close the {@code OutputStream} to signal that the value is complete.
   */
  public OutputStream put(String key) {
    checkClosed();
    Utils.checkKey(key);
    try {
      final PipedOutputStream src = new PipedOutputStream();
      final PipedInputStream in = new PipedInputStream(src, 64 * 1024);
      ThreadCloseable tc = new ThreadCloseable(src);
      Thread t = new Thread(() -> {
        int channelId = 0;
        try {
          channelId = backend.createChannel();
          backend.sendAsync(new Packet(PacketType.PUT_INIT, channelId, Utils.toUTF8(key)));
          while(isRun()) {
            byte[] buf = new byte[16*1024];
            int read = in.read(buf);
            if(read < 0) {
              break;
            } else if(read > 0) {
              if(read == buf.length) {
                backend.sendAsync(new Packet(PacketType.PUT_DATA, channelId, buf));
              } else {
                byte[] send = new byte[read];
                System.arraycopy(buf, 0, send, 0, read);
                backend.sendAsync(new Packet(PacketType.PUT_DATA, channelId, send));
              }
            }
          }
          backend.sendAsync(new Packet(PacketType.PUT_FINISH, channelId));
          BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
          while(isRun()) {
            Packet packet = queue.poll(1, TimeUnit.SECONDS);
            if(packet != null) {
              if(PacketType.PUT_COMPLETE.equals(packet.getType())) {
                log.trace("put complete");
                break;
              } else {
                throw new KvdException("received unexpected packet " + packet.getType());
              }
            }
          }
        } catch(Exception e) {
          log.warn("put failure", e);
        } finally {
          backend.closeChannel(channelId);
          Utils.closeQuietly(in);
          Utils.closeQuietly(src);
          ForkJoinPool.commonPool().execute(() -> removeCloseable(tc));
        }
      }, "kvd-put-" + backend.getClientId());
      tc.setThread(t);
      addCloseable(tc);
      t.start();
      return new JoiningOutputStream(src, t);
    } catch(IOException e) {
      throw new KvdException("put failed", e);
    }
  }

  /**
   * Returns the value to which the specified key is mapped
   * @param key the key whose associated value is to be returned
   * @return {@code Future} that evaluates either to an {@code InputStream} for keys that exist
   *         or {@code null} for keys that don't exist on the server.
   */
  public Future<InputStream> getAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    final CompletableFuture<InputStream> future = new CompletableFuture<>();
    ThreadCloseable tc = new ThreadCloseable();
    Thread t = new Thread(() -> {
      int channelId = 0;
      try(PipedOutputStream src = new PipedOutputStream()) {
        channelId = backend.createChannel();
        backend.sendAsync(new Packet(PacketType.GET_INIT, channelId, Utils.toUTF8(key)));
        BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
        while(isRun()) {
          Packet packet = queue.poll(1, TimeUnit.SECONDS);
          if(packet != null) {
            if(PacketType.GET_DATA.equals(packet.getType())) {
              if(!future.isDone()) {
                future.complete(new PipedInputStream(src, 64 * 1024));
              }
              src.write(packet.getBody());
            } else if(PacketType.GET_FINISH.equals(packet.getType())) {
              break;
            } else {
              throw new KvdException("received unexpected packet " + packet.getType());
            }
          }
        }
      } catch(Exception e) {
        log.warn("get failure", e);
      } finally {
        if(!future.isDone()) {
          future.complete(null);
        }
        backend.closeChannel(channelId);
        ForkJoinPool.commonPool().execute(() -> removeCloseable(tc));
      }
    }, "kvd-get-" + backend.getClientId());
    tc.setThread(t);
    addCloseable(tc);
    t.start();
    return future;
  }

  /**
   * Convenience method that calls {@getAsync} and waits for the {@code Future} to complete.
   * @param key key the key whose associated value is to be returned
   * @return the {@code InputStream} for keys that exist or {@code null} for keys that don't exist on the server.
   */
  public InputStream get(String key) {
    try {
      return getAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("get failed");
    }
  }

  /**
   * Convenience method that puts a {@code String} value.
   * <p>Note: Uses {@code DataOutputStream::writeUTF} to write the String
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   */
  public void putString(String key, String value) {
    if(value == null) {
      throw new KvdException("null string value not supported");
    }
    try(DataOutputStream out = new DataOutputStream(put(key))) {
      out.writeUTF(value);
    } catch(IOException e) {
      throw new KvdException("put string failed", e);
    }
  }

  /**
   * Convenience method that gets a {@code String} value.
   * <p>Note: Uses {@code DataInputStream::readUTF} to read the String
   * @param key the key whose associated value is to be returned
   * @return {@code String} value that is associated with the key or {@code null}
   *         if the key does not exist on the server.
   */
  public String getString(String key) {
    InputStream i = get(key);
    if(i != null) {
      try(DataInputStream in = new DataInputStream(i)) {
        return in.readUTF();
      } catch(IOException e) {
        throw new KvdException("get string failed", e);
      }
    } else {
      return null;
    }
  }

  /**
   * The returned {@code Future} evaluates to true if the key exists on the server, false otherwise
   * @param key The key whose presence is to be tested
   * @return {@code Future} evaluates to {@code true} if the key exists on the server, {@code false} otherwise
   */
  public Future<Boolean> containsAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    final CompletableFuture<Boolean> future = new CompletableFuture<>();
    FutureCloseable c = new FutureCloseable(future);
    Thread t = new Thread(() -> {
      int channelId = 0;
      try {
        channelId = backend.createChannel();
        backend.sendAsync(new Packet(PacketType.CONTAINS_REQUEST, channelId, Utils.toUTF8(key)));
        BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
        while(isRun()) {
          Packet packet = queue.poll(1, TimeUnit.SECONDS);
          if(packet != null) {
            if(PacketType.CONTAINS_RESPONSE.equals(packet.getType())) {
              byte[] buf = packet.getBody();
              if((buf != null) && (buf.length >= 1)) {
                future.complete((buf[0] == 1));
              } else {
                throw new KvdException("invalid response");
              }
              break;
            } else {
              throw new KvdException("received unexpected packet " + packet.getType());
            }
          }
        }
      } catch(Exception e) {
        log.warn("contains failure", e);
        future.completeExceptionally(e);
      } finally {
        backend.closeChannel(channelId);
        ForkJoinPool.commonPool().execute(() -> removeCloseable(c));
      }
    }, "kvd-contains-" + backend.getClientId());
    c.setThread(t);
    addCloseable(c);
    t.start();
    return future;
  }

  /**
   * Returns true if a mapping for the specified key exists on the server.
   * @param key the key whose presence is to be tested
   * @return {@code true} if the key exists on the server, {@code false} otherwise.
   */
  public boolean contains(String key) {
    try {
      return containsAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("contains failed", e);
    }
  }

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key whose mapping is to be removed
   * @return {@code Future} which evaluates to {@code true} if the key/value was removed from the server,
   *         @{code false} otherwise.
   */
  public Future<Boolean> removeAsync(String key) {
    checkClosed();
    Utils.checkKey(key);
    final CompletableFuture<Boolean> future = new CompletableFuture<>();
    FutureCloseable c = new FutureCloseable(future);
    Thread t = new Thread(() -> {
      int channelId = 0;
      try {
        channelId = backend.createChannel();
        backend.sendAsync(new Packet(PacketType.REMOVE_REQUEST, channelId, Utils.toUTF8(key)));
        BlockingQueue<Packet> queue = backend.getReceiveChannel(channelId);
        while(isRun()) {
          Packet packet = queue.poll(1, TimeUnit.SECONDS);
          if(packet != null) {
            if(PacketType.REMOVE_RESPONSE.equals(packet.getType())) {
              byte[] buf = packet.getBody();
              if((buf != null) && (buf.length >= 1)) {
                future.complete((buf[0] == 1));
              } else {
                throw new KvdException("invalid response");
              }
              break;
            } else {
              throw new KvdException("received unexpected packet " + packet.getType());
            }
          }
        }
      } catch(Exception e) {
        log.warn("remove failure", e);
        future.completeExceptionally(e);
      } finally {
        backend.closeChannel(channelId);
        ForkJoinPool.commonPool().execute(() -> removeCloseable(c));
      }
    }, "kvd-remove-" + backend.getClientId());
    c.setThread(t);
    addCloseable(c);
    t.start();
    return future;
  }

  /**
   * Removes the mapping for the specified key from the server.
   * @param key key key whose mapping is to be removed
   * @return {@code true} if the key/value was removed from the server, {@code false} otherwise.
   */
  public boolean remove(String key) {
    try {
      return removeAsync(key).get();
    } catch(Exception e) {
      throw new KvdException("remove failed", e);
    }
  }

  /**
   * Waits for pending requests to finish and closes the connection to the server. Once closed this instance
   * can't be reused and must be discarded.
   */
  @Override
  public synchronized void close() {
    if(!closed.get()) {
      closed.set(true);
      closeables.forEach(c -> {
        try {
          c.close();
        } catch(Exception e) {
          // ignore
        }
      });
      closeables.clear();
      try {
        backend.sendAsync(new Packet(PacketType.BYE));
      } catch(Exception e) {
        // ignore
      }
      backend.closeGracefully();
      run.set(false);
    }
  }

  private boolean isRun() {
    return run.get();
  }

  /**
   * Check if the KvdClient can still be used.
   * @return {@code true} if the instance is closed, {@code false} otherwise.
   */
  public boolean isClosed() {
    return closed.get();
  }

}
