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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class GetConsumer implements ChannelConsumer {

  private static final Logger log = LoggerFactory.getLogger(GetConsumer.class);

  private long clientId;

  private int channel;

  private StorageBackend storage;

  private ClientResponseHandler client;

  private AtomicBoolean closed = new AtomicBoolean(false);

  public GetConsumer(long clientId, int channel, StorageBackend storage, ClientResponseHandler client) {
    super();
    this.clientId = clientId;
    this.channel = channel;
    this.storage = storage;
    this.client = client;
  }

  @Override
  public void accept(Packet packet) {
    if(PacketType.GET_INIT.equals(packet.getType())) {
      if(channel != packet.getChannel()) {
        throw new KvdException("channel mismatch");
      }
      Thread t = new Thread(() -> {
        // FIXME transaction needs to be passed in on object creation
        try(Transaction tx = storage.begin()) {
          String key = Utils.fromUTF8(packet.getBody());
          if(Keys.isInternalKey(key)) {
            client.sendAsync(new Packet(PacketType.GET_ABORT, channel));
            return;
          }
          InputStream in = storage.get(tx, key);
          if(in != null) {
            // Send an empty packet so the client can distinguish between
            // non existing keys and keys with an empty value.
            // This is only required on empty values when no other GET_DATA packets are send
            // but to keep things simple here just send it first thing once before the loop.
            client.sendAsync(new Packet(PacketType.GET_DATA, channel, new byte[0]));
            while(!closed.get()) {
              byte[] buf = new byte[16*1024];
              int read = in.read(buf);
              if(read < 0) {
                break;
              } else if(read > 0) {
                if(read == buf.length) {
                  client.sendAsync(new Packet(PacketType.GET_DATA, channel, buf));
                } else {
                  byte[] send = new byte[read];
                  System.arraycopy(buf, 0, send, 0, read);
                  client.sendAsync(new Packet(PacketType.GET_DATA, channel, send));
                }
              }
            }
          }
          if(!closed.get()) {
            client.sendAsync(new Packet(PacketType.GET_FINISH, channel));
          }
          tx.commit();
        } catch(Exception e) {
          log.error("get failed", e);
        } finally {
          closed.set(true);
        }
      }, "get-" + clientId + "-" + channel);
      t.start();
      
    } else {
      throw new KvdException("unexpected packet type " + packet.getType());
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
  }

}
