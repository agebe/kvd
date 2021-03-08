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
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class GetConsumer implements ChannelConsumer {

  private static final Logger log = LoggerFactory.getLogger(GetConsumer.class);

  private long clientId;

  private int channel;

  private ClientResponseHandler client;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private Transaction tx;

  private boolean txOwner;

  public GetConsumer(long clientId, int channel, StorageBackend storage, ClientResponseHandler client, Transaction tx) {
    super();
    this.clientId = clientId;
    this.channel = channel;
    this.client = client;
    txOwner = (tx==null);
    this.tx = txOwner?storage.begin():tx;
  }

  @Override
  public void accept(Packet packet) {
    if(PacketType.GET_INIT.equals(packet.getType())) {
      if(channel != packet.getChannel()) {
        throw new KvdException("channel mismatch");
      }
      Thread t = new Thread(() -> {
        try {
          String key = packet.getStringBody().getStr();
          if(Keys.isInternalKey(key)) {
            client.sendAsync(Packets.packet(PacketType.GET_ABORT, channel));
            return;
          }
          try(InputStream in = tx.get(key)) {
            if(in != null) {
              // Send an empty packet so the client can distinguish between
              // non existing keys and keys with an empty value.
              // This is only required on empty values when no other GET_DATA packets are send
              // but to keep things simple here just send it first thing once before the loop.
              client.sendAsync(Packets.packet(PacketType.GET_DATA, channel, new byte[0]));
              while(!closed.get()) {
                byte[] buf = new byte[16*1024];
                int read = in.read(buf);
                if(read < 0) {
                  break;
                } else if(read > 0) {
                  if(read == buf.length) {
                    client.sendAsync(Packets.packet(PacketType.GET_DATA, channel, buf));
                  } else {
                    byte[] send = new byte[read];
                    System.arraycopy(buf, 0, send, 0, read);
                    client.sendAsync(Packets.packet(PacketType.GET_DATA, channel, send));
                  }
                }
              }
            }
          } catch(Exception e) {
            log.debug("get failed", e);
            client.sendAsync(Packets.packet(PacketType.GET_ABORT, channel));
          }
          if(!closed.get()) {
            client.sendAsync(Packets.packet(PacketType.GET_FINISH, channel));
          }
          if(txOwner) {
            tx.commit();
          }
        } catch(Exception e) {
          log.error("get failed", e);
          client.sendAsync(Packets.packet(PacketType.GET_ABORT, channel));
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
    if(txOwner) {
      tx.close();
    }
  }

}
