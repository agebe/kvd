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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.packet.Packets;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;
import kvd.server.storage.concurrent.AcquireLockException;

public class PutConsumer implements ChannelConsumer {

  private static final Logger log = LoggerFactory.getLogger(PutConsumer.class);

  private AbortableOutputStream out;

  private ClientResponseHandler client;

  private boolean aborted;

  private Transaction tx;

  private boolean txOwner;

  public PutConsumer(StorageBackend storage, ClientResponseHandler client, Transaction tx) {
    super();
    this.client = client;
 // if no transaction has been passed in this put will create a transaction but also needs to commit it on 'PUT_FINISH'
    txOwner = (tx==null);
    this.tx = txOwner?storage.begin():tx;
  }

  @Override
  public void accept(Packet packet) {
    log.trace("receive packet '{}'", packet.getType());
    if(aborted) {
      return;
    }
    if(PacketType.PUT_INIT.equals(packet.getType())) {
      Key key = new Key(packet.getPutInit().getKey().toByteArray());
      if(out != null) {
        throw new KvdException("put already initialized");
      }
      try {
        out = tx.put(key);
        // the client waits for a PUT_INIT or PUT_ABORT response before proceeding
        // PUT_INIT means put init complete normal (no body required)
        client.sendAsync(Packets.packet(PacketType.PUT_INIT, packet.getChannel()));
      } catch(Exception e) {
        if(e instanceof AcquireLockException) {
          log.debug("put init acquire lock failed", e);
        } else {
          log.warn("put init failed with exception", e);
        }
        client.sendAsync(Packets.packet(PacketType.PUT_ABORT, packet.getChannel()));
      }
    } else if(PacketType.PUT_DATA.equals(packet.getType())) {
      if(out != null) {
        try {
          // TODO maybe better to copy bytes
          out.write(packet.getByteBody().toByteArray());
        } catch(Exception e) {
          try {
            out.abort();
          } catch(Exception abortException) {
            log.warn("abort failed", abortException);
          }
          throw new KvdException("failed to write to stream", e);
        }
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else if(PacketType.PUT_FINISH.equals(packet.getType())) {
      if(out != null) {
        try {
          out.close();
          if(txOwner) {
            tx.commit();
          }
          client.sendAsync(Packets.packet(PacketType.PUT_COMPLETE, packet.getChannel()));
        } catch(Exception e) {
          log.warn("failed on close, aborting...", e);
          out.abort();
          if(txOwner) {
            tx.rollback();
          }
          client.sendAsync(Packets.packet(PacketType.PUT_ABORT, packet.getChannel()));
        }
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else if(PacketType.PUT_ABORT.equals(packet.getType())) {
      if(out != null) {
        try {
          out.abort();
          if(txOwner) {
            tx.rollback();
          }
        } catch(Exception e) {
          log.warn("failed on abort", e);
        }
      }
    } else {
      throw new KvdException("not a put packet " + packet.getType());
    }
  }

  @Override
  public void close() throws Exception {
    if(out != null) {
      out.abort();
    }
    if(txOwner && (tx != null)) {
      tx.close();
    }
  }

}
