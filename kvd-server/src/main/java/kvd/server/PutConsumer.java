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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.GenericOpPacket;
import kvd.common.packet.OpPacket;
import kvd.common.packet.Packet;
import kvd.common.packet.PacketType;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class PutConsumer implements ChannelConsumer {

  private static final Logger log = LoggerFactory.getLogger(PutConsumer.class);

  private String keyShort;

  private AbortableOutputStream out;

  private StorageBackend storage;

  private ClientResponseHandler client;

  private boolean aborted;

  private Transaction tx;

  public PutConsumer(StorageBackend storage, ClientResponseHandler client) {
    super();
    this.storage = storage;
    this.client = client;
  }

  @Override
  public void accept(Packet packet) {
    log.trace("receive packet '{}'", packet.getType());
    if(aborted) {
      return;
    }
    if(PacketType.PUT_INIT.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      if(StringUtils.isNotBlank(this.keyShort)) {
        throw new KvdException("put already initialized for key " + keyShort);
      }
      if(Keys.isInternalKey(key)) {
        aborted = true;
        client.sendAsync(new GenericOpPacket(PacketType.PUT_ABORT, ((OpPacket)packet).getChannel()));
      } else {
        this.keyShort = StringUtils.substring(key, 0, 200);
        // FIXME i think the transaction needs to be passed into this object on creation time
        tx = storage.begin();
        out = this.storage.put(tx, key);
      }
    } else if(PacketType.PUT_DATA.equals(packet.getType())) {
      if(out != null) {
        try {
          out.write(packet.getBody());
        } catch(Exception e) {
          try {
            out.abort();
          } catch(Exception abortException) {
            log.warn("abort failed", abortException);
          }
          throw new KvdException("failed to write to stream for key "+ this.keyShort, e);
        }
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else if(PacketType.PUT_FINISH.equals(packet.getType())) {
      if(out != null) {
        try {
          out.close();
          tx.commit();
          client.sendAsync(new GenericOpPacket(PacketType.PUT_COMPLETE, ((OpPacket)packet).getChannel()));
        } catch(Exception e) {
          log.warn("failed on close, aborting...", e);
          out.abort();
          tx.rollback();
          client.sendAsync(new GenericOpPacket(PacketType.PUT_ABORT, ((OpPacket)packet).getChannel()));
        }
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else if(PacketType.PUT_ABORT.equals(packet.getType())) {
      if(out != null) {
        try {
          out.abort();
          tx.rollback();
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
    if(tx != null) {
      tx.close();
    }
  }

}
