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

import java.io.OutputStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;
import kvd.server.storage.KeyUtils;
import kvd.server.storage.StorageBackend;

public class PutConsumer implements ChannelConsumer {

  private static final Logger log = LoggerFactory.getLogger(PutConsumer.class);

  private String key;

  private OutputStream out;

  private StorageBackend storage;

  private ClientResponseHandler client;

  public PutConsumer(StorageBackend storage, ClientResponseHandler client) {
    super();
    this.storage = storage;
    this.client = client;
  }

  @Override
  public void accept(Packet packet) {
    log.trace("receive packet '{}'", packet.getType());
    if(PacketType.PUT_INIT.equals(packet.getType())) {
      String key = Utils.fromUTF8(packet.getBody());
      String k = KeyUtils.internalKey(key);
      if(StringUtils.isNotBlank(this.key)) {
        throw new KvdException("put already initialized for key " + key);
      }
      this.key = k;
      out = this.storage.begin(this.key);
    } else if(PacketType.PUT_DATA.equals(packet.getType())) {
      if(out != null) {
        try {
          out.write(packet.getBody());
        } catch(Exception e) {
          throw new KvdException("failed to write to stream for key "+ this.key, e);
        }
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else if(PacketType.PUT_FINISH.equals(packet.getType())) {
      if(out != null) {
        storage.commit(key);
        client.sendAsync(new Packet(PacketType.PUT_COMPLETE, packet.getChannel()));
        this.out = null;
        this.key = null;
      } else {
        throw new KvdException("put has not been initialized yet");
      }
    } else {
      throw new KvdException("not a put packet " + packet.getType());
    }
  }

  @Override
  public void close() throws Exception {
    if(key != null) {
      storage.rollack(key);
    }
  }

}
