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

import java.io.DataOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.Utils;

public class ClientResponseHandler implements Runnable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientResponseHandler.class);

  private DataOutputStream out;

  private BlockingQueue<Packet> sendQueue = new ArrayBlockingQueue<>(100);

  private AtomicBoolean closed = new AtomicBoolean(false);

  public ClientResponseHandler(DataOutputStream out) {
    super();
    this.out = out;
  }

  @Override
  public void run() {
    try {
      while(true) {
        if(isClosed() && sendQueue.isEmpty()) {
          break;
        }
        Packet packet = sendQueue.poll(1, TimeUnit.SECONDS);
        if(packet != null) {
          try {
            packet.write(out);
          } catch(Exception e) {
            throw new KvdException("failed to write packet, " + packet.getType());
          }
        }
      }
    } catch(Exception e) {
      log.error("failed in send loop, exit", e);
      throw new KvdException("failed in send loop", e);
    } finally {
      closed.set(true);
      Utils.closeQuietly(out);
      log.debug("client response thread exit");
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
  }

  public void sendAsync(Packet packet) {
    while(!closed.get()) {
      try {
        if(sendQueue.offer(packet, 1, TimeUnit.SECONDS)) {
          return;
        }
      } catch(Exception e) {
        throw new KvdException("send failed", e);
      }
    }
    throw new KvdException("already closed");
  }

  public boolean isClosed() {
    return closed.get();
  }

}
