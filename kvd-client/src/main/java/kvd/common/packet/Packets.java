/*
 * Copyright 2021 Andre Gebers
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
package kvd.common.packet;

import java.io.IOException;
import java.io.InputStream;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.common.packet.proto.ByteString;
import kvd.common.packet.proto.Packet;
import kvd.common.packet.proto.PacketType;

public class Packets {

  public static Packet.Builder builder(PacketType type) {
    return Packet.newBuilder()
        .setType(type);
  }

  public static Packet.Builder builder(PacketType type, int channel) {
    return builder(type)
        .setChannel(channel);
  }

  public static Packet.Builder builder(PacketType type, int channel, int tx) {
    return builder(type, channel)
        .setTx(tx);
  }

  public static Packet packet(PacketType type) {
    return builder(type).build();
  }

  public static Packet packet(PacketType type, int channel) {
    return builder(type, channel).build();
  }

  public static Packet packet(PacketType type, int channel, int tx) {
    return builder(type, channel, tx).build();
  }

  public static Packet packet(PacketType type, int channel, int tx, byte[] body) {
    return builder(type, channel, tx)
        .setByteBody(ByteString.copyFrom(body))
        .build();
  }

  public static Packet packet(PacketType type, int channel, byte[] body) {
    return builder(type, channel)
        .setByteBody(ByteString.copyFrom(body))
        .build();
  }

  public static Packet hello() {
    return packet(PacketType.HELLO, 0, 0, Utils.toUTF8("KvdHello2"));
  }

  public static void receiveHello(InputStream in) throws IOException {
    long lastReceiveNs = System.nanoTime();
    for(;;) {
      Packet p = Packet.parseDelimitedFrom(in);
      if(p != null) {
        if(!"KvdHello2".equals(p.getByteBody().toStringUtf8())) {
          throw new KvdException("hello mismatch");
        } else {
          break;
        }
      }
      if(Utils.isTimeout(lastReceiveNs, 10)) {
        throw new KvdException("timeout waiting for hello packet");
      }
    }
  }

}
