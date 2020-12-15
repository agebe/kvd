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
package kvd.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.SocketTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Packet {

  private static final Logger log = LoggerFactory.getLogger(Packet.class);

  private int type;

  private int size;

  private int channel;

  // header check sum crc8
  private int chksum;

  private byte[] body;

  public Packet(PacketType type) {
    this(type, 0, null);
  }

  public Packet(PacketType type, int channel) {
    this(type, channel, null);
  }

  public Packet(PacketType type, int channel, byte[] body) {
    this.type = type.ordinal();
    this.channel = channel;
    if(body != null) {
      this.size = body.length;
      this.body = body;
    } else {
      this.size = 0;
      this.body = new byte[0];
    }
    this.chksum = chksum(this.type, size, channel);
  }

  public Packet(int type, int size, int channel, int chksum, byte[] body) {
    super();
    this.type = type;
    this.size = size;
    this.channel = channel;
    this.chksum = chksum;
    this.body = body;
  }

  public PacketType getType() {
    return PacketType.values()[type];
  }

  public int getChannel() {
    return channel;
  }

  public byte[] getBody() {
    return body;
  }

  public void write(DataOutputStream out) {
    try {
      out.writeInt(type);
      out.writeInt(size);
      out.writeInt(channel);
      out.writeByte(chksum);
      out.write(body);
      out.flush();
    } catch(Exception e) {
      throw new KvdException("writing packet failed", e);
    }
  }

  public static Packet readNext(DataInputStream in) throws EOFException {
    try {
      int type = in.readInt();
      PacketType.ofOrdinal(type);
      int size = in.readInt();
      int channel = in.readInt();
      int chksum = in.readUnsignedByte();
      int chksumCalc = chksum(type, size, channel);
      if(chksumCalc != chksum) {
        throw new KvdException(String.format("checksum mismatch (type %s, size %s, channel %s, chksum %s, chksum calc %s",
            type, size, channel, chksum, chksumCalc));
      }
      byte[] buf = new byte[size];
      in.readFully(buf);
      return new Packet(type, size, channel, chksum, buf);
    } catch(SocketTimeoutException e) {
      // ignore, actually this is only OK when reading the type as otherwise we get out of sync
      log.trace("socket read timeout");
      return null;
    } catch(EOFException e) {
      throw e;
    } catch(Exception e) {
      throw new KvdException("failed to read next packet", e);
    }
  }

  public static int chksum(int type, int size, int channel) {
    Crc8 crc = new Crc8();
    crc.update((byte) (type >> 24));
    crc.update((byte) (type >> 16));
    crc.update((byte) (type >> 8));
    crc.update((byte) (type));
    crc.update((byte) (size >> 24));
    crc.update((byte) (size >> 16));
    crc.update((byte) (size >> 8));
    crc.update((byte) (size));
    crc.update((byte) (channel >> 24));
    crc.update((byte) (channel >> 16));
    crc.update((byte) (channel >> 8));
    crc.update((byte) (channel));
    return (int)crc.getValue();
  }

  public int getSize() {
    return size;
  }

}
