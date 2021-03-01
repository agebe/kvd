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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import kvd.common.Crc8;
import kvd.common.KvdException;

// TODO rewrite this packet stuff again using google protobuf (faster than json and more flexible than this implementation)
public abstract class Packet {

  private byte[] body;

  protected Packet() {
    super();
  }

  protected Packet(byte[] body) {
    this.body = body;
  }

  private void readHeadersFromBytes(byte[] headers) {
    try(DataInputStream in = new DataInputStream(new ByteArrayInputStream(headers))) {
      readHeaders(in);
    } catch(IOException e) {
      throw new KvdException("failed to read headers from bytes", e);
    }
  }

  protected void readHeaders(DataInputStream in) throws IOException {
    // overwrite to read additional headers
  }

  private void readBody(int length, DataInputStream in) {
    if(length > 0) {
      body = new byte[length];
      try {
        in.readFully(body);
      } catch(IOException e) {
        throw new KvdException("failed to read packet body", e);
      }
    } else {
      body = new byte[0];
    }
  }

  public abstract PacketType getType();

  protected void setType(PacketType type) {
    if(!Objects.equals(getType(), type)) {
      throw new KvdException(String.format("packet type mismatch, %s, %s", type, getType()));
    }
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody(byte[] body) {
    this.body = body;
  }

  public void write(OutputStream o) {
    try {
      DataOutputStream out = new DataOutputStream(o);
      byte[] header = writeHeaderToBytes();
      if(header.length > Short.MAX_VALUE) {
        throw new KvdException("header to large, " + header.length);
      }
      out.writeShort(getType().getId());
      // packet length only contains the bytes that follow (not the type or the length itself)
      // header length (short) + header + CRC8 (byte) + body
      int packetLength = 2 + header.length + 1 + (body!=null?body.length:0);
      out.writeInt(packetLength);
      out.writeShort(header.length);
      out.write(header);
      out.write(chksum(getType().getId(), packetLength, header));
      writeBody(out);
      out.flush();
    } catch(Exception e) {
      throw new KvdException("writing packet failed", e);
    }
  }

  public int chksum(int type, int packetLength, byte[] header) {
    Crc8 crc = new Crc8();
    crc.update((byte) (type >> 8));
    crc.update((byte) (type));
    crc.update((byte) (packetLength >> 24));
    crc.update((byte) (packetLength >> 16));
    crc.update((byte) (packetLength >> 8));
    crc.update((byte) (packetLength));
    crc.update((byte) (header.length >> 8));
    crc.update((byte) (header.length));
    for(byte b : header) {
      crc.update(b);
    }
    return (int)crc.getValue();
  }

  private byte[] writeHeaderToBytes() {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try(DataOutputStream out = new DataOutputStream(buf)) {
      writeHeader(out);
    } catch(IOException e) {
      throw new KvdException("writing packet header failed", e);
    }
    return buf.toByteArray();
  }

  protected void writeHeader(DataOutputStream out) throws IOException {
    // overwrite for additional headers
  }

  private void writeBody(DataOutputStream out) {
    if(body != null) {
      try {
        out.write(body);
      } catch(IOException e) {
        throw new KvdException("failed to write packet body", e);
      }
    }
  }

  private void initFromStream(PacketType type, DataInputStream in) {
    setType(type);
    // note the packet type short has already been read so start from the packet length
    try {
      int packetLength = in.readInt();
      int headerlength = in.readUnsignedShort();
      byte[] headers = new byte[headerlength];
      in.readFully(headers);
      int chksum = in.readUnsignedByte();
      if(chksum != chksum(getType().getId(), packetLength, headers)) {
        throw new KvdException(String.format("checksum mismatch"));
      }
      readHeadersFromBytes(headers);
      int bodyLength = packetLength - 2 - headers.length - 1;
      readBody(bodyLength, in);
    } catch(IOException e) {
      throw new KvdException("failed to read packet from input stream", e);
    }
  }

  public static Packet readNextPacket(DataInputStream in) throws EOFException {
    try {
      int packetTypeId = in.readUnsignedShort();
      PacketType pt = PacketType.fromIdOrNull(packetTypeId);
      if(pt == null) {
        // TODO skip this packet
        throw new KvdException("unknown packet type, id " + packetTypeId);
      }
      Packet p = pt.newPacket();
      p.initFromStream(pt, in);
      return p;
    } catch(EOFException e) {
      throw e;
    } catch(IOException e) {
      throw new KvdException("failed to read next packet", e);
    }
  }

}
