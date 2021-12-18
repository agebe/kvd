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
package kvd.common.packet.proto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import kvd.common.KvdException;

public class Packet {

  public static class Builder {

    private PacketType type;

    private int channel;

    private int tx;

    private PacketBody body;

    public Builder setType(PacketType type) {
      this.type = type;
      return this;
    }

    public Builder setChannel(int channel) {
      this.channel = channel;
      return this;
    }

    public Builder setTx(int tx) {
      this.tx = tx;
      return this;
    }

    public Packet build() {
      return new Packet(type, channel, tx, body);
    }

    public Builder setTxBegin(TxBeginBody body) {
      this.body = body;
      return this;
    }

    public Builder setPutInit(PutInitBody body) {
      this.body = body;
      return this;
    }

    public Builder setByteBody(ByteString body) {
      this.body = body;
      return this;
    }

  }

  private PacketType type;

  private int channel;

  private int tx;

  private PacketBody body;

  public Packet(PacketType type, int channel, int tx, PacketBody body) {
    super();
    this.type = type;
    this.channel = channel;
    this.tx = tx;
    this.body = body;
  }

  public PacketType getType() {
    return type;
  }

  public int getChannel() {
    return channel;
  }

  public int getTx() {
    return tx;
  }

  public ByteString getByteBody() {
    return (ByteString)body;
  }

  public PutInitBody getPutInit() {
    return (PutInitBody)body;
  }

  public TxBeginBody getTxBegin() {
    return (TxBeginBody)body;
  }

  public void writeDelimitedTo(OutputStream out) throws IOException {
    int bodyType = getBodyType();
    byte[] bd = body!=null?body.toByteArray():new byte[0];
    ByteBuffer b = ByteBuffer.allocate(4*5+bd.length);
    b.putInt(type.ordinal());
    b.putInt(channel);
    b.putInt(tx);
    b.putInt(bodyType);
    b.putInt(bd.length);
    b.put(bd);
    out.write(b.array());
  }

  private int getBodyType() {
    if(body == null) {
      return 0;
    } else if(body instanceof ByteString) {
      return 1;
    } else if(body instanceof PutInitBody) {
      return 2;
    } else if(body instanceof TxBeginBody) {
      return 3;
    } else {
      throw new KvdException("unknown body type");
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((body == null) ? 0 : body.hashCode());
    result = prime * result + channel;
    result = prime * result + tx;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Packet other = (Packet) obj;
    if (body == null) {
      if (other.body != null)
        return false;
    } else if (!body.equals(other.body))
      return false;
    if (channel != other.channel)
      return false;
    if (tx != other.tx)
      return false;
    if (type != other.type)
      return false;
    return true;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Packet parseDelimitedFrom(InputStream in) throws IOException {
    try {
      PacketType type = PacketType.values()[getInt(in)];
      int channel = getInt(in);
      int tx = getInt(in);
      int bodyType = getInt(in);
      int bodyLength = getInt(in);
      byte[] body = in.readNBytes(bodyLength);
      return new Packet(type, channel, tx, bodyFromBytes(bodyType, body));
    } catch(EndOfStreamException e) {
      return null;
    }
  }

  private static PacketBody bodyFromBytes(int bodyType, byte[] body) {
    if(bodyType == 0) {
      return null;
    } else if(bodyType == 1) {
      return new ByteString(body);
    } else if(bodyType == 2) {
      return new PutInitBody(body);
    } else if(bodyType == 3) {
      return new TxBeginBody(body);
    } else {
      throw new KvdException("unknown body type");
    }
  }

  private static int getInt(InputStream in) throws IOException, EndOfStreamException {
    byte[] buf = in.readNBytes(4);
    if(buf.length < 4) {
      throw new EndOfStreamException();
    }
    ByteBuffer b = ByteBuffer.wrap(buf);
    return b.getInt();
  }

}
