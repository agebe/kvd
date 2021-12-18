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

import java.nio.ByteBuffer;

public class PutInitBody implements PacketBody {

  public static class Builder {

    private long ttlMs;

    private ByteString key;

    public Builder setTtlMs(long ttlMs) {
      this.ttlMs = ttlMs;
      return this;
    }

    public Builder setKey(ByteString key) {
      this.key = key;
      return this;
    }

    public PutInitBody build() {
      return new PutInitBody(ttlMs, key);
    }
  }

  private long ttlMs;

  private ByteString key;

  public PutInitBody(long ttlMs, ByteString key) {
    super();
    this.ttlMs = ttlMs;
    this.key = key;
  }

  public PutInitBody(byte[] bytes) {
    ByteBuffer b = ByteBuffer.wrap(bytes);
    ttlMs = b.getLong();
    byte[] key = new byte[bytes.length-8];
    b.get(key);
    this.key = new ByteString(key);
  }

  public Long getTtlMs() {
    return ttlMs;
  }

  public ByteString getKey() {
    return key;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + (int) (ttlMs ^ (ttlMs >>> 32));
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
    PutInitBody other = (PutInitBody) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (ttlMs != other.ttlMs)
      return false;
    return true;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer b = ByteBuffer.allocate(8+key.toByteArray().length);
    b.putLong(ttlMs);
    b.put(key.toByteArray());
    return b.array();
  }

}
