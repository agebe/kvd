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

public class TxBeginBody implements PacketBody {

  public static class Builder {

    private long timeoutMs;

    public Builder setTimeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public TxBeginBody build() {
      return new TxBeginBody(timeoutMs);
    }
  }

  private long timeoutMs;

  public TxBeginBody(long timeoutMs) {
    super();
    this.timeoutMs = timeoutMs;
  }

  public TxBeginBody(byte[] bytes) {
    ByteBuffer b = ByteBuffer.wrap(bytes);
    this.timeoutMs = b.getLong();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (timeoutMs ^ (timeoutMs >>> 32));
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
    TxBeginBody other = (TxBeginBody) obj;
    if (timeoutMs != other.timeoutMs)
      return false;
    return true;
  }

  @Override
  public byte[] toByteArray() {
    ByteBuffer b = ByteBuffer.allocate(8);
    b.putLong(timeoutMs);
    return b.array();
  }

}
