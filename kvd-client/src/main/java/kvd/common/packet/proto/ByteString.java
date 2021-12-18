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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteString implements PacketBody {

  private byte[] bytes;

  public ByteString(byte[] bytes) {
    super();
    this.bytes = bytes;
  }

  @Override
  public byte[] toByteArray() {
    return bytes;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(bytes);
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
    ByteString other = (ByteString) obj;
    if (!Arrays.equals(bytes, other.bytes))
      return false;
    return true;
  }

  public String toStringUtf8() {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static ByteString copyFrom(byte[] bytes) {
    return new ByteString(bytes);
  }

  public static ByteString copyFromUtf8(String string) {
    return new ByteString(string.getBytes(StandardCharsets.UTF_8));
  }

}
