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
package kvd.server;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

import com.google.common.io.BaseEncoding;

public class Key {

  private byte[] key;

  public Key(byte[] key) {
    super();
    this.key = key;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Key other = (Key) obj;
    if (!Arrays.equals(key, other.key))
      return false;
    return true;
  }

  public byte[] getBytes() {
    return key;
  }

  @Override
  public String toString() {
    if(key == null) {
      return "null";
    } else {
      return StringUtils.substring(BaseEncoding.base16().lowerCase().encode(key), 0, 20);
    }
  }

  public static Key of(String s) {
    return new Key(s.getBytes());
  }

  public static Key of(Key base, Key k) {
    byte[] buf = new byte[base.key.length + k.key.length];
    System.arraycopy(base.key, 0, buf, 0, base.key.length);
    System.arraycopy(k.key, 0, buf, base.key.length, k.key.length);
    return new Key(buf);
  }

}
