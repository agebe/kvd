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
package kvd.server.storage.mapdb.expire;

import java.nio.ByteBuffer;

import kvd.server.Key;

public class Timestamp {

  private Key key;

  private long timestamp;

  public Timestamp(Key key, long timestamp) {
    super();
    this.key = key;
    this.timestamp = timestamp;
  }

  Key getKey() {
    return key;
  }

  long getTimestamp() {
    return timestamp;
  }

  public static byte[] serialize(Timestamp t) {
    int size = t.key.getBytes().length+8;
    ByteBuffer buf = ByteBuffer.allocate(size);
    buf.put(t.key.getBytes());
    buf.putLong(t.timestamp);
    buf.flip();
    byte[] dst = new byte[size];
    buf.get(dst);
    return dst;
  }

  public static Timestamp deserialize(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    byte[] keyBuf = new byte[bytes.length-8];
    buf.get(keyBuf);
    long timestamp = buf.getLong();
    return new Timestamp(new Key(keyBuf), timestamp);
  }

}
