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
package kvd.server.storage.mapdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import kvd.common.KvdException;

public class Value {

  private ValueType type;

  private byte[] inline;

  private List<String> blobs;

  private Value(ValueType type, byte[] inline) {
    super();
    this.type = type;
    this.inline = inline;
  }

  private Value(List<String> blobs) {
    type = ValueType.BLOB;
    this.blobs = blobs;
  }

  public ValueType getType() {
    return type;
  }

  public byte[] serialize() {
    if(isInline()) {
      return toBytes(inline);
    } else if(isBlob()) {
      return toBytes(serializeBlob());
    } else {
      throw new KvdException("serialize not supported for type, " + type);
    }
  }

  private byte[] toBytes(byte[] content) {
    ByteBuffer b = ByteBuffer.allocate(4+4+content.length);
    b.putInt(type.ordinal());
    b.putInt(content.length);
    b.put(content);
    return b.array();
  }

  private byte[] serializeBlob() {
    ByteBuffer buf = ByteBuffer.allocate(blobs.size() * blobs.get(0).length() * 10);
    buf.putInt(blobs.size());
    blobs.forEach(s -> {
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      buf.putInt(b.length);
      buf.put(b);
    });
    buf.flip();
    byte[] result = new byte[buf.limit()];
    buf.get(result);
    return result;
  }

  public boolean isInline() {
    return ValueType.INLINE.equals(type);
  }

  public boolean isBlob() {
    return ValueType.BLOB.equals(type);
  }

  public byte[] inline() {
    if(isInline()) {
      return inline;
    } else {
      throw new KvdException("expected INLINE type, " + type);
    }
  }

  public List<String> blobs() {
    if(isBlob()) {
      return blobs;
    } else {
      throw new KvdException("expected BLOB type, " + type);
    }
  }

  public static Value inline(byte[] bytes) {
    return new Value(ValueType.INLINE, bytes);
  }

  public static Value blob(List<String> blobs) {
    return new Value(blobs);
  }

  public static Value remove() {
    return new Value(ValueType.REMOVE, null);
  }

  public static Value deserialize(byte[] buf) {
    if(buf != null) {
      try {
        ByteBuffer b = ByteBuffer.wrap(buf);
        int type = b.getInt();
        ValueType vt = ValueType.values()[type];
        if(ValueType.INLINE.equals(vt)) {
          int len = b.getInt();
          byte[] val = new byte[len];
          b.get(val);
          return inline(val);
        } else if(ValueType.BLOB.equals(vt)) {
          return deserializeBlob(b);
        } else {
          throw new KvdException("deserialize not supported for type, " + vt);
        }
      } catch(Exception e) {
        throw new KvdException("failed to deserialize", e);
      }
    } else {
      return null;
    }
  }

  private static Value deserializeBlob(ByteBuffer b) {
    // pop the overall length, not needed here.
    b.getInt();
    int len = b.getInt();
    List<String> blobs = new ArrayList<>();
    for(int i=0;i<len;i++) {
      int blen = b.getInt();
      byte[] buf = new byte[blen];
      b.get(buf);
      String s = new String(buf, StandardCharsets.UTF_8);
      blobs.add(s);
    }
    return blob(blobs);
  }

  public static byte[] serialize(Value v) {
    return v!=null?v.serialize():null;
  }

}
