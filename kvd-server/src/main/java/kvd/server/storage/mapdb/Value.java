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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import kvd.common.KvdException;

public class Value {

  private ValueType type;

  private Instant created;

  private Instant accessed;

  private byte[] inline;

  private List<String> blobs;

  private Value(ValueType type) {
    super();
    this.type = type;
  }

  private Value(Instant created, Instant accessed, byte[] inline) {
    super();
    this.type = ValueType.INLINE;
    this.created = created;
    this.accessed = accessed;
    this.inline = inline;
  }

  private Value(Instant created, Instant accessed, List<String> blobs) {
    super();
    this.type = ValueType.BLOB;
    this.created = created;
    this.accessed = accessed;
    this.blobs = blobs;
  }

  public ValueType getType() {
    return type;
  }

  public Instant getCreated() {
    return created;
  }

  void setCreated(Instant created) {
    this.created = created;
  }

  public Instant getAccessed() {
    return accessed;
  }

  void setAccessed(Instant accessed) {
    this.accessed = accessed;
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
    ByteBuffer b = ByteBuffer.allocate(4+8+8+4+content.length);
    b.putInt(type.ordinal());
    b.putLong(created.getEpochSecond());
    b.putLong(accessed.getEpochSecond());
    b.putInt(content.length);
    b.put(content);
    return b.array();
  }

  private byte[] serializeBlob() {
    List<byte[]> lb = blobs.stream()
        .map(s -> {
          byte[] sb = s.getBytes(StandardCharsets.UTF_8);
          ByteBuffer buf = ByteBuffer.allocate(4+sb.length);
          buf.putInt(sb.length);
          buf.put(sb);
          return buf.array();
        })
        .collect(Collectors.toList());
    ByteBuffer buf = ByteBuffer.allocate(4+lb.stream().mapToInt(b -> b.length).sum());
    buf.putInt(lb.size());
    lb.forEach(buf::put);
    return buf.array();
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

  public static Instant now() {
    return Instant.now().truncatedTo(ChronoUnit.SECONDS);
  }

  public static Value inline(byte[] bytes) {
    Instant now = now();
    return new Value(now, now, bytes);
  }

  public static Value blob(List<String> blobs) {
    Instant now = now();
    return new Value(now, now, blobs);
  }

  public static Value remove() {
    return new Value(ValueType.REMOVE);
  }

  public static Value deserialize(byte[] buf) {
    if(buf != null) {
      try {
        ByteBuffer b = ByteBuffer.wrap(buf);
        int type = b.getInt();
        ValueType vt = ValueType.values()[type];
        Instant created = Instant.ofEpochSecond(b.getLong());
        Instant accessed = Instant.ofEpochSecond(b.getLong());
        if(ValueType.INLINE.equals(vt)) {
          int len = b.getInt();
          byte[] inline = new byte[len];
          b.get(inline);
          return new Value(created, accessed, inline);
        } else if(ValueType.BLOB.equals(vt)) {
          return new Value(created, accessed, deserializeBlob(b));
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

  private static List<String> deserializeBlob(ByteBuffer b) {
    // pop the overall length, not needed here.
    b.getInt();
    int size = b.getInt();
    List<String> blobs = new ArrayList<>(size);
    for(int i=0;i<size;i++) {
      int blen = b.getInt();
      byte[] buf = new byte[blen];
      b.get(buf);
      String s = new String(buf, StandardCharsets.UTF_8);
      blobs.add(s);
    }
    return blobs;
  }

  public static byte[] serialize(Value v) {
    return v!=null?v.serialize():null;
  }

  @Override
  public String toString() {
    if(isInline()) {
      return type.toString();
    } else {
      return type.toString() + "/" + blobs!=null?blobs.toString():"";
    }
  }

}
