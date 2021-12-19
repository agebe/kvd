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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import kvd.common.KvdException;
import kvd.server.Key;

class BlobHeader {

  static final byte[] BLOB_MAGIC = "KVDBLOB".getBytes(StandardCharsets.US_ASCII);

  static final int BLOB_VERSION = 1;

  private int version;

  private int index;

  private Key key;

  BlobHeader(int index, Key key) {
    this.index = index;
    this.key = key;
    version = BLOB_VERSION;
  }

  private BlobHeader(int version, int index, Key key) {
    super();
    this.version = version;
    this.index = index;
    this.key = key;
  }

  int getVersion() {
    return version;
  }

  int getIndex() {
    return index;
  }

  Key getKey() {
    return key;
  }

  int writeToStream(OutputStream blobStream) throws IOException {
    int headerLength = headerLength(key);
    ByteBuffer header = ByteBuffer.allocate(headerLength);
    header.put(BLOB_MAGIC);
    // header size
    header.putInt(headerLength);
    //version
    header.putInt(BLOB_VERSION);
    // blob index
    header.putInt(index);
    // creation time
    Instant now = Instant.now();
    header.putLong(now.getEpochSecond());
    // add again as reserved field, might use it as access time later
    header.putLong(now.getEpochSecond());
    // key length
    header.putInt(key.getBytes().length);
    // key
    header.put(key.getBytes());
    header.flip();
    byte[] hbuf = new byte[header.limit()];
    header.get(hbuf);
    blobStream.write(hbuf);
    return hbuf.length;
  }

  static BlobHeader fromInputStream(InputStream blobStream) throws IOException {
    checkMagic(blobStream.readNBytes(BLOB_MAGIC.length));
    int headerLength = getInt(blobStream.readNBytes(4));
    if(headerLength <= 0) {
      throw new KvdException("invalid header on BLOB file (wrong length)");
    }
    int version = getInt(blobStream.readNBytes(4));
    int index = getInt(blobStream.readNBytes(4));
//    long created =
        getLong(blobStream.readNBytes(8));
//    long reserved =
        getLong(blobStream.readNBytes(8));
    int keyLength = getInt(blobStream.readNBytes(4));
    if(keyLength <= 0) {
      throw new KvdException("invalid header on BLOB file (wrong key length)");
    }
    if(headerLength != (BLOB_MAGIC.length+32+keyLength)) {
      throw new KvdException("invalid header on BLOB file (length mismatch)");
    }
    Key key = new Key(blobStream.readNBytes(keyLength));
    return new BlobHeader(version, index, key);
  }

  private static void checkMagic(byte[] buf) {
    for(int i=0;i<BLOB_MAGIC.length;i++) {
      if(buf[i] != BLOB_MAGIC[i]) {
        throw new KvdException("invalid header on BLOB file (magic number mismatch)");
      }
    }
  }

  private static int getInt(byte[] buf) {
    ByteBuffer b = ByteBuffer.wrap(buf);
    return b.getInt();
  }

  private static long getLong(byte[] buf) {
    ByteBuffer b = ByteBuffer.wrap(buf);
    return b.getLong();
  }

  public static int headerLength(Key key) {
    return BLOB_MAGIC.length+32+key.getBytes().length;
  }

}
