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

import kvd.common.KvdException;
import kvd.server.Key;

class BlobHeader {

  static final byte[] BLOB_MAGIC = "KVDBLOB".getBytes(StandardCharsets.US_ASCII);

  static final int BLOB_VERSION = 1;

//  private int length;

  private int version;

  private int index;

  private Key key;

  BlobHeader(int index, Key key) {
    this.index = index;
    this.key = key;
    version = BLOB_VERSION;
//    length = -1;
  }

  private BlobHeader(int length, int version, int index, Key key) {
    super();
//    this.length = length;
    this.version = version;
    this.index = index;
    this.key = key;
  }

//  int getLength() {
//    return length;
//  }

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
    ByteBuffer header = ByteBuffer.allocate(BLOB_MAGIC.length+16+key.getBytes().length);
    header.put(BLOB_MAGIC);
    // header size without magic number
    int length = 16+key.getBytes().length;
    header.putInt(length);
    //version
    header.putInt(BLOB_VERSION);
    // blob index
    header.putInt(index);
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
    int keyLength = getInt(blobStream.readNBytes(4));
    if(keyLength <= 0) {
      throw new KvdException("invalid header on BLOB file (wrong key length)");
    }
    Key key = new Key(blobStream.readNBytes(keyLength));
    return new BlobHeader(headerLength, version, index, key);
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

}
