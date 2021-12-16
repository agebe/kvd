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

import static kvd.server.storage.mapdb.BinaryLargeObjectOutputStream.BLOB_MAGIC;
import static kvd.server.storage.mapdb.BinaryLargeObjectOutputStream.BLOB_VERSION;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import kvd.common.IOStreamUtils;
import kvd.common.KvdException;
import kvd.common.KvdInputStream;
import kvd.server.Key;

@NotThreadSafe
public class BinaryLargeObjectInputStream extends KvdInputStream {

  private File blobBase;

  private Value v;

  private InputStream inlineStream;

  private InputStream blobStream;

  private int blob;

  private Key key;

  public BinaryLargeObjectInputStream(File blobBase, Value v) throws IOException {
    this.blobBase = blobBase;
    this.v = v;
    if(v.isInline()) {
      inlineStream = new ByteArrayInputStream(v.inline());
    } else if(v.isBlob()) {
      openBlobStream(0);
    } else {
      throw new KvdException("type not supported: " + v.getType());
    }
  }

  private void openBlobStream(int blob) throws IOException {
    String filename = v.blobs().get(blob);
    File f = new File(blobBase, filename);
    blobStream = new BufferedInputStream(new FileInputStream(f));
    // read header
    checkMagic(blobStream.readNBytes(BLOB_MAGIC.length));
    int headerLength = headerLength(blobStream.readNBytes(4));
    if(headerLength <= 0) {
      throw new KvdException("invalid header on BLOB file (wrong length)");
    }
    checkVersion(blobStream.readNBytes(4));
    checkBlobIndex(blob, blobStream.readNBytes(4));
    int keyLength = headerLength(blobStream.readNBytes(4));
    if(keyLength <= 0) {
      throw new KvdException("invalid header on BLOB file (wrong key length)");
    }
    this.key = new Key(blobStream.readNBytes(keyLength));
  }

  private void checkMagic(byte[] buf) {
    for(int i=0;i<BLOB_MAGIC.length;i++) {
      if(buf[i] != BLOB_MAGIC[i]) {
        throw new KvdException("invalid header on BLOB file (magic number mismatch)");
      }
    }
  }

  private int headerLength(byte[] buf) {
    ByteBuffer b = ByteBuffer.wrap(buf);
    return b.getInt();
  }

  private void checkVersion(byte[] buf) {
    ByteBuffer b = ByteBuffer.wrap(buf);
    if(b.getInt() != BLOB_VERSION) {
      throw new KvdException("invalid header on BLOB file (wrong version)");
    }
  }

  private void checkBlobIndex(int blobIndex, byte[] buf) {
    ByteBuffer b = ByteBuffer.wrap(buf);
    if(b.getInt() != blobIndex) {
      throw new KvdException("invalid header on BLOB file (wrong index)");
    }
  }

  private boolean hasBlob(int i) {
    return i < v.blobs().size();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    IOStreamUtils.checkFromIndexSize(buf, off, len);
    if(inlineStream != null) {
      return inlineStream.read(buf, off, len);
    } else if(blobStream != null) {
      int read = blobStream.read(buf, off, len);
      if(read == -1) {
        blobStream.close();
        if(hasBlob(blob+1)) {
          blob++;
          openBlobStream(blob);
          return read(buf, off, len);
        } else {
          return -1;
        }
      } else {
        return read;
      }
    } else {
      throw new KvdException("type not supported");
    }
  }

  @Override
  public int available() throws IOException {
    if(inlineStream != null) {
      return inlineStream.available();
    } else {
      return blobStream.available();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if(inlineStream!=null) {
      inlineStream.close();
    }
    if(blobStream!=null) {
      blobStream.close();
    }
  }

  Key getKey() {
    return key;
  }

}