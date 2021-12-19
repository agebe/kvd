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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

import kvd.common.IOStreamUtils;
import kvd.common.KvdException;
import kvd.server.Key;

@NotThreadSafe
public class BinaryLargeObjectOutputStream extends OutputStream {

  private Key key;

  private ByteBuffer buf;

  private File blobBase;

  private OutputStream blobStream;

  private long blobSize;

  private long blobSplitSize;

  private boolean closed;

  private List<String> blobs = new ArrayList<>();

  public BinaryLargeObjectOutputStream(Key key, File blobBase) {
    this(key, blobBase, 64*1024, Long.MAX_VALUE);
  }

  public BinaryLargeObjectOutputStream(Key key, File blobBase, int blobThreshold) {
    this(key, blobBase, blobThreshold, Long.MAX_VALUE);
  }

  public BinaryLargeObjectOutputStream(Key key, File blobBase,
      int blobThreshold,
      long blobSplitSize) {
    super();
    this.key = key;
    this.blobSplitSize = blobSplitSize;
    buf = ByteBuffer.allocate(blobThreshold);
    this.blobBase = blobBase;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if(closed) {
      throw new IOException("stream closed");
    }
    IOStreamUtils.checkFromIndexSize(b, off, len);
    if(blobStream != null) {
      writeToBlob(b, off, len);
    } else {
      if(buf.remaining() < len) {
        writeToBlob(buf.array(), 0, buf.position());
        writeToBlob(b, off, len);
        buf = null;
      } else {
        buf.put(b, off, len);
      }
    }
  }

  private void writeToBlob(byte[] b, int off, int len) throws IOException {
    if(blobStream == null) {
      newBlob();
    }
    if((blobSize+len) > blobSplitSize) {
      // fill up the blob to capacity
      int len2 = (int)(blobSplitSize - blobSize);
      blobStream.write(b, off, len2);
      blobStream.close();
      newBlob();
      // write the remaining into the new blob (might need to split again)
      int len3 = len-len2;
      writeToBlob(b, off+len2, len3);
    } else {
      blobStream.write(b, off, len);
      blobSize += len;
    }
  }

  private void newBlob() throws IOException {
    String name = UUID.randomUUID().toString();
    File f = new File(blobBase, name);
    blobStream = new BufferedOutputStream(new FileOutputStream(f));
    BlobHeader header = new BlobHeader(blobs.size(), key);
    blobSize = header.writeToStream(blobStream);
    blobs.add(name);
    if(blobSplitSize < blobSize) {
      // silently increase the split size so we can put some content into the file
      // this should be a edge case when either the split size is tiny or the keys are huge or both
      blobSplitSize = blobSize + 64*1024;
    }
  }

  public Value toValue() {
    if(!closed) {
      throw new KvdException("stream not closed");
    }
    return blobs.isEmpty()?inline():blob();
  }

  private Value inline() {
    buf.flip();
    byte[] bytes = new byte[buf.limit()];
    buf.get(bytes);
    return Value.inline(bytes);
  }

  private Value blob() {
    return Value.blob(blobs);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    if(blobStream != null) {
      blobStream.close();
      blobStream = null;
    }
  }

  Key getKey() {
    return key;
  }

}
