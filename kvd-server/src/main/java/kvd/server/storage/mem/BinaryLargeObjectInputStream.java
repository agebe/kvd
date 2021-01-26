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
package kvd.server.storage.mem;

import java.io.IOException;
import java.io.InputStream;

import kvd.common.IOStreamUtils;

public class BinaryLargeObjectInputStream extends InputStream {

  private BinaryLargeObject b;

  private long index;

  public BinaryLargeObjectInputStream(BinaryLargeObject b) {
    this.b = b;
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    read(buf);
    return buf[0];
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    IOStreamUtils.checkFromIndexSize(buf, off, len);
    if(index >= b.size()) {
      return -1;
    }
    int read = Math.min(len, available());
    b.read(index, buf, off, read);
    index += read;
    //System.out.println(String.format("read %s, %s, %s, read %s, new index %s", buf.length, off, len, read, index));
    return read;
  }

  @Override
  public int available() throws IOException {
    return (int)Math.min(Integer.MAX_VALUE, b.size() - index);
  }

}
