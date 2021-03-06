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
import java.io.OutputStream;

import kvd.common.IOStreamUtils;

public class BinaryLargeObjectOutputStream extends OutputStream {

  private BinaryLargeObject a;

  private boolean compact;

  public BinaryLargeObjectOutputStream() {
    this(new BinaryLargeObject(), true);
  }

  public BinaryLargeObjectOutputStream(BinaryLargeObject blob) {
    this(blob, true);
  }

  public BinaryLargeObjectOutputStream(BinaryLargeObject blob, boolean compact) {
    a = blob;
    this.compact = compact;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    IOStreamUtils.checkFromIndexSize(b, off, len);
    a.append(b, off, len);
  }

  public BinaryLargeObject toBinaryLargeObject() {
    return a;
  }

  @Override
  public void close() {
    if(compact) {
      a.compact();
    }
  }

}
