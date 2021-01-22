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
import java.util.Objects;

public class BinaryLargeObjectOutputStream extends OutputStream {

  private BinaryLargeObject a;

  public BinaryLargeObjectOutputStream() {
    this(new BinaryLargeObject());
  }

  public BinaryLargeObjectOutputStream(BinaryLargeObject blob) {
    a = blob;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    a.append(b, off, len);
  }

  public BinaryLargeObject toBinaryLargeObject() {
    a.compact();
    return a;
  }

}
