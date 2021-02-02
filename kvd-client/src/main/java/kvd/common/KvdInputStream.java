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
package kvd.common;

import java.io.IOException;
import java.io.InputStream;

public abstract class KvdInputStream extends InputStream {

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int read = read(b, 0, b.length);
    return read==-1?-1:(int)(b[0] & 0xff);
  }

  @Override
  public abstract int read(byte[] b, int off, int len) throws IOException;

}
