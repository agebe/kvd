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
package kvd.server.storage;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

public class AbortableOutputStream extends FilterOutputStream {

  private String id;

  private Consumer<String> commit;

  private Consumer<String> rollback;

  private volatile boolean closed;

  public AbortableOutputStream(OutputStream out, String id, Consumer<String> commit, Consumer<String> rollback) {
    super(out);
    this.id = id;
    this.commit = commit;
    this.rollback = rollback;
  }

  public void abort() {
    if(!closed) {
      closed = true;
      rollback.accept(id);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if(closed) {
      throw new IOException("stream closed");
    }
    out.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      try {
        super.close();
      } catch(Exception e) {
        abort();
        throw new IOException("aborted due to exception on close", e);
      } finally {
        commit.accept(id);
      }
    }
  }

}
