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

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

public class CompletableOutputStream extends AbortableOutputStream {

  private Consumer<CompletableOutputStream> completeListener;

  private Consumer<CompletableOutputStream> abortListener;

  private OutputStream wrapped;

  private volatile boolean closed;

  public CompletableOutputStream(OutputStream wrapped,
      Consumer<CompletableOutputStream> completeListener,
      Consumer<CompletableOutputStream> abortListener) {
    this.wrapped = wrapped;
    this.completeListener = completeListener;
    this.abortListener = abortListener;
  }

  public OutputStream getWrapped() {
    return wrapped;
  }

  @Override
  public synchronized void abort() {
    if(!closed) {
      try {
        wrapped.close();
      } catch(IOException e) {
        // ignore
      } finally {
        closed = true;
        abortListener.accept(this);
      }
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    if(closed) {
      throw new IOException("stream closed");
    }
    wrapped.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    if(!closed) {
      try {
        wrapped.close();
        completeListener.accept(this);
      } catch(Exception e) {
        abort();
        throw new IOException("aborted due to exception on close", e);
      } finally {
        closed = true;
      }
    }
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if(closed) {
      throw new IOException("stream closed");
    }
    wrapped.write(b);
  }

  @Override
  public synchronized void flush() throws IOException {
    wrapped.flush();
  }

}
