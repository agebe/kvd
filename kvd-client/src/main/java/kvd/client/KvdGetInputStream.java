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
package kvd.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import kvd.common.ByteRingBuffer;
import kvd.common.IOStreamUtils;
import kvd.common.KvdException;
import kvd.common.KvdInputStream;

class KvdGetInputStream extends KvdInputStream implements Abortable {

  private ByteRingBuffer ring = new ByteRingBuffer(64*1024);

  private AtomicBoolean closed = new AtomicBoolean();

  private AtomicBoolean aborted = new AtomicBoolean();

  private Runnable closeListener;

  public KvdGetInputStream(Runnable closeListener) {
    this.closeListener = closeListener;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    IOStreamUtils.checkFromIndexSize(b, off, len);
    try {
      while(ring.getUsed() == 0) {
        if(aborted.get()) {
          throw new IOException("aborted");
        }
        if(closed.get()) {
          return -1;
        }
        this.wait(1000);
      }
    } catch(InterruptedException e) {
      throw new KvdException("interrupted", e);
    }
    int read = ring.read(b, off, len);
    notifyAll();
    return read;
  }

  public synchronized void fill(byte[] buf) {
    if(buf.length > ring.getSize()) {
      ring.resize(buf.length);
    }
    try {
      while(ring.getFree() < buf.length) {
        if(aborted.get()) {
          return;
        }
        if(closed.get()) {
          return;
        }
        this.wait(1000);
      }
    } catch(InterruptedException e) {
      throw new KvdException("interrupted", e);
    }
    int written = ring.write(buf);
    this.notifyAll();
    if(written != buf.length) {
      throw new KvdException("failed to fill buffer");
    }
  }

  @Override
  public synchronized void abort() {
    aborted.set(true);
    notifyAll();
  }

  @Override
  public synchronized int available() throws IOException {
    return ring.getUsed();
  }

  @Override
  public synchronized void close() {
    closed.set(true);
    closeListener.run();
    notifyAll();
  }

}
