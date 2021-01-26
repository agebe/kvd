package kvd.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import kvd.common.ByteRingBuffer;
import kvd.common.IOStreamUtils;
import kvd.common.KvdException;

public class KvdGetInputStream extends InputStream implements Abortable {

  private ByteRingBuffer ring = new ByteRingBuffer(64*1024);

  private AtomicBoolean closed = new AtomicBoolean();

  private AtomicBoolean aborted = new AtomicBoolean();

  private Runnable closeListener;

  public KvdGetInputStream(Runnable closeListener) {
    this.closeListener = closeListener;
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    int read = read(buf);
    if(read == -1) {
      return -1;
    } else {
      return buf[0];
    }
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    IOStreamUtils.checkFromIndexSize(off, len, b.length);
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
