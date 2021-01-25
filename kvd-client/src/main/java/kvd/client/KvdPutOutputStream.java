package kvd.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import kvd.common.ByteRingBuffer;
import kvd.common.IOStreamUtils;
import kvd.common.KvdException;
import kvd.common.Packet;
import kvd.common.PacketType;
import kvd.common.Utils;

public class KvdPutOutputStream extends OutputStream implements Abortable {

  private ClientBackend backend;

  private int channelId;

  private ByteRingBuffer ring = new ByteRingBuffer(16*1024);

  private Consumer<Abortable> closeListener;

  private AtomicBoolean closed = new AtomicBoolean();

  private AtomicBoolean aborted = new AtomicBoolean();

  public KvdPutOutputStream(ClientBackend backend, String key, Consumer<Abortable> closeListener) {
    this.backend = backend;
    this.closeListener = closeListener;
    channelId = backend.createChannel(this::channelReceiver);
    try {
      backend.sendAsync(new Packet(PacketType.PUT_INIT, channelId, Utils.toUTF8(key)));
    } catch(Exception e) {
      throw new KvdException("kvd put failed", e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte)b;
    write(buf);
  }

  private void channelReceiver(Packet packet) {
    if(PacketType.PUT_ABORT.equals(packet.getType())) {
      aborted.set(true);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if(closed.get()) {
      throw new IOException("stream closed");
    }
    if(aborted.get()) {
      abort();
      throw new IOException("stream aborted");
    }
    IOStreamUtils.checkFromIndexSize(off, len, b.length);
    int written = 0;
    for(;;) {
      written += ring.write(b, off+written, len-written);
      if(written < len) {
        flush();
      } else {
        break;
      }
    }
  }

  @Override
  public void flush() throws IOException {
    int used = ring.getUsed();
    if(used > 0) {
      byte[] buf = new byte[used];
      int read = ring.read(buf);
      if(used != read) {
        throw new KvdException(String.format("internal error, read (%s) != used (%s)", read, used));
      }
      try {
        backend.sendAsync(new Packet(PacketType.PUT_DATA, channelId, buf));
      } catch(Exception e) {
        throw new KvdException("flush failed", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
      backend.sendAsync(new Packet(PacketType.PUT_FINISH, channelId));
    } catch(Exception e) {
      throw new KvdException("close failed", e);
    } finally {
      closeInternal();
    }
  }

  @Override
  public void abort() {
    try {
      backend.sendAsync(new Packet(PacketType.PUT_ABORT, channelId));
    } catch(Exception e) {
      throw new KvdException("abort failed", e);
    } finally {
      closeInternal();
    }
  }

  private void closeInternal() {
    closed.set(true);
    backend.closeChannel(channelId);
    this.closeListener.accept(this);
  }

}
