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
