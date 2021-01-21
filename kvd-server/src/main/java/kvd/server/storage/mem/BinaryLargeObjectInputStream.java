package kvd.server.storage.mem;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

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
    Objects.checkFromIndexSize(off, len, buf.length);
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
