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
