package kvd.server.storage;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

public class AbortableOutputStream extends FilterOutputStream {

  private String id;

  private Consumer<String> commit;

  private Consumer<String> rollback;

  public AbortableOutputStream(OutputStream out, String id, Consumer<String> commit, Consumer<String> rollback) {
    super(out);
    this.id = id;
    this.commit = commit;
    this.rollback = rollback;
  }

  public void abort() {
    rollback.accept(id);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
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
