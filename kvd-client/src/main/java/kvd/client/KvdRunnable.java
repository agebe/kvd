package kvd.client;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KvdRunnable implements Runnable {

  private AtomicBoolean run = new AtomicBoolean(true);

  protected ClientBackend backend;

  public KvdRunnable(AtomicBoolean run, ClientBackend backend) {
    super();
    this.run = run;
    this.backend = backend;
  }

  protected boolean isRun() {
    return run.get();
  }

}
