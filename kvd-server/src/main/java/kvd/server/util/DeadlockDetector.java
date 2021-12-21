package kvd.server.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadlockDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadlockDetector.class);

  private Thread t;

  private AtomicBoolean stop = new AtomicBoolean();

  // https://stackoverflow.com/a/217701
  private ThreadInfo[] detectDeadlock() {
    ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
    long[] ids = tmx.findDeadlockedThreads();
    return (ids != null)?tmx.getThreadInfo(ids, true, true):null; 
  }

  public synchronized void start(long intervalMs, Consumer<ThreadInfo[]> consumer) {
    if(t != null) {
      return;
    }
    Runnable r = () -> {
      log.info("started");
      try {
        while(!stop.get()) {
          try {
            ThreadInfo[] ti = detectDeadlock();
            if(ti != null) {
              String msg = "thread deadlock detected\n";
              msg += Arrays.stream(ti)
                  .filter(Objects::nonNull)
                  .map(threadinfo -> threadinfo.toString())
                  .collect(Collectors.joining("\n"));
              log.error(msg);
              consumer.accept(ti);
            }
          } catch(Throwable t) {
            log.error("exception in deadlock detector", t);
          }
          try {
            Thread.sleep(intervalMs);
          } catch(InterruptedException e) {
            break;
          }
        }
      } finally {
        log.info("stopped");
      }
    };
    t = new Thread(r, "deadlock-detector");
    t.setDaemon(true);
    t.start();
  }

  public synchronized void stop() {
    stop.set(true);
    if(t != null) {
      t.interrupt();
    }
  }

}
