package kvd.server.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadlockDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadlockDetector.class);

  private Thread t;

  // https://stackoverflow.com/a/217701
  private ThreadInfo[] detectDeadlock() {
    ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
    long[] ids = tmx.findDeadlockedThreads();
    return (ids != null)?tmx.getThreadInfo(ids, true, true):null; 
  }

  public void start(long intervalMs, Consumer<ThreadInfo[]> consumer) {
    Runnable r = () -> {
      log.info("started");
      for(;;) {
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
    };
    t = new Thread(r, "deadlock-detector");
    t.setDaemon(true);
    t.start();
  }

}
