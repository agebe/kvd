package kvd.server.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadlockDetectorTest {

  private static final Logger log = LoggerFactory.getLogger(DeadlockDetectorTest.class);

  private CompletableFuture<Boolean> s1 = new CompletableFuture<>();

  private CompletableFuture<Boolean> s2 = new CompletableFuture<>();

  private Object lock1 = new Object();

  private Object lock2 = new Object();

  @Test
  public void test() throws Exception {
    DeadlockDetector dd = new DeadlockDetector();
    CompletableFuture<Boolean> f = new CompletableFuture<>();
    dd.start(1000, ti -> f.complete(true));
    Thread t1 = new Thread(() -> {
      sync1();
    });
    Thread t2 = new Thread(() -> {
      sync2();
    });
    Thread.sleep(100);
    t1.start();
    t2.start();
    assertTrue(f.get(30, TimeUnit.SECONDS));
  }

  private void sync1() {
    synchronized(lock1) {
      log.info("got lock1");
      s1.complete(true);
      try {
        s2.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      synchronized(lock2) {
        log.info("got lock2");
      }
    }
  }

  private synchronized void sync2() {
    synchronized(lock2) {
      log.info("got lock2");
      s2.complete(true);
      try {
        s1.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      synchronized(lock1) {
        log.info("got lock1");
      }
    }
  }

}
