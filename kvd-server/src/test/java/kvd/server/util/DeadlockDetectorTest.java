package kvd.server.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadlockDetectorTest {

  private static final Logger log = LoggerFactory.getLogger(DeadlockDetectorTest.class);

  private CompletableFuture<Boolean> s1 = new CompletableFuture<>();

  private CompletableFuture<Boolean> s2 = new CompletableFuture<>();

  private ReentrantLock rlock1 = new ReentrantLock();

  private ReentrantLock rlock2 = new ReentrantLock();

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
    try {
      t1.start();
      t2.start();
      assertTrue(f.get(30, TimeUnit.SECONDS));
    } finally {
      t1.interrupt();
      t2.interrupt();
      dd.stop();
      t1.join(5000);
      t2.join(5000);
    }
  }

  private void sync1() {
    try {
      rlock1.lockInterruptibly();
      log.info("got lock1");
      s1.complete(true);
      try {
        s2.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      rlock2.lockInterruptibly();
      log.info("got lock2");
    } catch(InterruptedException e) {
      // ignore
    } finally {
      if(rlock1.isHeldByCurrentThread()) {
        rlock1.unlock();
      }
      if(rlock2.isHeldByCurrentThread()) {
        rlock2.unlock();
      }
    }
  }

  private void sync2() {
    try {
      rlock2.lockInterruptibly();
      log.info("got lock2");
      s2.complete(true);
      try {
        s1.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      rlock1.lockInterruptibly();
      log.info("got lock1");
    } catch(InterruptedException e) {
      // ignore
    } finally {
      if(rlock1.isHeldByCurrentThread()) {
        rlock1.unlock();
      }
      if(rlock2.isHeldByCurrentThread()) {
        rlock2.unlock();
      }
    }
  }

}
