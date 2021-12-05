/*
 * Copyright 2021 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.server.ConcurrencyControl;
import kvd.server.Key;
import kvd.server.Kvd;

public class ExpireTest {

  private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

  private boolean isTimeout(long nowNs, long startNs, long timeoutNs) {
    return (nowNs - startNs) >= timeoutNs;
  }

  private boolean isTimeout(long startNs, long timeoutNs) {
    return isTimeout(System.nanoTime(), startNs, timeoutNs);
  }

  @Test
  public void testTimeout() throws Exception {
    long startNs = System.nanoTime();
    for(;;) {
      if(isTimeout(startNs, TimeUnit.MILLISECONDS.toNanos(50))) {
        break;
      } else {
        Thread.sleep(1);
      }
    }
  }

  @Test
  public void writeExpireTest() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "2s";
      options.logLevel = "info";
      server = new Kvd();
      server.run(options);
      KvdClient client = server.newLocalClient();
      final String key = "writeExpireTest";
      client.putString(key, key);
      long startNs = System.nanoTime();
      for(;;) {
        if(isTimeout(startNs, TIMEOUT_NANOS)) {
          fail("timeout reached, key should have expired by now");
        }
        if(client.contains(key)) {
          Thread.sleep(100);
        } else {
          break;
        }
      }
    } finally {
      if(server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  public void accessExpireTest() throws Exception {
    final Key key = Key.of("accessExpireTest");
    final CompletableFuture<Boolean> removedFuture = new CompletableFuture<>();
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterAccess = "1s";
      options.logLevel = "info";
      server = new Kvd();
      server.run(options);
      server.getExpiredKeysRemover().registerRemovalListener(k -> {
        if(key.equals(k)) {
          removedFuture.complete(true);
        }
      });
      KvdClient client = server.newLocalClient();
      client.putBytes(key.getBytes(), key.getBytes());
      for(int i=0;i<10;i++) {
        assertTrue(client.contains(key.getBytes()));
        Thread.sleep(250);
      }
      removedFuture.get(TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
      assertFalse(client.contains(key.getBytes()));
    } finally {
      if(server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  public void writeExpireTestTx() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "500ms";
      options.logLevel = "info";
      options.concurrency = ConcurrencyControl.PESRW;
      server = new Kvd();
      server.run(options);
      KvdClient client = server.newLocalClient();
      final String key = "writeExpireTest";
      client.withTransactionVoid(tx -> {
        tx.putString(key, key);
      });
      long startNs = System.nanoTime();
      for(;;) {
        if(isTimeout(startNs, TIMEOUT_NANOS)) {
          fail("timeout reached, key should have expired by now");
        }
        boolean contains = client.withTransaction(tx -> tx.contains(key));
        if(contains) {
          Thread.sleep(25);
        } else {
          break;
        }
      }
    } finally {
      if(server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  public void writeExpireTestTxLocked() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "500ms";
      options.logLevel = "info";
      options.concurrency = ConcurrencyControl.PESRW;
      server = new Kvd();
      server.run(options);
      KvdClient client = server.newLocalClient();
      final String key1 = "writeExpireTest1";
      final String key2 = "writeExpireTest2";
      client.withTransactionVoid(tx -> {
        tx.putString(key1, key1);
        tx.putString(key2, key2);
      });
      client.withTransactionVoid(tx -> {
        assertTrue(tx.contains(key1));
        try {
          long startNs = System.nanoTime();
          for(;;) {
            if(isTimeout(startNs, TIMEOUT_NANOS)) {
              fail("timeout reached, key should have expired by now");
            }
            if(client.contains(key2)) {
              Thread.sleep(25);
            } else {
              break;
            }
          }
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
        // key1 is expired but should still be there as we held a read lock.
        assertTrue(tx.contains(key1));
        // key2 is expired and removed
        assertFalse(tx.contains(key2));
      });
      long startNs = System.nanoTime();
      for(;;) {
        if(isTimeout(startNs, TIMEOUT_NANOS)) {
          fail("timeout reached, key should have expired by now");
        }
        if(client.contains(key1)) {
          Thread.sleep(25);
        } else {
          break;
        }
      }
      assertFalse(client.contains(key1));
      assertFalse(client.contains(key2));
    } finally {
      if(server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  public void writeExpireTestTxMany() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "2s";
      options.expireCheckInterval = "5s";
      options.logLevel = "info";
      options.concurrency = ConcurrencyControl.PESRW;
      server = new Kvd();
      server.run(options);
      KvdClient client = server.newLocalClient();
      client.withTransactionVoid(tx -> {
        for(int i=0;i<250;i++) {
          tx.putString("key"+i, "test");
        }
      });
      long startNs = System.nanoTime();
      for(;;) {
        if(isTimeout(startNs, TIMEOUT_NANOS)) {
          fail("timeout reached, key should have expired by now");
        }
        boolean contains = client.withTransaction(tx -> tx.contains("key249"));
        if(contains) {
          Thread.sleep(100);
        } else {
          break;
        }
      }
    } finally {
      if(server != null) {
        server.shutdown();
      }
    }
  }

}
