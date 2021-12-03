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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.client.KvdClient;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ExpireTest {

  private static final Logger log = LoggerFactory.getLogger(ExpireTest.class);

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
      long now = System.nanoTime();
      for(;;) {
        if(now + TimeUnit.SECONDS.toNanos(30) < System.nanoTime()) {
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
  public void writeExpireTestTx() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "2s";
      options.logLevel = "info";
      options.concurrency = ConcurrencyControl.PESRW;
      server = new Kvd();
      server.run(options);
      KvdClient client = server.newLocalClient();
      final String key = "writeExpireTest";
      client.withTransactionVoid(tx -> {
        tx.putString(key, key);
      });
      long now = System.nanoTime();
      for(;;) {
        if(now + TimeUnit.SECONDS.toNanos(30) < System.nanoTime()) {
          fail("timeout reached, key should have expired by now");
        }
        boolean contains = client.withTransaction(tx -> tx.contains(key));
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

  @Test
  public void writeExpireTestTxLocked() throws Exception {
    Kvd server = null;
    try {
      Kvd.KvdOptions options = TestUtils.prepareFileServer();
      options.expireAfterWrite = "2s";
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
          log.info("waiting");
          Thread.sleep(5000);
          log.info("done waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
        // key1 is expired but should still be there as we held a read lock.
        assertTrue(tx.contains(key1));
        // key2 is expired and removed
        assertFalse(tx.contains(key2));
      });
      Thread.sleep(1000);
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
      long now = System.nanoTime();
      for(;;) {
        if(now + TimeUnit.SECONDS.toNanos(30) < System.nanoTime()) {
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
