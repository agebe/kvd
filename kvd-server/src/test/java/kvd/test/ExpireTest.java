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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ExpireTest {

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
