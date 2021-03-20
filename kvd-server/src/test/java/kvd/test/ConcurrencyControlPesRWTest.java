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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.client.KvdTransaction;
import kvd.common.KvdException;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ConcurrencyControlPesRWTest {
  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startMemServer("warn", ConcurrencyControl.PESRW);
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void concurrentRead1() {
    final String key = "concurrentRead1";
    final String value = key;
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value, tx1.getString(key));
      assertEquals(value, tx2.getString(key));
      assertTrue(tx1.contains(key));
      assertTrue(tx2.contains(key));
      tx1.rollback();
      tx2.rollback();
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentReadWrite1() throws Exception {
    final String key = "concurrentReadWrite1";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction(250);
      assertEquals(value1, tx1.getString(key));
      tx1.putString(key, value2);
      assertEquals(value2, tx1.getString(key));
      assertTrue(tx1.contains(key));
      assertThrows(KvdException.class, () -> tx2.getString(key));
      assertThrows(KvdException.class, () -> tx2.contains(key));
      tx1.commit();
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentReadWrite2() throws Exception {
    // testing get write lock in t2
    final String key = "concurrentReadWrite2";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, "1");
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f1.complete(true);
          Thread.sleep(100);
          tx.putString(key, "2");
          Thread.sleep(100);
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t1 failed", e);
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          f1.get();
          tx.putString(key, "3");
          assertEquals("3", tx.getString(key));
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t2 failed", e);
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      assertEquals("3", client.getString(key));
    }
  }

  @Test
  public void concurrentReadWrite3() throws Exception {
    // testing write lock upgrade in t1
    final String key = "concurrentReadWrite3";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, "1");
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f1.get();
          tx.putString(key, "2");
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t1 failed", e);
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f1.complete(true);
          Thread.sleep(100);
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t2 failed", e);
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      assertEquals("2", client.getString(key));
    }
  }

//  @Test
  public void testDeadlock1() throws Exception {
    // neither thread can upgrade read lock
    final String key = "testDeadlock1";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, "1");
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f2.get();
          tx.putString(key, "2");
          f1.complete(true);
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t1 failed", e);
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f2.complete(true);
          f1.get();
          assertThrows(KvdException.class, () -> tx.putString(key, "3"));
          tx.commit();
        } catch(Exception e) {
          throw new RuntimeException("t2 failed", e);
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      assertEquals("2", client.getString(key));
    }
  }

}
