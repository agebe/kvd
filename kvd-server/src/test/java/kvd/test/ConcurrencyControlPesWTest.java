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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.client.KvdTransaction;
import kvd.common.KvdException;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ConcurrencyControlPesWTest {

  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startServer("warn", ConcurrencyControl.PESW);
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void concurrentRead1() {
    final String key = "concurrentRead1_pesw";
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
    final String key = "concurrentReadWrite1_pesw";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value1, tx1.getString(key));
      tx1.putString(key, value2);
      assertEquals(value2, tx1.getString(key));
      assertTrue(tx1.contains(key));
      assertEquals(value1, tx2.getString(key));
      assertTrue(tx2.contains(key));
      tx1.commit();
      assertEquals(value2, tx2.getString(key));
      tx2.rollback();
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentReadWrite3() throws Exception {
    // testing write lock upgrade in t1
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    final String key = "concurrentReadWrite3_pesw";
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
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          f1.complete(true);
          Thread.sleep(100);
          tx.commit();
          ft2.complete(true);
        } catch(Throwable t) {
          ft2.completeExceptionally(new RuntimeException("t2 failed", t));
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      ft1.get();
      ft2.get();
      assertEquals("2", client.getString(key));
    }
  }

  @Test
  public void testDeadlock1() throws Exception {
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    // deadlock in t2
    final String key1 = "testDeadlock1_pesw";
    final String key2 = "testDeadlock2_pesw";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key1));
      assertFalse(client.contains(key2));
      client.putString(key1, "client");
      client.putString(key2, "client");
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key1));
          tx.putString(key1, "1");
          f1.complete(true);
          f2.get();
          assertEquals("client", tx.getString(key2));
          long startNs = System.nanoTime();
          tx.putString(key2, "1");
          long endNs = System.nanoTime();
          assertTrue(TimeUnit.NANOSECONDS.toMillis(endNs - startNs) > 200);
          tx.commit();
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key2));
          tx.putString(key2, "2");
          f2.complete(true);
          f1.get();
          Thread.sleep(250);
          assertThrows(KvdException.class, () -> tx.putString(key1, "2"));
          tx.commit();
          ft2.complete(true);
        } catch(Throwable t) {
          ft2.completeExceptionally(new RuntimeException("t2 failed", t));
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      ft1.get();
      ft2.get();
      assertEquals("1", client.getString(key1));
      assertEquals("1", client.getString(key2));
    }
  }

  @Test
  public void deadlock3() throws Exception {
    // t3 can't get write lock
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft3 = new CompletableFuture<>();
    final String key1 = "deadlock3_1_pesw";
    final String key2 = "deadlock3_2_pesw";
    final String key3 = "deadlock3_3_pesw";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    CompletableFuture<Boolean> f3 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key1));
      assertFalse(client.contains(key2));
      assertFalse(client.contains(key3));
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertFalse(tx.contains(key1));
          tx.putString(key1, "1");
          f1.complete(true);
          f2.get();
          f3.get();
          long startNs = System.nanoTime();
          tx.putString(key2, "1");
          assertEquals("1", tx.getString(key2));
          long endNs = System.nanoTime();
          assertTrue(TimeUnit.NANOSECONDS.toMillis(endNs - startNs) > 500);
          tx.commit();
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertFalse(tx.contains(key2));
          tx.putString(key2, "2");
          f2.complete(true);
          f1.get();
          f3.get();
          long startNs = System.nanoTime();
          tx.putString(key3, "2");
          assertEquals("2", tx.getString(key3));
          long endNs = System.nanoTime();
          assertTrue(TimeUnit.NANOSECONDS.toMillis(endNs - startNs) > 200);
          Thread.sleep(250);
          tx.commit();
          ft2.complete(true);
        } catch(Throwable t) {
          ft2.completeExceptionally(new RuntimeException("t2 failed", t));
        }
      });
      Thread t3 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertFalse(tx.contains(key3));
          tx.putString(key3, "3");
          f3.complete(true);
          f1.get();
          f2.get();
          Thread.sleep(250);
          // this creates the deadlock
          assertThrows(KvdException.class, () -> tx.putString(key1, "3"));
          tx.commit();
          ft3.complete(true);
        } catch(Throwable t) {
          ft3.completeExceptionally(new RuntimeException("t3 failed", t));
        }
      });
      t1.start();
      t2.start();
      t3.start();
      t1.join();
      t2.join();
      t3.join();
      ft1.get();
      ft2.get();
      ft3.get();
      assertEquals("1", client.getString(key1));
      assertEquals("1", client.getString(key2));
      assertEquals("2", client.getString(key3));
    }
  }

  @Test
  public void testLock1() throws Exception {
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    final String key = "testLock1";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          tx.lock(key);
          f1.complete(true);
          Thread.sleep(250);
          tx.putString(key, "1");
          tx.commit();
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          f1.get();
          assertFalse(tx.contains(key));
          long startNs = System.nanoTime();
          tx.lock(key);
          long endNs = System.nanoTime();
          assertTrue(TimeUnit.NANOSECONDS.toMillis(endNs - startNs) > 200);
          assertEquals("1", tx.getString(key));
          tx.putString(key, "2");
          tx.commit();
          ft2.complete(true);
        } catch(Throwable t) {
          ft2.completeExceptionally(new RuntimeException("t2 failed", t));
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      ft1.get();
      ft2.get();
      assertEquals("2", client.getString(key));
    }
  }

  @Test
  public void readUntilSet() throws Exception {
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    final String key = "readUntilSet_pesw";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, "client");;
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          tx.putString(key, "1");
          assertEquals("1", tx.getString(key));
          Thread.sleep(250);
          tx.commit();
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key));
          int counter = 0;
          while(!tx.getString(key).equals("1")) {
            counter++;
            Thread.sleep(5);
          }
          assertEquals("1", tx.getString(key));
          assertTrue(counter > 0);
          tx.commit();
          ft2.complete(true);
        } catch(Throwable t) {
          ft2.completeExceptionally(new RuntimeException("t2 failed", t));
        }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      ft1.get();
      ft2.get();
      assertEquals("1", client.getString(key));
    }
  }

}
