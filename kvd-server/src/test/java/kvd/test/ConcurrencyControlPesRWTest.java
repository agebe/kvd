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

public class ConcurrencyControlPesRWTest {

  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startServer("warn", ConcurrencyControl.PESRW);
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void concurrentRead1() {
    final String key = "concurrentRead1_pesrw";
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
    final String key = "concurrentReadWrite1_pesrw";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction(2500);
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
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    final String key = "concurrentReadWrite2_pesrw";
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
          ft1.complete(true);
        } catch(Throwable t) {
          ft1.completeExceptionally(new RuntimeException("t1 failed", t));
        }
      });
      Thread t2 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          f1.get();
          tx.putString(key, "3");
          assertEquals("3", tx.getString(key));
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
      assertEquals("3", client.getString(key));
    }
  }

  @Test
  public void concurrentReadWrite3() throws Exception {
    // testing write lock upgrade in t1
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    final String key = "concurrentReadWrite3_pesrw";
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
    // neither thread can upgrade read lock
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    final String key1 = "testDeadlock1_pesrw";
    final String key2 = "testDeadlock2_pesrw";
    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    try(KvdClient client = client()) {
      assertFalse(client.contains(key1));
      assertFalse(client.contains(key2));
      client.putString(key1, "1");
      client.putString(key2, "1");
      Thread t1 = new Thread(() -> {
        try(KvdTransaction tx = client.beginTransaction()) {
          assertTrue(tx.contains(key1));
          tx.putString(key1, "2");
          f1.complete(true);
          f2.get();
          long startNs = System.nanoTime();
          assertEquals("2", tx.getString(key2));
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
          assertThrows(KvdException.class, () -> tx.getString(key1));
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
      assertEquals("2", client.getString(key1));
      assertEquals("2", client.getString(key2));
    }
  }

  @Test
  public void deadlock3() throws Exception {
    // neither thread can upgrade read lock
    CompletableFuture<Boolean> ft1 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft2 = new CompletableFuture<>();
    CompletableFuture<Boolean> ft3 = new CompletableFuture<>();
    final String key1 = "deadlock3_1_pesrw";
    final String key2 = "deadlock3_2_pesrw";
    final String key3 = "deadlock3_3_pesrw";
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
          assertTrue(tx.contains(key2));
          assertEquals("2", tx.getString(key2));
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
          assertTrue(tx.contains(key3));
          assertEquals("3", tx.getString(key3));
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
          assertThrows(KvdException.class, () -> tx.getString(key1));
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

}
