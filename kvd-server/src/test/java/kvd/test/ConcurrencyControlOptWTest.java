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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.client.KvdTransaction;
import kvd.common.KvdException;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ConcurrencyControlOptWTest {

  private static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startServer("warn", ConcurrencyControl.OPTW);
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
    }
  }

  @Test
  public void concurrentReadWrite1() throws Exception {
    final String key = "concurrentReadWrite1_optw";
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
      assertEquals(value1, tx2.getString(key));
      assertTrue(tx1.contains(key));
      assertTrue(tx2.contains(key));
      tx1.commit();
      assertEquals(value2, tx2.getString(key));
      tx2.commit();
    }
  }

  @Test
  public void concurrentReadWrite2() throws Exception {
    final String key = "concurrentReadWrite2";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value1, tx1.getString(key));
      assertEquals(value1, tx2.getString(key));
      assertEquals(value1, client.getString(key));
      tx1.putString(key, value2);
      assertEquals(value2, tx1.getString(key));
      assertEquals(value1, tx2.getString(key));
      tx1.commit();
      assertEquals(value2, tx2.getString(key));
      assertEquals(value2, client.getString(key));
    }
  }

  @Test
  public void concurrentWrite1() throws Exception {
    final String key = "concurrentWrite1";
    final String value1 = key;
    final String value2 = value1+"value2";
    final String value3 = value1+"value3";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      tx1.putString(key, value2);
      assertThrows(KvdException.class, () -> tx2.putString(key, value3));
      assertThrows(KvdException.class, () -> tx2.remove(key));
      assertEquals(value1, tx2.getString(key));
      assertEquals(value1, client.getString(key));
      assertEquals(value2, tx1.getString(key));
      tx1.remove(key);
      assertEquals(value1, tx2.getString(key));
      assertEquals(value1, client.getString(key));
      assertFalse(tx1.contains(key));
      tx1.commit();
      assertFalse(client.contains(key));
      assertFalse(tx2.contains(key));
      tx2.putString(key, value3);
      assertEquals(value3, tx2.getString(key));
      assertFalse(client.contains(key));
      tx2.commit();
      assertEquals(value3, client.getString(key));
    }
  }

  private void threadRunner(KvdClient client, int tId) {
    try(KvdTransaction tx1 = client.beginTransaction()) {
      String tx1Key = "t"+tId+"tx1_";
      String tx2Key = "t"+tId+"tx2_";
      for(int i=0;i<10;i++) {
        tx1.putString(tx1Key+i, ""+i);
        try(KvdTransaction tx2 = client.beginTransaction()) {
          tx2.putString(tx2Key+i, ""+i);
          tx2.commit();
        }
        assertTrue(tx1.contains(tx1Key+i));
        assertFalse(client.contains(tx1Key+i));
        assertTrue(tx1.contains(tx2Key+i));
        assertTrue(client.contains(tx2Key+i));
        try {
          Thread.sleep(tId+10);
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
      }
    }
  }

  @Test
  public void multiThreadTest() {
    try(KvdClient client = client()) {
      List<Thread> threads = IntStream.range(0, 10).mapToObj(
          i -> new Thread(() -> threadRunner(client, i))).collect(Collectors.toList());
      threads.forEach(Thread::start);
      threads.forEach(t -> {
        try {
          t.join();
        } catch(Exception e) {
          throw new RuntimeException("",e);
        }
      });
    }
  }

}
