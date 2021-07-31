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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.client.KvdClient;
import kvd.client.KvdTransaction;
import kvd.common.KvdException;
import kvd.server.Kvd;

public abstract class KvdTest {

  private static final Logger log = LoggerFactory.getLogger(KvdTest.class);

  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @Test
  public void charsetTest() {
    log.info("charsetTest");
    try(KvdClient client = client()) {
      client.putString("string", "test", "UTF-16");
      assertEquals("test", client.getString("string", "UTF-16"));
    }
  }

  @Test
  public void getTest() {
    log.info("getTest");
    final String key = "getTEST";
    final String val = "my string value";
    try(KvdClient client = client()) {
      client.putString(key, val);
      assertEquals(val, client.getString(key));
    }
  }

  @Test
  public void testEmpty() {
    log.info("testEmpty");
    try(KvdClient client = client()) {
      assertFalse(client.contains("empty"), "contains key 'empty'");
      client.putString("empty", "");
      assertEquals("", client.getString("empty"));
      assertTrue(client.contains("empty"), "contains key 'empty'");
      assertTrue(client.remove("empty"));
      assertFalse(client.remove("empty"));
      assertFalse(client.contains("empty"), "contains key 'empty'");
    }
  }

  @Test
  public void testEmptyStream() throws Exception {
    log.info("testEmptyStream");
    try(KvdClient client = client()) {
      String key = "empty-stream";
      assertFalse(client.contains(key), "contains key '"+key+"'");
      try(OutputStream out = client.put(key)) {
        // do nothing
      }
      assertTrue(client.contains(key));
      Future<InputStream> f = client.getAsync(key);
      InputStream in = f.get();
      assertNotNull(in);
      assertEquals(-1, in.read());
      in.close();
      assertTrue(client.remove(key));
      assertFalse(client.remove(key));
      assertFalse(client.contains(key));
    }
  }


  @Test
  public void testNull() {
    log.info("testNull");
    try(KvdClient client = client()) {
      assertFalse(client.contains("null"), "contains key 'null'");
      assertNull(client.get("null"));
      assertThrows(KvdException.class, () -> client.putString("null", null));
    }
  }

  @Test
  public void testRemoveNonExist() {
    log.info("testRemoveNonExist");
    try(KvdClient client = client()) {
      String key = "not-existing";
      assertFalse(client.contains(key), "contains key '"+key+"'");
      assertTrue(client.get(key) == null);
      assertFalse(client.remove(key));
    }
  }

  @Test
  public void getStringTest() {
    log.info("getStringTest");
    try(KvdClient client = client()) {
      String key = "string-key-not-existing";
      assertNull(client.getString(key));
    }
  }

  @Test
  public void streamingTest() throws Exception {
    log.info("streamingTest");
    String key = "streaming";
    String chars = "abcdefghijklmnopqrstuvwxyz1234567890";
    try(KvdClient client = client()) {
      try(OutputStream out = client.put(key)) {
        for(int i = 0;i<1024;i++) {
          write1Kb(out, chars.charAt(i%chars.length()));
        }
      }
    }
    try(KvdClient client = client()) {
      assertTrue(client.contains(key));
      try(BufferedReader reader = new BufferedReader(new InputStreamReader(client.get(key), "UTF-8"))) {
        long read = 0;
        while(true) {
          String line = reader.readLine();
          if(line != null) {
            read += line.length() + 1;
          } else {
            break;
          }
        }
        assertEquals(1024*1024, read);
        assertTrue(client.remove(key));
        assertFalse(client.contains(key));
      }
    }
  }

  private static void write1Kb(OutputStream out, char c) throws Exception {
    String s = StringUtils.repeat(c, 31) + "\n";
    byte[] buf = s.getBytes("UTF-8");
    if(buf.length != 32) {
      throw new Exception("wrong buf size");
    }
    for(int i=0;i<32;i++) {
      out.write(buf);
    }
  }

  @Test
  public void simpleStreamWriteTest() throws Exception {
    log.info("simpleStreamWriteTest");
    try(KvdClient client = client()) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
    }
  }

  @Test
  public void simpleStreamReadTest() throws Exception {
    log.info("simpleStreamWriteTest");
    try(KvdClient client = client()) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
    }
    try(KvdClient client = client()) {
      InputStream i = client.get("simplestream");
      if(i != null) {
        try(DataInputStream in = new DataInputStream(i)) {
         assertEquals(42, in.readLong());
        }
      } else {
        throw new RuntimeException("value missing");
      }
    }
  }

  private void testRunner(KvdClient client, int threadId) {
    Random r = new Random(threadId);
    byte[] buf = new byte[(threadId+1)*10];
    IntStream.range(0,50).forEachOrdered(i -> {
      r.nextBytes(buf);
      String key = "multithreadedtest_threadid_" + threadId + "_" + i;
      String val = TestUtils.bytesToHex(buf);
      assertFalse(client.contains(key));
      client.putString(key, val);
      assertTrue(client.contains(key));
      assertEquals(val, client.getString(key));
      assertTrue(client.remove(key));
      assertFalse(client.contains(key));
    });
  }

  @Test
  public void multiThreadedTest() throws Exception {
    log.info("multiThreadedTest");
    try(KvdClient client = client()) {
      ExecutorService exec = Executors.newFixedThreadPool(10);
      List<Future<Boolean>> futures = IntStream.range(0, 10).mapToObj(i -> exec.submit(() -> {
        testRunner(client, i);
        return true;
      })).collect(Collectors.toList());
      for(Future<Boolean> f : futures) {
        f.get();
      }
      exec.shutdown();
    }
  }

  @Test
  public void largeStringTest() {
    log.info("largeStringTest");
    try(KvdClient client = client()) {
      String key = "largestring";
      String val = StringUtils.repeat("0123456789", 10000);
      client.putString(key, val);
      assertEquals(val, client.getString(key));
    }
  }

  private Long getLong(KvdClient client, String key) throws Exception {
    try(DataInputStream in = new DataInputStream(client.get(key))) {
      return in.readLong();
    }
  }

  @Test
  public void clobberTest() throws Exception {
    log.info("clobberTest");
    try(KvdClient client = client()) {
      String key = "clobber";
      DataOutputStream stream1 = new DataOutputStream(client.put(key));
      DataOutputStream stream2 = new DataOutputStream(client.put(key));
      stream1.writeLong(1);
      stream2.writeLong(2);
      stream1.close();
      assertEquals(1, getLong(client, key));
      stream2.close();
      assertEquals(2, getLong(client, key));
    }
  }

  @Test
  public void abortTest() throws Exception {
    log.info("abortTest");
    String key1 = "abort1";
    String key2 = "abort2";
    try {
      KvdClient client = client();
      DataOutputStream stream1 = new DataOutputStream(client.put(key1));
      DataOutputStream stream2 = new DataOutputStream(client.put(key2));
      stream1.writeLong(1);
      stream2.writeLong(2);
      stream1.close();
      client.close();
    } catch(Exception e) {
      e.printStackTrace();
    }
    try(KvdClient client = client()) {
      assertTrue(client.contains(key1));
      assertFalse(client.contains(key2));
    }
  }

  @Test
  public void containsTest() {
    log.info("containsTest");
    try(KvdClient client = client()) {
      String key = "contains";
      assertFalse(client.contains(key));
      client.putString(key, "12345");
      assertTrue(client.contains(key));
    }
  }

  @Test
  public void serverAbortTest() {
    // test server abort on close
    log.info("serverAbortTest");
    try(KvdClient client = client()) {
      String key = "__kvd_test";
      assertThrows(KvdException.class, () -> {
        client.contains(key);
      });
      assertThrows(KvdException.class, () -> {
        client.putString(key, "12345");
      });
      assertThrows(KvdException.class, () -> {
        client.getString(key);
      });
      assertThrows(KvdException.class, () -> {
        client.remove(key);
      });
    }
  }

  @Test
  public void serverAbortTest2() {
    // test server abort on upload
    log.info("serverAbortTest2");
    try(KvdClient client = client()) {
      String key = "__kvd_test2";
      assertThrows(KvdException.class, () -> {
        client.contains(key);
      });
      assertThrows(KvdException.class, () -> {
        OutputStream out = client.put(key);
        byte[] b = new byte[1024];
        for(int i=0;i<10_000;i++) {
          out.write(b);
        }
      });
    }
  }

  @Test
  public void binaryKeyTest() {
    log.info("binaryKeyTest");
    try(KvdClient client = client()) {
      byte[] key1 = new byte[] { 0 };
      byte[] key2 = new byte[] { 42 };
      byte[] key3 = new byte[] { 0, 1, 2 };
      assertFalse(client.contains(key1));
      assertFalse(client.contains(key2));
      assertFalse(client.contains(key3));
      assertNull(client.getBytes(key1));
      assertNull(client.getBytes(key2));
      assertNull(client.getBytes(key3));
      assertFalse(client.remove(key1));
      assertFalse(client.remove(key2));
      assertFalse(client.remove(key3));
      client.putBytes(key1, key1);
      client.putBytes(key2, key2);
      client.putBytes(key3, key3);
      assertTrue(client.contains(key1));
      assertTrue(client.contains(key2));
      assertTrue(client.contains(key3));
      assertArrayEquals(key1, client.getBytes(key1));
      assertArrayEquals(key2, client.getBytes(key2));
      assertArrayEquals(key3, client.getBytes(key3));
      assertTrue(client.remove(key1));
      assertTrue(client.remove(key2));
      assertTrue(client.remove(key3));
    }
  }

  private void simpleKeyTest(String msg, byte[] key) {
    log.info("{}", msg);
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      assertNull(client.getBytes(key));
      assertFalse(client.remove(key));
      client.putBytes(key, key);
      assertTrue(client.contains(key));
      assertArrayEquals(key, client.getBytes(key));
      assertTrue(client.remove(key));
      assertFalse(client.contains(key));
    }
  }

  private void simpleKeyTestTx(String msg, byte[] key) {
    try(KvdClient client = client()) {
      try(KvdTransaction tx = client.beginTransaction()) {
        assertFalse(tx.contains(key));
        assertNull(tx.getBytes(key));
        assertFalse(tx.remove(key));
        tx.putBytes(key, key);
        assertTrue(tx.contains(key));
        assertArrayEquals(key, tx.getBytes(key));
        assertTrue(tx.remove(key));
        assertFalse(tx.contains(key));
        tx.commit();
      }
      assertFalse(client.contains(key));
    }
  }

  @Test
  public void largeKeyTest() {
    // 1KiB key
    byte[] key = StringUtils.repeat("0123456789abcdef", 64).getBytes();
    simpleKeyTest("largeKeyTest", key);
    simpleKeyTestTx("largeKeyTestTx", key);
  }

  @Test
  public void hugeKeyTest() {
    // 1MiB key
    byte[] key = StringUtils.repeat("0123456789abcdef", 64*1024).getBytes();
    simpleKeyTest("hugeKeyTest", key);
    simpleKeyTestTx("hugeKeyTestTx", key);
  }

  @Test
  public void removeAllTest() {
    log.info("removeAllTest");
    try(KvdClient client = client()) {
      String key = "removeAllTest";
      assertFalse(client.contains(key));
      client.putString(key, key);
      assertTrue(client.contains(key));
      assertTrue(client.removeAll());
      assertFalse(client.contains(key));
    }
  }

}
