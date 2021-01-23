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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.common.KvdException;
import kvd.server.Kvd;

public class KvdTest {

  private static Kvd server;

  @BeforeAll
  public static void setup() throws Exception {
    Kvd.KvdOptions options = new Kvd.KvdOptions();
    Path tempDirWithPrefix = Files.createTempDirectory("kvd");
    options.port = 0;
    options.storage = tempDirWithPrefix.toFile().getAbsolutePath();
    options.logLevel = "warn";
    server = new Kvd();
    server.run(options);
  }

  @AfterAll
  public static void done() {
    server.getSocketServer().stop();
  }

  private KvdClient client() {
    return new KvdClient("localhost:"+server.getSocketServer().getLocalPort());
  }

  @Test
  public void charsetTest() {
    try(KvdClient client = client()) {
      client.putString("string", "test", "UTF-16");
      assertEquals("test", client.getString("string", "UTF-16"));
    }
  }

  @Test
  public void getTest() {
    final String key = "getTEST";
    final String val = "my string value";
    try(KvdClient client = client()) {
      client.putString(key, val);
      assertEquals(val, client.getString(key));
    }
  }

  @Test
  public void testEmpty() {
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
    try(KvdClient client = client()) {
      assertFalse(client.contains("null"), "contains key 'null'");
      assertNull(client.get("null"));
      assertThrows(KvdException.class, () -> client.putString("null", null));
    }
  }

  @Test
  public void testRemoveNonExist() {
    try(KvdClient client = client()) {
      String key = "not-existing";
      assertFalse(client.contains(key), "contains key '"+key+"'");
      assertTrue(client.get(key) == null);
      assertFalse(client.remove(key));
    }
  }

  @Test
  public void getStringTest() {
    try(KvdClient client = client()) {
      String key = "string-key-not-existing";
      assertNull(client.getString(key));
    }
  }

  @Test
  public void streaming() throws Exception {
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
    try(KvdClient client = client()) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
    }
  }

  @Test
  public void simpleStreamReadTest() throws Exception {
    simpleStreamWriteTest();
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
    IntStream.range(0,100).forEachOrdered(i -> {
      r.nextBytes(buf);
      String key = "multithreadedtest_threadid_" + threadId + "_" + i;
      String val = TestUtils.bytesToHex(buf);
      client.putString(key, val);
      assertEquals(val, client.getString(key));
    });
  }

  @Test
  public void multiThreadedTest() {
    try(KvdClient client = client()) {
      List<Thread> threads = IntStream.range(0, 10).mapToObj(i -> {
        Thread t = new Thread(() -> testRunner(client, i), "kvd-testrunner-"+i);
        t.start();
        return t;
      }).collect(Collectors.toList());
      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
      });
    }
  }

  @Test
  public void largeStringTest() {
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

}
