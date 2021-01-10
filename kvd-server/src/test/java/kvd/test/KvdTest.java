package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Future;

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

  @Test
  public void putTest() {
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      client.putString("string", "test");
    }
  }

  @Test
  public void getTest() {
    final String key = "getTEST";
    final String val = "my string value";
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      client.putString(key, val);
      assertEquals(val, client.getString(key));
    }
  }

  @Test
  public void testEmpty() {
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
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
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
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
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      assertFalse(client.contains("null"), "contains key 'null'");
      assertNull(client.get("null"));
      assertThrows(KvdException.class, () -> client.putString("null", null));
    }
  }

  @Test
  public void testRemoveNonExist() {
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      String key = "not-existing";
      assertFalse(client.contains(key), "contains key '"+key+"'");
      assertTrue(client.get(key) == null);
      assertFalse(client.remove(key));
    }
  }

  @Test
  public void streaming() throws Exception {
    String key = "streaming";
    String chars = "abcdefghijklmnopqrstuvwxyz1234567890";
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      try(OutputStream out = client.put(key)) {
        for(int i = 0;i<1024;i++) {
          write1Kb(out, chars.charAt(i%chars.length()));
        }
      }
    }
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
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

}
