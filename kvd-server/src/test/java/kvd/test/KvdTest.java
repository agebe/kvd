package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
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
  public void emptyNull() {
    try(KvdClient client = new KvdClient("localhost:"+server.getSocketServer().getLocalPort())) {
      assertFalse(client.contains("empty"), "contains key 'empty'");
      assertFalse(client.contains("null"), "contains key 'null'");
      client.putString("empty", "");
      client.putString("null", null);
      assertEquals("", client.getString("empty"));
      assertEquals("", client.getString("null"));
      assertTrue(client.contains("empty"), "contains key 'empty'");
      assertTrue(client.contains("null"), "contains key 'null'");
      assertTrue(client.remove("empty"));
      assertTrue(client.remove("null"));
      assertFalse(client.contains("empty"), "contains key 'empty'");
      assertFalse(client.contains("null"), "contains key 'null'");
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
