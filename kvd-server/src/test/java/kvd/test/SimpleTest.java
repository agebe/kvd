package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;

import kvd.client.KvdClient;

public class SimpleTest {
  public static void main(String[] args) throws Exception {
    // test README.md examples

    try(KvdClient client = new KvdClient("localhost:3030")) {
      client.putString("my-key", "my-value");
      System.out.println(client.getString("my-key"));
    }
    try(KvdClient client = new KvdClient("localhost:3030")) {
      try(DataOutputStream out = new DataOutputStream(client.put("simplestream"))) {
        out.writeLong(42);
      }
      InputStream i = client.get("simplestream");
      if(i != null) {
        try(DataInputStream in = new DataInputStream(i)) {
          System.out.println(in.readLong());
        }
      } else {
        throw new RuntimeException("value missing");
      }
    }
    try(KvdClient client = new KvdClient("localhost")) {
//      client.removeAll();
      if(!client.contains("simple")) {
        client.putString("simple", "foo");
        Thread.sleep(1000);
      }
      client.getString("simple");
      client.putBytes(new byte[] {0}, "foo".getBytes());
      String largeKey = StringUtils.repeat('a', 1000);
      String val = "large key test";
      client.putString(largeKey, val);
      assertEquals(val, client.getString(largeKey));
//      System.out.println(client.getString("simple"));
//      client.removeAll();
//      client.putString("foo", "bar");
      client.getString("simple");
      client.remove("simple");
    }
  }
}
