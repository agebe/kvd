package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.lang3.StringUtils;

import kvd.client.KvdClient;

public class SimpleTest {
  public static void main(String[] args) throws Exception {
    try(KvdClient client = new KvdClient("localhost")) {
      client.putString("simple", "foo");
      client.putBytes(new byte[] {0}, "foo".getBytes());
      String largeKey = StringUtils.repeat('a', 1000);
      String val = "large key test";
      client.putString(largeKey, val);
      assertEquals(val, client.getString(largeKey));
      System.out.println(client.getString("simple"));
    }
  }
}
