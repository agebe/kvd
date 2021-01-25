package kvd.test;

import kvd.client.KvdClient;

public class SimpleTest {
  public static void main(String[] args) throws Exception {
    try(KvdClient client = new KvdClient("localhost")) {
      client.putString("simple", "foo");
      System.out.println(client.getString("simple"));
    }
  }
}
