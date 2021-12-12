package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import kvd.client.KvdClient;
import kvd.server.Kvd;

public class JsonTest {

  protected static Kvd server;

  @BeforeAll
  public static void setup() {
    server = TestUtils.startServer();
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  private KvdClient client() {
    return server.newLocalClient();
  }

  @Test
  public void jsonTest() throws IOException {
    SerializableTestObject test1 = new SerializableTestObject(
        "foo", 1l, Stream.of(1,2,3,4,5).collect(Collectors.toList()));
    String key = "myJson";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      try(OutputStreamWriter writer = new OutputStreamWriter(client.put(key))) {
        new Gson().toJson(test1, writer);
      }
      assertTrue(client.contains(key));
      assertEquals("{\"prop1\":\"foo\",\"prop2\":1,\"myList\":[1,2,3,4,5]}", client.getString(key));
      try(InputStreamReader reader = new InputStreamReader(client.get(key))) {
        SerializableTestObject test2 = new Gson().fromJson(reader, SerializableTestObject.class);
        assertEquals(test1, test2);
      }
    }
  }

}
