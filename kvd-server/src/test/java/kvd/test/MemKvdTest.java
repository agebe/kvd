package kvd.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class MemKvdTest extends KvdTest {

  @BeforeAll
  public static void setup() {
    server = TestUtils.startMemServer();
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

}
