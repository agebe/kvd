package kvd.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class FileKvdTest extends KvdTest {

  @BeforeAll
  public static void setup() throws Exception {
    server = TestUtils.startFileServer();
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

}
