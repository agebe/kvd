package kvd.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import kvd.server.Kvd;

public class MemKvdTest extends KvdTest {

  @BeforeAll
  public static void setup() throws Exception {
    Kvd.KvdOptions options = new Kvd.KvdOptions();
    options.port = 0;
    options.storage = "mem:";
    options.logLevel = "warn";
    server = new Kvd();
    server.run(options);
  }

  @AfterAll
  public static void done() {
    server.getSocketServer().stop();
  }

}
