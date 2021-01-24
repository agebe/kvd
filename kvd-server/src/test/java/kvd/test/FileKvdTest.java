package kvd.test;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import kvd.server.Kvd;

public class FileKvdTest extends KvdTest {

  @BeforeAll
  public static void setup() throws Exception {
    Kvd.KvdOptions options = new Kvd.KvdOptions();
    Path tempDirWithPrefix = Files.createTempDirectory("kvd");
    options.port = 0;
    options.storage = "file:"+tempDirWithPrefix.toFile().getAbsolutePath();
    options.logLevel = "warn";
    server = new Kvd();
    server.run(options);
  }

  @AfterAll
  public static void done() {
    server.getSocketServer().stop();
  }

}
