/*
 * Copyright 2021 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import kvd.server.ConcurrencyControl;
import kvd.server.DbType;
import kvd.server.Kvd;

public class TestUtils {

  // from https://stackoverflow.com/a/9855338
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  public static File createTempDirectory(String prefix) throws IOException {
    return Files.createTempDirectory(prefix).toFile();
  }

  public static Kvd.KvdOptions prepareServer(DbType type) throws IOException {
    Kvd.KvdOptions options = new Kvd.KvdOptions();
    options.port = 0;
    options.datadir = createTempDirectory("kvd");
    options.logLevel = "warn";
    return options;
  }

  public static Kvd startServer() {
    return startServer("warn", ConcurrencyControl.NONE);
  }

  public static Kvd startServer(String loglevel, ConcurrencyControl cc) {
    try {
      Kvd.KvdOptions options = prepareServer(DbType.MAPDB);
      options.logLevel = loglevel;
      options.concurrency = cc;
      Kvd server = new Kvd();
      server.run(options);
      return server;
    } catch(IOException e) {
      throw new RuntimeException("failed to start kvd server", e);
    }
  }

}
