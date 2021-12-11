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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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

  public static Kvd.KvdOptions prepareServer(DbType type) throws IOException {
    Kvd.KvdOptions options = new Kvd.KvdOptions();
    Path tempDirWithPrefix = Files.createTempDirectory("kvd");
    options.port = 0;
    options.datadir = tempDirWithPrefix.toFile();
    options.defaultDbType = type;
    options.logLevel = "warn";
    return options;
  }

  public static Kvd.KvdOptions prepareFileServer() throws IOException {
    return prepareServer(DbType.FILE);
  }

  public static Kvd startFileServer() throws IOException {
    Kvd.KvdOptions options = prepareFileServer();
    Kvd server = new Kvd();
    server.run(options);
    return server;
  }

  public static Kvd startMemServer() {
    return startMemServer("warn", ConcurrencyControl.NONE);
  }

  public static Kvd startMemServer(String loglevel, ConcurrencyControl cc) {
    try {
      Kvd.KvdOptions options = new Kvd.KvdOptions();
      Path tempDirWithPrefix = Files.createTempDirectory("kvd");
      options.port = 0;
      options.datadir = tempDirWithPrefix.toFile();
      options.defaultDbType = DbType.MEM;
      options.logLevel = loglevel;
      options.concurrency = cc;
      Kvd server = new Kvd();
      server.run(options);
      return server;
    } catch(IOException e) {
      throw new RuntimeException("failed to start kvd server", e);
    }
  }

  public static Kvd startMapdbServer() throws IOException {
    Kvd.KvdOptions options = prepareServer(DbType.MAPDB);
    Kvd server = new Kvd();
    server.run(options);
    return server;
  }

}
