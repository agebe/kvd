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
package kvd.server.storage.fs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.server.Key;
import kvd.server.Kvd;
import kvd.server.storage.Transaction;
import kvd.server.storage.trash.SimpleTrash;

public class NestedTransactionTest {

  @BeforeAll
  public static void setupLogging() {
    Kvd.setLogLevel("kvd", "info");
  }

  @Test
  public void nestedTxTest() throws Exception {
    File fStore = Files.createTempDirectory("kvdfile").toFile();
    Key key1 = Key.of("key1");
    FileStorageBackend storage = new FileStorageBackend(fStore, new SimpleTrash());
    storage.withTransactionVoid(tx -> {
      tx.putBytes(key1, new byte[] {1,2,3,4,5});
      assertTrue(tx.contains(key1));
      assertTrue(nestedTx(storage, key1));
    });
  }

  @Test
  public void nonNestedTxTest() throws Exception {
    File fStore = Files.createTempDirectory("kvdfile").toFile();
    Key key1 = Key.of("key1");
    FileStorageBackend storage = new FileStorageBackend(fStore, new SimpleTrash());
    try(Transaction tx = storage.begin()) {
      tx.putBytes(key1, new byte[] {1,2,3,4,5});
      assertTrue(tx.contains(key1));
      assertFalse(nestedTx(storage, key1));
    }
  }

  private boolean nestedTx(FileStorageBackend storage, Key key1) {
    return storage.withTransaction(tx -> {
      return(tx.contains(key1));
    });
  }

}
