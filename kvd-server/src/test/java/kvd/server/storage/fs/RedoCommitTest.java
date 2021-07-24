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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.Kvd;
import kvd.server.storage.Transaction;

public class RedoCommitTest {

  @BeforeAll
  public static void setupLogging() {
    Kvd.setLogLevel("kvd", "info");
  }

  @Test
  public void test1() throws Exception {
    Key key1 = Key.of("key1");
    Key key2 = Key.of("key2");
    Key key3 = Key.of("key3");
    File fStore = Files.createTempDirectory("kvdfile").toFile();
    {
      FileStorageBackend storage = new FileStorageBackend(fStore);
      try(Transaction t = storage.begin()) {
        assertFalse(t.contains(key1));
        assertFalse(t.contains(key2));
        assertFalse(t.contains(key3));
        t.putBytes(key1, key1.getBytes());
        t.commit();
      }
    }
    {
      FileStorageBackend storage = new FileStorageBackend(fStore);
      Transaction t1 = storage.begin();
      Transaction t2 = storage.begin();
      assertTrue(t1.contains(key1));
      assertFalse(t1.contains(key2));
      assertFalse(t1.contains(key3));
      assertTrue(t2.contains(key1));
      assertFalse(t2.contains(key2));
      assertFalse(t2.contains(key3));
      t1.remove(key1);
      t1.putBytes(key2, key2.getBytes());
      t2.putBytes(key3, key3.getBytes());
      assertFalse(t1.contains(key1));
      assertTrue(t1.contains(key2));
      assertFalse(t1.contains(key3));
      assertTrue(t2.contains(key1));
      assertFalse(t2.contains(key2));
      assertTrue(t2.contains(key3));
      ((FileTx)t1).setCrashOnCommitTest();
      assertThrows(KvdException.class, () -> t1.commit());
      assertTrue(new File(fStore, "transactions/1/"+FileTx.COMMIT_MARKER).exists());
      assertTrue(new File(fStore, "transactions/1/"+FileTx.REMOVE).exists());
      assertFalse(new File(fStore, "transactions/2/"+FileTx.COMMIT_MARKER).exists());
      assertFalse(new File(fStore, "transactions/2/"+FileTx.REMOVE).exists());
    }
    {
      FileStorageBackend storage = new FileStorageBackend(fStore);
      try(Transaction t = storage.begin()) {
        assertFalse(t.contains(key1));
        assertTrue(t.contains(key2));
        assertFalse(t.contains(key3));
      }
    }
  }

}
